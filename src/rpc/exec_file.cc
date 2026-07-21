#include "config.h"

#include <cassert>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <mutex>
#include <pthread.h>
#include <signal.h>
#include <spawn.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <torrent/exceptions.h>
#include <torrent/net/fd.h>
#include <torrent/system/thread.h>
#include <torrent/system/types.h>

#include "exec_file.h"
#include "parse.h"

// Standard POSIX environment pointer
extern char** environ;

namespace rpc {

// TODO: Access fd through torrent logging?

ExecFile::~ExecFile() {
  cleanup();
}

void
ExecFile::initialize() {
  sigset_t signal_mask;
  int mask_status = pthread_sigmask(SIG_SETMASK, nullptr, &signal_mask);

  if (mask_status != 0)
    throw torrent::internal_error("ExecFile::initialize() failed to read the signal mask: " + std::string(std::strerror(mask_status)));

  if (sigismember(&signal_mask, SIGCHLD) != 1)
    throw torrent::internal_error("ExecFile::initialize() requires SIGCHLD to be blocked on the calling thread.");

  // POSIX permits blocked, ignored signals to be discarded. Preserve them for
  // sigwait() by setting a non-ignored handler, but keep it blocked.
  struct sigaction action{};
  sigemptyset(&action.sa_mask);
  action.sa_handler = &ExecFile::sigchld_handler;
  action.sa_flags   = SA_NOCLDSTOP | SA_RESTART;

  if (::sigaction(SIGCHLD, &action, &m_previous_sigchld) == -1) {
    int error_number = errno;
    throw torrent::internal_error("ExecFile::initialize() failed to install the SIGCHLD handler: " + std::string(std::strerror(error_number)));
  }

  try {
    m_reaper_thread = std::thread(&ExecFile::reaper, this);

  } catch (...) {
    ::sigaction(SIGCHLD, &m_previous_sigchld, nullptr);
    throw;
  }
}

void
ExecFile::cleanup() noexcept {
  if (!m_reaper_thread.joinable())
    return;

  {
    std::lock_guard<std::mutex> lock(m_background_mutex);
    m_reaper_stopping = true;
  }

  pthread_kill(m_reaper_thread.native_handle(), SIGCHLD);
  m_reaper_thread.join();

  ::sigaction(SIGCHLD, &m_previous_sigchld, nullptr);
}

std::size_t
ExecFile::background_process_count() const {
  std::lock_guard<std::mutex> lock(m_background_mutex);
  return m_background_processes.size();
}

void
ExecFile::sigchld_handler([[maybe_unused]] int signum) noexcept {
}

void
ExecFile::reap_background_processes() noexcept {
  auto itr = m_background_processes.begin();

  while (itr != m_background_processes.end()) {
    int   status{};
    pid_t waited_pid;

    do {
      waited_pid = ::waitpid(*itr, &status, WNOHANG);
    } while (waited_pid == -1 && errno == EINTR);

    if (waited_pid == 0 || (waited_pid == -1 && errno != ECHILD)) {
      ++itr;
      continue;
    }

    itr = m_background_processes.erase(itr);
  }
}

void
ExecFile::reaper() {
  sigset_t child_signal;
  sigemptyset(&child_signal);
  sigaddset(&child_signal, SIGCHLD);

  while (true) {
    // SIGCHLD remains blocked in this thread; sigwait() accepts it synchronously.
    int received_signal{};
    int wait_status;

    do {
      wait_status = ::sigwait(&child_signal, &received_signal);
    } while (wait_status == EINTR);

    if (wait_status != 0) [[unlikely]]
      throw torrent::internal_error("ExecFile::reaper() sigwait failed: " + std::string(std::strerror(wait_status)));

    std::lock_guard<std::mutex> lock(m_background_mutex);

    reap_background_processes();

    if (m_reaper_stopping)
      break;
  }
}

int
ExecFile::execute(const char* file, char* const* argv, int flags) {
  assert(!((flags & flag_capture) && (flags & flag_background)));

  std::unique_lock<std::mutex> background_lock;

  if (flags & flag_background) {
    background_lock = std::unique_lock<std::mutex>(m_background_mutex);

    if (!m_reaper_thread.joinable() || m_reaper_stopping)
      throw torrent::internal_error("ExecFile::execute() background process reaper is not running.");

    // Reserve before spawning so an allocation failure can't leave an untracked child.
    if (m_background_processes.size() == m_background_processes.max_size())
      throw torrent::internal_error("ExecFile::execute() background process allocation failed.");

    m_background_processes.reserve(m_background_processes.size() + 1);
  }

  // Reset the signal handlers/masks for the new process.
  sigset_t spawn_signal_mask;
  sigset_t spawn_signal_defaults;
  sigemptyset(&spawn_signal_mask);
  sigfillset(&spawn_signal_defaults);

  // Write the executed command and its parameters to the log fd.
  [[maybe_unused]] int result;

  if (m_log_fd != -1) {
    for (char* const* itr = argv; *itr != NULL; itr++) {
      if (itr == argv)
        result = write(m_log_fd, "\n---\n", sizeof("\n---\n"));
      else
        result = write(m_log_fd, " ", 1);

      result = write(m_log_fd, *itr, std::strlen(*itr));
    }

    result = write(m_log_fd, "\n---\n", sizeof("\n---\n"));
  }

  posix_spawn_file_actions_t actions{};

  if (posix_spawn_file_actions_init(&actions) != 0)
    throw torrent::internal_error("ExecFile::execute(...) posix_spawn_file_actions_init failed.");

  posix_spawnattr_t attr;
  posix_spawnattr_init(&attr);

  // Try to avoid leaking open fds to the spawned process. Prefer POSIX_SPAWN_CLOEXEC_DEFAULT
  // (macOS-only) or posix_spawn_file_actions_addclosefrom_np (glibc >= 2.34, FreeBSD >= 13.1).
  //
  // Other platforms like musl libc, OpenBSD and NetBSD must rely on explicit O_CLOEXEC.

  // Handle standard input redirection (/dev/null), posix_spawn_file_actions_addopen handles opening
  // and dup2 natively
  if (posix_spawn_file_actions_addopen(&actions, 0, "/dev/null", O_RDWR, 0) != 0) {
    // Fallback if open fails inside action setup
    posix_spawn_file_actions_addclose(&actions, 0);
  }

  int pipe_0 = -1;
  int pipe_1 = -1;

  // Handle standard output redirection
  if (flags & flag_capture) {
    torrent::fd_open_pipe(pipe_0, pipe_1);

    posix_spawn_file_actions_adddup2(&actions, pipe_1, 1);

    // Ensure the write end of the pipe is closed in the child after duplicating.
    posix_spawn_file_actions_addclose(&actions, pipe_0);
    posix_spawn_file_actions_addclose(&actions, pipe_1);

  } else if (m_log_fd != -1) {
    posix_spawn_file_actions_adddup2(&actions, m_log_fd, 1);

  } else {
    posix_spawn_file_actions_addopen(&actions, 1, "/dev/null", O_RDWR, 0);
  }

  if (m_log_fd != -1) {
    posix_spawn_file_actions_adddup2(&actions, m_log_fd, 2);
  } else {
    posix_spawn_file_actions_addopen(&actions, 2, "/dev/null", O_RDWR, 0);
  }

  short spawn_flags = 0;

#if defined(POSIX_SPAWN_CLOEXEC_DEFAULT)
  spawn_flags |= POSIX_SPAWN_CLOEXEC_DEFAULT;
#elif defined(HAVE_POSIX_SPAWN_FILE_ACTIONS_ADDCLOSEFROM_NP)
  posix_spawn_file_actions_addclosefrom_np(&actions, 3);
#endif

  if (flags & flag_background) {
#ifdef POSIX_SPAWN_SETSID
    spawn_flags |= POSIX_SPAWN_SETSID;
#else
    spawn_flags |= POSIX_SPAWN_SETPGROUP;
    posix_spawnattr_setpgroup(&attr, 0);
#endif
  }

  posix_spawnattr_setsigmask(&attr, &spawn_signal_mask);
  posix_spawnattr_setsigdefault(&attr, &spawn_signal_defaults);
  spawn_flags |= POSIX_SPAWN_SETSIGMASK | POSIX_SPAWN_SETSIGDEF;

  posix_spawnattr_setflags(&attr, spawn_flags);

  pid_t child_pid{};
  int   spawn_status = posix_spawnp(&child_pid, file, &actions, &attr, argv, environ);

  posix_spawn_file_actions_destroy(&actions);
  posix_spawnattr_destroy(&attr);

  if (spawn_status != 0) {
    if (pipe_0 != -1)
      torrent::fd_close(pipe_0);

    if (pipe_1 != -1)
      torrent::fd_close(pipe_1);

    throw torrent::input_error("ExecFile::execute() posix_spawn failed: " + torrent::system::errno_enum_str(spawn_status));
  }

  if (flags & flag_background) {
    m_background_processes.push_back(child_pid);

    // Make sure the process we just started hasn't already exited.
    reap_background_processes();
    background_lock.unlock();
  }

  if (flags & flag_capture) {
    m_capture = std::string();
    torrent::fd_close(pipe_1);

    char buffer[4096];
    ssize_t length;

    do {
      length = read(pipe_0, buffer, sizeof(buffer));

      if (length > 0)
        m_capture += std::string(buffer, length);

    } while (length > 0);

    torrent::fd_close(pipe_0);

    if (m_log_fd != -1) {
      result = write(m_log_fd, "Captured output:\n", sizeof("Captured output:\n"));
      result = write(m_log_fd, m_capture.data(), m_capture.length());
    }
  }

  if (flags & flag_background) {
    if (m_log_fd != -1)
      result = write(m_log_fd, "\n--- Running in Background ---\n", sizeof("\n--- Running in Background ---\n"));

    return 0;
  }

  int status;

  while (::waitpid(child_pid, &status, 0) == -1) {
    switch (errno) {
    case EINTR:
      continue;
    case ECHILD:
      throw torrent::internal_error("ExecFile::execute(...) waitpid failed with ECHILD, child process not found.");
    case EINVAL:
      throw torrent::internal_error("ExecFile::execute(...) waitpid failed with EINVAL.");
    default:
      throw torrent::internal_error("ExecFile::execute(...) waitpid failed with unexpected error: " + std::string(std::strerror(errno)));
    }
  };

  // Check return value?
  if (m_log_fd != -1) {
    if (WIFEXITED(status) && WEXITSTATUS(status) == 0)
      result = write(m_log_fd, "\n--- Success ---\n", sizeof("\n--- Success ---\n"));
    else
      result = write(m_log_fd, "\n--- Error ---\n", sizeof("\n--- Error ---\n"));
  }

  return status;
}

torrent::Object
ExecFile::execute_object(const torrent::Object& rawArgs, int flags) {
  char*  argsBuffer[max_args];
  char** argsCurrent = argsBuffer;

  // Size of value strings are less than 24.
  char   valueBuffer[buffer_size+1];
  char*  valueCurrent = valueBuffer;

  if (rawArgs.is_list()) {
    const torrent::Object::list_type& args = rawArgs.as_list();

    if (args.empty())
      throw torrent::input_error("Too few arguments.");

    for (torrent::Object::list_const_iterator itr = args.begin(), last = args.end(); itr != last; itr++, argsCurrent++) {
      if (argsCurrent == argsBuffer + max_args - 1)
        throw torrent::input_error("Too many arguments.");

      if (itr->is_string() && (!(flags & flag_expand_tilde) || *itr->as_string().c_str() != '~')) {
        *argsCurrent = const_cast<char*>(itr->as_string().c_str());

      } else {
        *argsCurrent = valueCurrent;
        valueCurrent = print_object(valueCurrent, valueBuffer + buffer_size, &*itr, flags) + 1;

        if (valueCurrent >= valueBuffer + buffer_size)
          throw torrent::input_error("Overflowed execute arg buffer.");
      }
    }

  } else {
    const torrent::Object::string_type& args = rawArgs.as_string();

    if ((flags & flag_expand_tilde) && args.c_str()[0] == '~') {
      *argsCurrent = valueCurrent;
      valueCurrent = print_object(valueCurrent, valueBuffer + buffer_size, &rawArgs, flags) + 1;
    } else {
      *argsCurrent = const_cast<char*>(args.c_str());
    }

    argsCurrent++;
  }

  *argsCurrent = NULL;

  int status = execute(argsBuffer[0], argsBuffer, flags);

  if ((flags & flag_throw) && status != 0)
    throw torrent::input_error("Bad return code.");

  if (flags & flag_capture)
    return m_capture;

  return torrent::Object((int64_t)status);
}

}
