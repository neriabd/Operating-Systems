#define SIGACTION_CALL_FAILED "system error: sigaction call failed"
#define SETITIMER_CALL_FAILED "system error: setitimer call failed"
#define QUANTUM_LENGTH_ERROR "thread library error: invalid quantum length"
#define MAX_THREADS_ERROR "thread library error: passed maximum amount \
of threads"
#define ENRTY_POINT_ERROR "thread library error: invalid entry point"
#define MAIN_THREAD_SLEEP_ERROR "thread library error: main thread can't call \
sleep function"
#define THREAD_BLOCK_ERROR "thread library error: can't blocked this id"
#define RESUME_ERROR "thread library error: cannot resume this tid"
#define TERMINATE_ERROR "thread library error: tid doesn't exist"
#define GET_QUANTUM_ERROR "thread library error: tid doesn't exist"
#define NUM_QUANTUMS_SLEEP_ERROR "thread library error: quantums of sleep \
should be positive"
#define ALLOCATION_FAILED_ERROR "system error: allocation failed"
#define SECOND 1000000

#include "uthreads.h"
#include <iostream>
#include <setjmp.h>
#include <signal.h>
#include <sys/time.h>
#include <deque>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <algorithm>

#ifdef __x86_64__
/* code for 64 bit Intel arch */
typedef unsigned long address_t;
#define JB_SP 6
#define JB_PC 7

address_t translate_address (address_t addr)
{
  address_t ret;
  asm volatile("xor    %%fs:0x30,%0\n"
               "rol    $0x11,%0\n"
      : "=g" (ret)
      : "0" (addr));
  return ret;
}

#else
/* code for 32 bit Intel arch */

typedef unsigned int address_t;
#define JB_SP 4
#define JB_PC 5


/* A translation is required when using an address of a variable.
   Use this as a black box in your code. */
address_t translate_address(address_t addr)
{
    address_t ret;
    asm volatile("xor    %%gs:0x18,%0\n"
                 "rol    $0x9,%0\n"
                 : "=g" (ret)
                 : "0" (addr));
    return ret;
}
#endif

enum ThreadState
{
    RUNNING = 1,
    READY = 2,
    BLOCKED = 3,
    READY_SLEEPING = 4,
    BLOCKED_SLEEPING = 5
};

class Thread
{
 private:
  int _tid;
  int _status = READY;
  int _sleeping_time = 0;
  int _running_quantums = 0;
  char *_thread_stack;
  thread_entry_point _entry_point;

 public:
  sigjmp_buf _env{};
  Thread (int tid, thread_entry_point entry_point = nullptr)
      : _tid (tid), _entry_point (entry_point)
  {
    if (tid == 0)
    {
      sigsetjmp(_env, 1);
      sigemptyset (&_env->__saved_mask);
      _status = RUNNING;
      _running_quantums = 1;
      return;
    }
    _thread_stack = new (std::nothrow) char[STACK_SIZE];
    if (_thread_stack == nullptr)
    {
      std::cerr << ALLOCATION_FAILED_ERROR << std::endl;
      exit (1);
    }
    setup_thread ();
  }

  void setup_thread ()
  {
    address_t sp = (address_t) _thread_stack + STACK_SIZE
                   - sizeof (unsigned long);
    address_t pc = (address_t) _entry_point;
    sigsetjmp(_env, 1);
    (_env->__jmpbuf)[JB_SP] = translate_address (sp);
    (_env->__jmpbuf)[JB_PC] = translate_address (pc);
    if (sigemptyset (&_env->__saved_mask) == -1)
    {
      std::cerr << ALLOCATION_FAILED_ERROR << std::endl;
      exit (1);
    }
  }

  ~Thread ()
  {
    if (_tid != 0)
    {
      delete[] _thread_stack;
      _thread_stack = nullptr;
    }
  }

  void set_sleeping_time (int quantums)
  {
    _sleeping_time = quantums;
  }

  int get_sleeping_time () const
  {
    return _sleeping_time;
  }

  void increase_quantum ()
  {
    _running_quantums += 1;
  }

  int get_running_quantums () const
  {
    return _running_quantums;
  }

  int get_status () const
  {
    return _status;
  }

  void set_status (int status)
  {
    _status = status;
  }

};

void wake_up ();
int get_new_tid ();
void add_to_ready (int tid);
void add_to_blocked (int tid);
void jump_next_thread ();
void remove_tid (int tid);
void timer_handler (int sig);
void reset_timer (int quantum_usec);
void add_to_threads (int tid, thread_entry_point entry_point);
void terminate_thread (int tid);
void block_timer_signals ();
void unblock_timer_signals ();
void add_to_sleeping (int tid, int num_quantums);
bool tid_exist (int tid);

std::deque<int> ready;
std::unordered_set<int> sleeping;
std::unordered_set<int> blocked;
std::unordered_map<int, Thread *> threads;

static int quantum;
static sigset_t set;
static int total_quantums = 1;
static int first_available_id = 1;

int uthread_init (int quantum_usecs)
{
  if (quantum_usecs <= 0)
  {
    std::cerr << QUANTUM_LENGTH_ERROR << std::endl;
    return -1;
  }
  Thread *init_thread = new Thread (0);
  threads.insert ({0, init_thread});
  ready.emplace_back (0);
  quantum = quantum_usecs;
  reset_timer (quantum_usecs);
  return 0;
}

int uthread_spawn (thread_entry_point entry_point)
{
  block_timer_signals ();
  if (entry_point == nullptr)
  {
    std::cerr << ENRTY_POINT_ERROR << std::endl;
    unblock_timer_signals ();
    return -1;
  }
  int tid = get_new_tid ();
  if (tid == -1)
  {
    std::cerr << MAX_THREADS_ERROR << std::endl;
    unblock_timer_signals ();
    return -1;
  }
  add_to_threads (tid, entry_point);
  add_to_ready (tid);
  unblock_timer_signals ();
  return tid;
}

int uthread_terminate (int tid)
{
  block_timer_signals ();
  if (!tid_exist (tid))
  {
    std::cerr << TERMINATE_ERROR << std::endl;
    unblock_timer_signals ();
    return -1;
  }
  block_timer_signals ();
  if (tid == 0)
  {
    unblock_timer_signals ();
    for (auto it: threads)
    {
      Thread *cur_thread = it.second;
      delete cur_thread;
    }
    exit (0);
  }
  remove_tid (tid);
  Thread *cur_thread = threads[tid];
  int status = cur_thread->get_status ();
  terminate_thread (tid);
  unblock_timer_signals ();
  if (status == RUNNING)
  {
    jump_next_thread ();
  }
  return 0;
}

int uthread_block (int tid)
{
  block_timer_signals ();
  if (!tid_exist (tid) || tid == 0)
  {
    std::cerr << THREAD_BLOCK_ERROR << std::endl;
    unblock_timer_signals ();
    return -1;
  }
  Thread *cur_thread = threads[tid];
  int status = cur_thread->get_status ();
  if (status == BLOCKED || status == BLOCKED_SLEEPING)
  {
    unblock_timer_signals ();
    return 0;
  }
  if (status == READY_SLEEPING)
  {
    cur_thread->set_status (BLOCKED_SLEEPING);
    unblock_timer_signals ();
    return 0;
  }
  remove_tid (tid);
  add_to_blocked (tid);
  unblock_timer_signals ();
  if (status == RUNNING)
  {
    int ret_val = sigsetjmp(cur_thread->_env, 1);
    if (ret_val == 0)
    {
      jump_next_thread ();
    }
  }
  return 0;
}

int uthread_resume (int tid)
{
  block_timer_signals ();
  if (!tid_exist (tid))
  {
    std::cerr << RESUME_ERROR << std::endl;
    unblock_timer_signals ();
    return -1;
  }
  Thread *cur_thread = threads[tid];
  int status = cur_thread->get_status ();
  if (status == BLOCKED)
  {
    remove_tid (tid);
    add_to_ready (tid);
  }
  if (status == BLOCKED_SLEEPING)
  {
    cur_thread->set_status (READY_SLEEPING);
  }
  unblock_timer_signals ();
  return 0;
}

int uthread_sleep (int num_quantums)
{
  block_timer_signals ();
  if (num_quantums <= 0)
  {
    std::cerr << NUM_QUANTUMS_SLEEP_ERROR << std::endl;
    unblock_timer_signals ();
    return -1;
  }
  int tid = ready.front ();
  if (tid == 0)
  {
    std::cerr << MAIN_THREAD_SLEEP_ERROR << std::endl;
    unblock_timer_signals ();
    return -1;
  }
  remove_tid (tid);
  add_to_sleeping (tid, num_quantums);
  Thread *cur_thread = threads[tid];
  unblock_timer_signals ();
  int ret_val = sigsetjmp(cur_thread->_env, 1);
  if (ret_val == 0)
  {
    jump_next_thread ();
  }
  return 0;
}

int uthread_get_tid ()
{
  return ready.front ();
}

int uthread_get_total_quantums ()
{
  return total_quantums;
}

int uthread_get_quantums (int tid)
{
  if (!tid_exist (tid))
  {
    std::cerr << GET_QUANTUM_ERROR << std::endl;
    return -1;
  }
  Thread *cur_thread = threads[tid];
  return cur_thread->get_running_quantums ();
}

////////////// HELPER FUNCTIONS //////////////

bool tid_exist (int tid)
{
  if (threads.find (tid) == threads.end ())
  {
    return false;
  }
  return true;
}

void terminate_thread (int tid)
{
  if (tid < first_available_id)
  {
    first_available_id = tid;
  }
  Thread *cur_thread = threads[tid];
  threads.erase (tid);
  delete cur_thread;
}

void remove_tid (int tid)
{
  Thread *cur_thread = threads[tid];
  int state = cur_thread->get_status ();
  switch (state)
  {
    case RUNNING:
    {
      ready.pop_front ();
      break;
    }
    case READY:
    {
      auto it = std::find (ready.begin (), ready.end (), tid);
      ready.erase (it);
      break;
    }
    case BLOCKED:
    {
      blocked.erase (tid);
      break;
    }
    default:
      sleeping.erase (tid);
      cur_thread->set_sleeping_time (tid);
      break;
  }
}

void add_to_threads (int tid, thread_entry_point entry_point)
{
  Thread *new_thread = new Thread (tid, entry_point);
  threads.insert ({tid, new_thread});
}

void add_to_sleeping (int tid, int num_quantums)
{
  int wake_up_time = num_quantums + total_quantums;
  Thread *cur_thread = threads[tid];
  sleeping.insert (tid);
  cur_thread->set_status (READY_SLEEPING);
  cur_thread->set_sleeping_time (wake_up_time);
}

int get_new_tid ()
{
  if (first_available_id == MAX_THREAD_NUM)
  {
    return -1;
  }
  int cur_tid = first_available_id;
  bool found = false;
  int check_id = first_available_id;
  while (!found)
  {
    check_id++;
    if (threads.find (check_id) == threads.end ())
    {
      first_available_id = check_id;
      found = true;
    }
  }
  return cur_tid;
}

void block_timer_signals ()
{
  sigemptyset (&set);
  sigaddset (&set, SIGVTALRM);
  sigprocmask (SIG_BLOCK, &set, NULL);
}

void unblock_timer_signals ()
{
  sigprocmask (SIG_UNBLOCK, &set, NULL);
}

void increase_total_quantums ()
{
  ++total_quantums;
  int cur_tid = ready.front ();
  Thread *cur_thread = threads[cur_tid];
  cur_thread->increase_quantum ();
}

void jump_next_thread ()
{
  block_timer_signals ();
  increase_total_quantums ();
  wake_up ();
  int cur_tid = ready.front ();
  Thread *cur_thread = threads[cur_tid];
  cur_thread->set_status (RUNNING);
  unblock_timer_signals ();
  reset_timer (quantum);
  siglongjmp (cur_thread->_env, 1);
}

void timer_handler (int sig)
{
  int cur_tid = ready.front ();
  Thread *cur_thread = threads[cur_tid];
  int ret_val = sigsetjmp(cur_thread->_env, 1);
  if (ret_val == 0)
  {
    if (ready.size () != 1)
    {
      cur_thread->set_status (READY);
    }
    ready.pop_front ();
    ready.push_back (cur_tid);
    jump_next_thread ();
  }
}

void add_to_blocked (int tid)
{
  blocked.insert (tid);
  threads[tid]->set_status (BLOCKED);
}

void add_to_ready (int tid)
{
  ready.emplace_back (tid);
  threads[tid]->set_status (READY);
}

void wake_up ()
{
  std::vector<int> wake_up_tids = {};
  for (auto tid: sleeping)
  {
    Thread *cur_thread = threads[tid];
    if (cur_thread->get_sleeping_time () == total_quantums)
    {
      int status = cur_thread->get_status ();
      if (status == BLOCKED_SLEEPING)
      {
        add_to_blocked (tid);
      }
      else if (status == READY_SLEEPING)
      {
        add_to_ready (tid);
      }
      wake_up_tids.push_back (tid);
    }
  }
  for (auto id: wake_up_tids)
  {
    sleeping.erase (id);
  }
}

void reset_timer (int quantum_usec)
{
  struct sigaction sa = {0};
  struct itimerval timer;
  sa.sa_handler = &timer_handler;
  if (sigaction (SIGVTALRM, &sa, NULL) < 0)
  {
    std::cerr << SIGACTION_CALL_FAILED << std::endl;
    exit (1);
  }

  int sec = quantum_usec / SECOND;
  int usec = quantum_usec % SECOND;

  timer.it_value.tv_sec = sec;
  timer.it_value.tv_usec = usec;
  timer.it_interval.tv_sec = 0;
  timer.it_interval.tv_usec = 0;

  if (setitimer (ITIMER_VIRTUAL, &timer, NULL))
  {
    std::cerr << SETITIMER_CALL_FAILED << std::endl;
    exit (1);
  }
}

