#include "MapReduceFramework.h"
#include "Barrier.h"
#include <pthread.h>
#include <cstdio>
#include <iostream>
#include <atomic>
#include <vector>
#include <set>
#include <algorithm>
#define SYSTEM_ERROR "system error: system call or standard library \
function failed"

/**
 * The pair_sort_inter function is a comparison function that sorts
 * intermediate pairs based on the values of their first elements.
 * @param pair1 - pair of key 2 and value 2
 * @param pair2 - pair of key 2 and value 2
 * @return true if *pair1 < *pair2
 */
bool
pair_sort_inter (const IntermediatePair &pair1, const IntermediatePair &pair2);

/** reduces intermediate vectors in parallel using multiple threads.
 * It iterates over a queue of intermediate vectors,
 * performs reduction operations, and updates the count of reduced pairs.
 * @param context - threads context
 */
void threads_reduce_phase (void *context);

/**
 * updates an atomic counter value associated with a specific job and stage.
 * It calculates the total count based on the stage and adjusts the counter
 * value if it exceeds certain limits before updating the atomic state.
 * @param job - struct pointer to the program data
 * @param stage - current stage the program is in
 * @param counter - number of processed items in the current stage
 */
void update_atomic_counter (JobHandle job, stage_t stage, int counter);

/**
 * processing input pairs in parallel using multiple threads.
 * It iterates over the input vector, updates an atomic counter,
 * and performs mapping operations on each input pair.
 * The goal is to process all the input pairs until the
 * end of the vector is reached.
 * @param context - threads context
 */
void threads_map_phase (void *context);

/**
 * performs the shuffling phase by iterating over ordered keys and populating
 * a shuffle vector with intermediate pairs from multiple thread vectors.
 * It updates an atomic counter to track the number of processed pairs
 * during the shuffling stage.
 * @param context - threads context
 */
void shuffle (void *context);

/**
 * the main entry point for each thread in the parallel execution.
 * It sequentially executes the map, sort, shuffle, and reduce phases based on
 * the provided thread context, and synchronizes the threads using a barrier.
 * @param context - threads context
 * @return -
 */
void *thread_func (void *context);

/**
 * custom comparison operator that compares two pointers to objects of type K2
 */
struct compare
{
    bool operator() (const K2 *key1, const K2 *key2) const
    {
      return *key2 < *key1;
    }
};

/////////// typedefs ///////////

typedef struct Job Job;
typedef struct ThreadContext ThreadContext;
typedef std::set<K2 *, compare> keys_set;
typedef std::atomic<int> atomic_int;
typedef std::atomic<int64_t> atomic_int_64;
typedef std::vector<IntermediateVec *> vec_queue;

/////////// structs ///////////


struct ThreadContext
{
    Job *job;
    IntermediateVec thread_vec = {};
    unsigned int thread_id;
};

struct Job
{
    /////////// VARIABLES ///////////
    const MapReduceClient *client;
    const InputVec *inputVec;
    int input_vec_size;
    OutputVec *outputVec;
    vec_queue shuffle_vector;
    int shuffled_vec_size = 0;
    int multiThreadLevel;
    pthread_t *threads;
    ThreadContext *thread_contexts;
    Barrier *barrier;
    keys_set ordered_keys;
    bool called_wait = false;

    /////////// ATOMIC ///////////
    atomic_int *ac_num_input_items;
    atomic_int *ac_num_inter_pairs;
    atomic_int *ac_num_vec_in_queue;
    atomic_int *ac_num_reduced_pairs;
    atomic_int_64 *atomic_state;

    /////////// MUTEXES ///////////
    pthread_mutex_t update_atomic_mutex;
    pthread_mutex_t emit2_mutex;
    pthread_mutex_t emit3_mutex;
    pthread_mutex_t reduce_mutex;
};

/////////// FUNCTIONS ///////////

void check_system_error (int indicator)
{
  if (indicator)
  {
    std::cout << SYSTEM_ERROR << std::endl;
    exit (1);
  }
}

bool
pair_sort_inter (const IntermediatePair &pair1, const IntermediatePair &pair2)
{
  return (*pair1.first) < (*pair2.first);
}

void update_atomic_counter (JobHandle job, stage_t stage, int counter)
{
  Job *cur_job = (Job *) job;
  int64_t stage_shifted = (int64_t) stage << 62;
  int64_t total = 0;
  switch (stage)
  {
    case UNDEFINED_STAGE:
      break;
    case MAP_STAGE:
      if (counter >= cur_job->input_vec_size)
      {
        counter = cur_job->input_vec_size;
      }
      total = (int64_t) (cur_job->input_vec_size) << 31;
      break;
    default:
      if (counter >= *cur_job->ac_num_inter_pairs)
      {
        counter = *cur_job->ac_num_inter_pairs;
      }
      total = (int64_t) (*cur_job->ac_num_inter_pairs) << 31;
      break;
  }
  int64_t value = stage_shifted | total | (int64_t) counter;
  *(cur_job->atomic_state) = value;
}

void threads_map_phase (void *context)
{
  ThreadContext *cur_context = (ThreadContext *) context;
  int input_vec_size = (int) cur_context->job->inputVec->size ();
  bool finish_processing = false;
  while (!finish_processing)
  {
    int old_value = (*(cur_context->job->ac_num_input_items))++;
    if (old_value >= input_vec_size)
    {
      finish_processing = true;
    }
    else
    {
      int lock_success = pthread_mutex_lock
          (&cur_context->job->update_atomic_mutex);
      check_system_error (lock_success);
      update_atomic_counter (cur_context->job, MAP_STAGE,
                             (*(cur_context->job->ac_num_input_items)));
      int unlock_success = pthread_mutex_unlock
          (&cur_context->job->update_atomic_mutex);
      check_system_error (unlock_success);
      const InputPair &cur_pair = (*(cur_context->job->inputVec))[old_value];
      cur_context->job->client->map (cur_pair.first, cur_pair.second, context);
    }
  }
}

void shuffle (void *context)
{
  ThreadContext *cur_context = (ThreadContext *) context;
  int num_processed_pairs = 0;
  for (K2 *cur_key: cur_context->job->ordered_keys)
  {
    IntermediateVec *cur_key_vec = new IntermediateVec;
    for (int i = 0; i < cur_context->job->multiThreadLevel; ++i)
    {
      IntermediateVec &vec2 = cur_context->job->thread_contexts[i].thread_vec;
      int vec_size = (int) vec2.size ();
      for (int j = 0; j < vec_size; ++j)
      {
        if ((*vec2.back ().first) < *cur_key
            || *cur_key < (*(vec2.back ().first)))
        {
          break;
        }
        cur_key_vec->push_back (vec2.back ());
        num_processed_pairs++;
        update_atomic_counter (cur_context->job, SHUFFLE_STAGE,
                               num_processed_pairs);
        vec2.pop_back ();
      }
    }
    cur_context->job->shuffle_vector.push_back (cur_key_vec);
  }
}

void *thread_func (void *context)
{
  ThreadContext *cur_context = (ThreadContext *) context;

  // MAP
  threads_map_phase (context);

  // SORT
  std::sort (cur_context->thread_vec.begin (),
             cur_context->thread_vec.end (), pair_sort_inter);
  cur_context->job->barrier->barrier ();

  // SHUFFLE
  if (cur_context->thread_id == 0)
  {
    update_atomic_counter (cur_context->job, SHUFFLE_STAGE, 0);
    shuffle (context);
    cur_context->job->shuffled_vec_size =
        (int) cur_context->job->shuffle_vector.size ();
    update_atomic_counter (cur_context->job, REDUCE_STAGE, 0);
  }
  cur_context->job->barrier->barrier ();

  // REDUCE
  threads_reduce_phase (context);

  return 0;
}

void threads_reduce_phase (void *context)
{
  ThreadContext *cur_context = (ThreadContext *) context;
  vec_queue &shuffle_vec = cur_context->job->shuffle_vector;
  bool finish_processing = false;
  while (!finish_processing)
  {
    int old_value = (*(cur_context->job->ac_num_vec_in_queue))++;
    if (old_value >= cur_context->job->shuffled_vec_size)
    {
      finish_processing = true;
    }
    else
    {
      const IntermediateVec *cur_vec_to_reduce = shuffle_vec[old_value];
      cur_context->job->client->reduce (cur_vec_to_reduce, context);
      atomic_int *&counter = cur_context->job->ac_num_reduced_pairs;
      int lock_success = pthread_mutex_lock (&cur_context->job->reduce_mutex);
      check_system_error (lock_success);
      int cur_num_pairs = (int) cur_vec_to_reduce->size ();
      *counter += cur_num_pairs;
      update_atomic_counter (cur_context->job, REDUCE_STAGE, *counter);
      int unlock_success = pthread_mutex_unlock (&cur_context->job->reduce_mutex);
      check_system_error (unlock_success);
    }
  }
}

void emit3 (K3 *key, V3 *value, void *context)
{
  ThreadContext *cur_context = (ThreadContext *) context;

  int lock_success = pthread_mutex_lock (&cur_context->job->emit3_mutex);
  check_system_error (lock_success);
  (*(cur_context->job->outputVec)).emplace_back (std::make_pair (key, value));
  int unlock_success = pthread_mutex_unlock (&cur_context->job->emit3_mutex);
  check_system_error (unlock_success);
}

void emit2 (K2 *key, V2 *value, void *context)
{
  ThreadContext *cur_context = (ThreadContext *) context;
  int lock_success = pthread_mutex_lock (&cur_context->job->emit2_mutex);
  check_system_error (lock_success);
  keys_set &set_keys = cur_context->job->ordered_keys;
  if (set_keys.find (key) == set_keys.end ())
  {
    set_keys.insert (key);
  }
  cur_context->thread_vec.emplace_back (std::make_pair (key, value));
  (*cur_context->job->ac_num_inter_pairs)++;
  int unlock_success = pthread_mutex_unlock (&cur_context->job->emit2_mutex);
  check_system_error (unlock_success);
}

JobHandle startMapReduceJob (const MapReduceClient &client,
                             const InputVec &inputVec, OutputVec &outputVec,
                             int multiThreadLevel)
{
  Job *job = new Job;
  ThreadContext *thread_contexts = new ThreadContext[multiThreadLevel];
  pthread_t *threads = new pthread_t[multiThreadLevel];
  Barrier *barrier = new Barrier (multiThreadLevel);

  atomic_int *ac_num_input_items = new atomic_int (0);
  atomic_int *ac_num_inter_pairs = new atomic_int (0);
  atomic_int *ac_num_reduced_pairs = new atomic_int (0);
  atomic_int *ac_num_vec_in_queue = new atomic_int (0);
  atomic_int_64 *atomic_counter_state = new atomic_int_64 (0);

  pthread_mutex_t emit2_mutex = PTHREAD_MUTEX_INITIALIZER;
  pthread_mutex_t emit3_mutex = PTHREAD_MUTEX_INITIALIZER;
  pthread_mutex_t update_atomic_mutex = PTHREAD_MUTEX_INITIALIZER;
  pthread_mutex_t reduce_mutex = PTHREAD_MUTEX_INITIALIZER;

  job->client = &client;
  job->inputVec = &inputVec;
  job->input_vec_size = (int) inputVec.size ();
  job->outputVec = &outputVec;
  job->multiThreadLevel = multiThreadLevel;
  job->threads = threads;
  job->thread_contexts = thread_contexts;
  job->barrier = barrier;
  job->ac_num_input_items = ac_num_input_items;
  job->ac_num_inter_pairs = ac_num_inter_pairs;
  job->ac_num_vec_in_queue = ac_num_vec_in_queue;
  job->ac_num_reduced_pairs = ac_num_reduced_pairs;
  job->atomic_state = atomic_counter_state;
  job->update_atomic_mutex = update_atomic_mutex;
  job->emit2_mutex = emit2_mutex;
  job->emit3_mutex = emit3_mutex;//
  job->reduce_mutex = reduce_mutex;

  for (int i = 0; i < multiThreadLevel; ++i)
  {
    thread_contexts[i].job = job;
    thread_contexts[i].thread_id = i;
    int create_success = pthread_create (
        threads + i, NULL, thread_func, thread_contexts + i);
    check_system_error (create_success);
  }
  return job;
}

void waitForJob (JobHandle job)
{
  Job *cur_job = (Job *) job;
  if (cur_job->called_wait)
  {
    return;
  }
  for (int i = 0; i < cur_job->multiThreadLevel; ++i)
  {
    int joined_success = pthread_join (cur_job->threads[i], NULL);
    check_system_error (joined_success);
  }
  cur_job->called_wait = true;
}

void getJobState (JobHandle job, JobState *state)
{
  Job *cur_job = (Job *) job;
  if (*cur_job->atomic_state == UNDEFINED_STAGE)
  {
    state->stage = UNDEFINED_STAGE;
    state->percentage = 0;
    return;
  }
  else
  {
    int64_t atomic_state = *cur_job->atomic_state;
    int64_t lsb_31_mask = 0x7FFFFFFF;  // Mask with the first 31 bits set to 1
    int64_t counter = (atomic_state & lsb_31_mask);
    int64_t shifts_31 = atomic_state >> 31;
    int64_t total = shifts_31 & lsb_31_mask;
    state->percentage = (float) (counter * 100) / (float) total;
    int64_t stage_mask = 0x03;
    state->stage = (stage_t) ((shifts_31 >> 31) & stage_mask);
  }
}

void closeJobHandle (JobHandle job)
{
  waitForJob (job);
  Job *cur_job = (Job *) job;

  // release atomic counters & barrier
  delete cur_job->ac_num_input_items;
  delete cur_job->ac_num_inter_pairs;
  delete cur_job->ac_num_reduced_pairs;
  delete cur_job->ac_num_vec_in_queue;
  delete cur_job->atomic_state;
  delete cur_job->barrier;

  // release vector pointers in shuffle vector
  for (int i = 0; i < cur_job->shuffled_vec_size; ++i)
  {
    delete cur_job->shuffle_vector[i];
  }

  pthread_mutex_destroy (&cur_job->update_atomic_mutex);
  pthread_mutex_destroy (&cur_job->emit2_mutex);
  pthread_mutex_destroy (&cur_job->emit3_mutex);
  pthread_mutex_destroy (&cur_job->reduce_mutex);

  // release threads arrays
  delete[] cur_job->threads;
  delete[] cur_job->thread_contexts;

  // release job
  delete cur_job;

}

