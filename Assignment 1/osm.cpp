#include <iostream>
#include "osm.h"
#include <sys/time.h>

double osm_operation_time (unsigned int iterations)
{
  if (iterations == 0)
  {
    return -1;
  }
  if (iterations % 10 == 0)
  {
    iterations = iterations / 10;
  }
  else
  {
    iterations = (iterations / 10) + 1;
  }
  struct timeval tv;
  if (gettimeofday (&tv, NULL) == -1)
  {
    return -1;
  }
  time_t sec_begin = tv.tv_sec;
  suseconds_t msec_begin = tv.tv_usec;
  for (unsigned int i = 0; i < iterations; i++)
  {
    (void) (1 + 1);
    (void) (1 + 1);
    (void) (1 + 1);
    (void) (1 + 1);
    (void) (1 + 1);
    (void) (1 + 1);
    (void) (1 + 1);
    (void) (1 + 1);
    (void) (1 + 1);
    (void) (1 + 1);
  }
  if (gettimeofday (&tv, NULL) == -1)
  {
    return -1;
  }
  return (double) ((tv.tv_sec - sec_begin) * 1000000000 + (tv.tv_usec -
                                                           msec_begin) * 1000)
         / (double) (iterations*10);
}

void empty ()
{};

double osm_function_time (unsigned int iterations)
{
  if (iterations == 0)
  {
    return -1;
  }
  if (iterations % 10 == 0)
  {
    iterations = iterations / 10;
  }
  else
  {
    iterations = (iterations / 10) + 1;
  }
  struct timeval tv;
  if (gettimeofday (&tv, NULL) == -1)
  {
    return -1;
  }
  time_t sec_begin = tv.tv_sec;
  suseconds_t msec_begin = tv.tv_usec;
  for (unsigned int i = 0; i < iterations; i++)
  {
    empty ();
    empty ();
    empty ();
    empty ();
    empty ();
    empty ();
    empty ();
    empty ();
    empty ();
    empty ();
  }
  if (gettimeofday (&tv, NULL) == -1)
  {
    return -1;
  }
  return (double) ((tv.tv_sec - sec_begin) * 1000000000 + (tv.tv_usec -
                                                           msec_begin) * 1000)
         / (double) (iterations*10);
}

double osm_syscall_time (unsigned int iterations)
{
  if (iterations == 0)
  {
    return -1;
  }
  if (iterations % 10 == 0)
  {
    iterations = iterations / 10;
  }
  else
  {
    iterations = (iterations / 10) + 1;
  }
  struct timeval tv;
  if (gettimeofday (&tv, NULL) == -1)
  {
    return -1;
  }
  time_t sec_begin = tv.tv_sec;
  suseconds_t msec_begin = tv.tv_usec;
  for (unsigned int i = 0; i < iterations; i++)
  {
    OSM_NULLSYSCALL;
    OSM_NULLSYSCALL;
    OSM_NULLSYSCALL;
    OSM_NULLSYSCALL;
    OSM_NULLSYSCALL;
    OSM_NULLSYSCALL;
    OSM_NULLSYSCALL;
    OSM_NULLSYSCALL;
    OSM_NULLSYSCALL;
    OSM_NULLSYSCALL;
  }
  if (gettimeofday (&tv, NULL) == -1)
  {
    return -1;
  }
  return (double) ((tv.tv_sec - sec_begin) * 1000000000 + (tv.tv_usec -
                                                           msec_begin) * 1000)
         / (double) (iterations*10);
}