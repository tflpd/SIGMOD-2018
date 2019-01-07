#ifndef __THREADPOOL_H__
#define __THREADPOOL_H__

#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>

#define THREADPOOL_ERROR -1
#define THREADPOOL_LOCK_FAIL -2
#define THREADPOOL_QUEUE_FULL -3
#define THREADPOOL_SHUTDOWN -4
#define THREADPOOL_THREAD_FAIL -5
///////////////////////////////////
#define THREADS_SHUT 1
#define IMMEDIATE_SHUTDOWN 1
#define GRACEFULL_SHUTDOWN 2
///////////////////////////////////
#define MAX_THREADS 64
#define MAX_QUEUE 65536

typedef struct task{
  void (*function)(void *);
  void *argument;
} thread_task;

typedef struct threadpool{
  pthread_mutex_t locker;
  pthread_cond_t signal;
  pthread_t *threads;
  thread_task *queue;
  int thread_counter;
  int queue_size;
  int tail;
  int head;
  int started;
  int shutdown;
  int pending_tasks;
} threadpool;

////////////////////////////////////////////////////////////////////////////
///////////////////////functions///////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////


threadpool *create_threadpool(int, int);
int add_task(threadpool *, void (*routine)(void *), void *, int);
int destroy_threadpool(threadpool *, int);
int free_threadpool(threadpool *);
void *thread_worker(void *);


#endif
