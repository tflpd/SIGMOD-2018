#ifndef _JOBSCHEDULER_H_
#define _JOBSCHEDULER_H_

#define THREADPOOL_ERROR -1
#define THREADPOOL_LOCK_ERROR -2
#define THREADPOOL_FULL_QUEUE -3
#define THREADPOOL_SHUTDOWN -4
#define THREADPOOL_THREAD_ERROR -5
#define GRACEFULL_SHUTDOWN 2
#define IMMEDIATE_SHUTDOWN 1
#define GRACEFULL 1

typedef struct Job{
    void (*function)(void *);
    void *argument;
} Job;


typedef struct JobScheduler {
  pthread_mutex_t lock_mutex;
  pthread_cond_t signal;
  pthread_mutex_t waiter;
  pthread_cond_t waiter_cond;
  pthread_t *threads;
  Job *queue;
  int thread_count;
  int amount_of_jobs;
  int queue_size;
  int finished;
  int head;
  int tail;
  int count;
  int shutdown;
  int started;
} JobScheduler;



static void *worker_thread(void *threadpool);
int free_scheduler(JobScheduler *scheduler);
int wait_all_tasks(JobScheduler *,int);
JobScheduler *Scheduler_Init(int thread_count, int queue_size, int flags);
int add_new_job(JobScheduler *scheduler, void (*routine)(void *), void *arg);
int destroy_scheduler(JobScheduler *scheduler, int flags);



#endif
