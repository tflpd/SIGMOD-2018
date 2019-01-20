#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include "jobscheduler.h"




int wait_all_tasks(JobScheduler *scheduler, int flags){

      int i, err = 0;

      if(scheduler == NULL) {
          return THREADPOOL_ERROR;
      }

      if(pthread_mutex_lock(&(scheduler->lock_mutex)) != 0) {
          return THREADPOOL_LOCK_ERROR;
      }

      do {
          if(scheduler->shutdown) {
              err = THREADPOOL_SHUTDOWN;
              break;
          }

          scheduler->shutdown = (flags & GRACEFULL) ?
              GRACEFULL_SHUTDOWN : IMMEDIATE_SHUTDOWN;

          if((pthread_cond_broadcast(&(scheduler->signal)) != 0) ||
             (pthread_mutex_unlock(&(scheduler->lock_mutex)) != 0)) {
              err = THREADPOOL_LOCK_ERROR;
              break;
          }

          for(i = 0; i < scheduler->thread_count; i++) {
              if(pthread_join(scheduler->threads[i], NULL) != 0) {
                  err = THREADPOOL_THREAD_ERROR;
              }
          }
      } while(0);

}

JobScheduler *Scheduler_Init(int thread_count, int queue_size, int flags)
{
    JobScheduler *scheduler;

    if((scheduler = (JobScheduler *)malloc(sizeof(JobScheduler))) == NULL) {
        perror("Error in initializing scheduler\n");
        return NULL;
    }

    /* Initialize */
    scheduler->amount_of_jobs = 0;
    scheduler->finished =0;
    scheduler->thread_count = 0;
    scheduler->queue_size = queue_size;
    scheduler->head = scheduler->tail = scheduler->count = 0;
    scheduler->shutdown = scheduler->started = 0;
    scheduler->threads = malloc(sizeof(pthread_t) * thread_count);
    scheduler->queue = malloc(sizeof(Job) * queue_size);
    pthread_mutex_init(&(scheduler->lock_mutex), NULL);
    pthread_mutex_init(&(scheduler->waiter), NULL);
    pthread_cond_init(&(scheduler->waiter_cond), NULL);
    pthread_cond_init(&(scheduler->signal), NULL);
    /* Initialize mutex and conditional variable first */

    /* Start worker threads */
    for(int i = 0; i < thread_count; i++) {
        if(pthread_create(&(scheduler->threads[i]), NULL, worker_thread, (void*)scheduler) != 0) {
            perror("Error in initializing threads\n");
            return NULL;
        }
        scheduler->thread_count++;
        scheduler->started++;
    }

    return scheduler;

}

int add_new_job(JobScheduler *scheduler, void (*function)(void *), void *argument)
{
    int err = 0;
    int next;

    if(scheduler == NULL || function == NULL) {
        return THREADPOOL_ERROR;
    }

    if(pthread_mutex_lock(&(scheduler->lock_mutex)) != 0) {
        return THREADPOOL_LOCK_ERROR;
    }

    next = (scheduler->tail + 1) % scheduler->queue_size;

    do {
        /* Are we full ? */
        if(scheduler->count == scheduler->queue_size) {
            err = THREADPOOL_FULL_QUEUE;
            break;
        }

        /* Are we shutting down ? */
        if(scheduler->shutdown) {
            err = THREADPOOL_SHUTDOWN;
            break;
        }

        /* Add task to queue */
        scheduler->queue[scheduler->tail].function = function;
        scheduler->queue[scheduler->tail].argument = argument;
        scheduler->tail = next;
        scheduler->count += 1;
        scheduler->amount_of_jobs++;

        /* pthread_cond_broadcast */
        if(pthread_cond_signal(&(scheduler->signal)) != 0) {
            err = THREADPOOL_LOCK_ERROR;
            break;
        }
    } while(0);

    if(pthread_mutex_unlock(&scheduler->lock_mutex) != 0) {
        err = THREADPOOL_LOCK_ERROR;
    }

    return err;
}

int destroy_scheduler(JobScheduler *scheduler, int flags)
{
    int i, err = 0;

    if(scheduler == NULL) {
        return THREADPOOL_ERROR;
    }

    if(pthread_mutex_lock(&(scheduler->lock_mutex)) != 0) {
        return THREADPOOL_LOCK_ERROR;
    }

    do {
        /* Already shutting down */
        if(scheduler->shutdown) {
            err = THREADPOOL_SHUTDOWN;
            break;
        }

        scheduler->shutdown = (flags & GRACEFULL) ?
            GRACEFULL_SHUTDOWN : IMMEDIATE_SHUTDOWN;

        /* Wake up all worker threads */
        if((pthread_cond_broadcast(&(scheduler->signal)) != 0) ||
           (pthread_mutex_unlock(&(scheduler->lock_mutex)) != 0)) {
            err = THREADPOOL_LOCK_ERROR;
            break;
        }

        /* Join all worker thread */
        for(i = 0; i < scheduler->thread_count; i++) {
            if(pthread_join(scheduler->threads[i], NULL) != 0) {
                err = THREADPOOL_THREAD_ERROR;
            }
        }
    } while(0);

    /* Only if everything went well do we deallocate the scheduler */
    if(!err) {
        free_scheduler(scheduler);
    }
    return err;
}

int free_scheduler(JobScheduler *scheduler)
{
    if(scheduler == NULL || scheduler->started > 0) {
        return -1;
    }

    /* Did we manage to allocate ? */
    if(scheduler->threads) {
        free(scheduler->threads);
        free(scheduler->queue);

        /* Because we allocate scheduler->threads after initializing the
           mutex and condition variable, we're sure they're
           initialized. Let's lock_mutex the mutex just in case. */
        pthread_mutex_lock(&(scheduler->lock_mutex));
        pthread_mutex_destroy(&(scheduler->lock_mutex));
        pthread_cond_destroy(&(scheduler->signal));
    }
    free(scheduler);
    return 0;
}


static void *worker_thread(void *threadpool)
{
    JobScheduler *scheduler = (JobScheduler *)threadpool;
    Job task;

    for(;;) {
        /* lock_mutex must be taken to wait on conditional variable */
        pthread_mutex_lock(&(scheduler->lock_mutex));

        /* Wait on condition variable, check for spurious wakeups.
           When returning from pthread_cond_wait(), we own the lock_mutex. */
        while((scheduler->count == 0) && (!scheduler->shutdown)) {
            pthread_cond_wait(&(scheduler->signal), &(scheduler->lock_mutex));
        }

        if((scheduler->shutdown == IMMEDIATE_SHUTDOWN) ||
           ((scheduler->shutdown == GRACEFULL_SHUTDOWN) &&
            (scheduler->count == 0))) {
            break;
        }

        /* Grab our task */
        task.function = scheduler->queue[scheduler->head].function;
        task.argument = scheduler->queue[scheduler->head].argument;
        scheduler->head = (scheduler->head + 1) % scheduler->queue_size;
        scheduler->count -= 1;

        /* Unlock */
        pthread_mutex_unlock(&(scheduler->lock_mutex));

        /* Get to work */
        (*(task.function))(task.argument);
    }
    scheduler->started--;

    pthread_mutex_unlock(&(scheduler->lock_mutex));
    pthread_exit(NULL);
    return(NULL);
}
