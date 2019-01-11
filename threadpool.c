#include "threadpool.h"
#include <pthread.h>


threadpool *create_threadpool(int num_of_threads, int queue_size){

	threadpool *my_threadpool;
	if((my_threadpool = malloc(sizeof(threadpool))) == NULL) {
        perror("Error in allocatin memory for threadpool!\n");
        exit(0);

    }
    //////////////////////////////////////////////////////////////////////////////
    /////////////initialize threadpool///////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////
   	my_threadpool->thread_counter = 0;
   	my_threadpool->queue_size = queue_size;
   	my_threadpool->head = 0;
   	my_threadpool->tail = 0;
   	my_threadpool->pending_tasks = 0;
   	my_threadpool->started = 0;
   	my_threadpool->shutdown = 0;

   	my_threadpool->threads = malloc(sizeof(pthread_t)*num_of_threads);
   	my_threadpool->queue = malloc(sizeof(thread_task)*queue_size);

   	/////////////////////////////////////////////////////////////////////////////
   	//////////////////////initiliaze mutexes and conditional variables//////////
   	///////////////////////////////////////////////////////////////////////////
   	pthread_mutex_init(&(my_threadpool->locker), NULL);
   	pthread_cond_init(&(my_threadpool->signal), NULL);

   	for(int i = 0; i < num_of_threads; i++){
   		if(pthread_create(&(my_threadpool->threads[i]), NULL, thread_worker, (void*)my_threadpool) != 0){
   			return NULL;
   		}
   	my_threadpool->thread_counter++;
   	my_threadpool->started++;
   	}
   	printf("created");
   	return my_threadpool;
}

int add_task(threadpool *my_threadpool, void (*function)(void *), void *argument, int flags){
	
	printf("mphka");
	if(pthread_mutex_lock(&(my_threadpool->locker)) !=0){
		return THREADPOOL_LOCK_FAIL;
	}

	int next = (my_threadpool->tail + 1) % my_threadpool->queue_size;
	
	do{
		
		if(my_threadpool->pending_tasks == my_threadpool->queue_size){
			return THREADPOOL_QUEUE_FULL;
		}

		if(my_threadpool->shutdown){
			return THREADPOOL_SHUTDOWN;
		}

		my_threadpool->queue[my_threadpool->tail].function = function;
		my_threadpool->queue[my_threadpool->tail].argument = argument;
		my_threadpool->tail = next;
		my_threadpool->pending_tasks++;

		if(pthread_cond_signal(&(my_threadpool->signal)) != 0){
			return THREADPOOL_LOCK_FAIL;
		}

	}while(0);

	if(pthread_mutex_unlock(&(my_threadpool->locker)) != 0){
		return THREADPOOL_LOCK_FAIL;
	}

	return 0;
}

int destroy_threadpool(threadpool *my_threadpool, int flags){

	do{

		if(my_threadpool->shutdown){
			return THREADPOOL_SHUTDOWN;
		}

		my_threadpool->shutdown = (flags & THREADS_SHUT) ? GRACEFULL_SHUTDOWN : IMMEDIATE_SHUTDOWN;

		if((pthread_cond_broadcast(&(my_threadpool->signal)) != 0)){
			return THREADPOOL_LOCK_FAIL;
		}

		if((pthread_mutex_unlock(&(my_threadpool->locker)) != 0)){
			return THREADPOOL_LOCK_FAIL;
		}

		for(int i = 0; i < my_threadpool->pending_tasks; i++){
			if(pthread_join(my_threadpool->threads[i], NULL) != 0){
				return THREADPOOL_ERROR;
			}
		}
	}while(0);

	free_threadpool(my_threadpool);
	return 0;
}

int free_threadpool(threadpool *my_threadpool){

	free(my_threadpool->threads);
	free(my_threadpool->queue);

	pthread_mutex_lock(&(my_threadpool->locker));
	pthread_mutex_destroy(&(my_threadpool->locker));
	pthread_cond_destroy(&(my_threadpool->signal));

	free(my_threadpool);
	return 0;
}

void *thread_worker(void *pool){

	threadpool *my_threadpool = (threadpool *)pool;
	thread_task task;

	while(1){

		pthread_mutex_lock(&(my_threadpool->locker));

		while((my_threadpool->pending_tasks == 0) && (!my_threadpool->shutdown)){
			pthread_cond_wait(&(my_threadpool->signal), &(my_threadpool->locker));
		}
		if( (my_threadpool->shutdown == IMMEDIATE_SHUTDOWN) || ((my_threadpool->shutdown == GRACEFULL_SHUTDOWN) && (my_threadpool->pending_tasks == 0)) ){
			break;
		}

		task.function = my_threadpool->queue[my_threadpool->head].function;
		task.argument = my_threadpool->queue[my_threadpool->head].argument;
		my_threadpool->head = (my_threadpool->head + 1) % my_threadpool->queue_size;
		my_threadpool->pending_tasks--;

		pthread_mutex_unlock(&(my_threadpool->locker));
		printf("im here\n");
		(*(task.function))(task.argument);
	}

	my_threadpool->started--;
	pthread_mutex_unlock(&(my_threadpool->locker));
	pthread_exit(NULL);

}
