#include "jobs.h"
#include <pthread.h>
#define NUMS 128

pthread_mutex_t global = PTHREAD_MUTEX_INITIALIZER;


void HistogramJob(void *arguments){
	HistJobArgs *args = (HistJobArgs*)arguments;

	int hashed_value;
	//printf("startR %d endR %d\n",args->startR, args->endR);
	for(int i = args->startR; i < args->endR; i++){

		hashed_value = args->relationR->tuples[i].payload % NUMBUCKETS;
		//printf("%d haha\n", args->relationR->num_tuples);
		args->HistR[hashed_value]++;
	}

	for(int i = args->startS; i < args->endS; i++){
		hashed_value = args->relationS->tuples[i].payload % NUMBUCKETS;
		args->HistS[hashed_value]++;
	}

}

void PartitionJob(void *arguments){

	PartitionArgs *args = (PartitionArgs*) arguments;
    //pthread_mutex_init(&global,NULL);
  pthread_mutex_lock(&global);
	int hashed_value;
	// printf("startR %d endR %d\n",args->startR, args->endR);
	for(int i = args->startR; i < args->endR; i++){

		hashed_value = args->relR->tuples[i].payload % NUMBUCKETS;
		args->reordered_R->tuples[args->psumR[hashed_value]].key = args->relR->tuples[i].key;
		args->reordered_R->tuples[args->psumR[hashed_value]].payload = args->relR->tuples[i].payload;

		args->psumR[hashed_value]++;
	}
	for(int i = args->startS; i < args->endS; i++){

		hashed_value = args->relS->tuples[i].payload % NUMBUCKETS;
		args->reordered_S->tuples[args->psumS[hashed_value]] = args->relS->tuples[i];
		args->psumS[hashed_value]++;
	}
	pthread_mutex_unlock(&global);
    //pthread_mutex_destroy(&global);
}
////////////////////////////////////////////////////////////////////////////////////
void JoinJob(void *arguments){

	///////////////////ARGUMENTS//////////////////////////////////////////////////////
	struct 	JoinJobArgs *args = arguments;
	int bucket = args->bucket_index;
	struct relation *rel_R = args->reordered_R;
	struct relation *rel_S = args->reordered_S;
	struct my_list *list = list_init(NUMS);
	//////////////////////////////////////////////////////////////////////////////////
	int results_counter=0;
	if(args->index[bucket].minTuples == R){

		for(int j = args->psumS[bucket]; j < args->psumS[bucket] + args->histS[bucket]; j++ ){

			int hashed_value = rel_S->tuples[j].payload % args->index[bucket].bucketSize;
			int possible_position = args->index[bucket].bucket[hashed_value];

			if(possible_position == -1){
				continue;
			}
			else{

				int actualPositionInR = args->psumR[bucket] + possible_position;

				if(rel_S->tuples[j].payload == rel_R->tuples[actualPositionInR].payload){
					list = add_to_buff(list, rel_R->tuples[actualPositionInR].key, rel_S->tuples[j].key);
					results_counter++;
				}

				possible_position = args->index[bucket].chain[possible_position];

				while(possible_position != -2){
					actualPositionInR = args->psumR[bucket] + possible_position;

					if(rel_S->tuples[j].payload == rel_R->tuples[actualPositionInR].payload){

						list = add_to_buff(list, rel_R->tuples[actualPositionInR].key, rel_S->tuples[j].key);
						results_counter++;
					}
					possible_position = args->index[bucket].chain[possible_position];
				}
			}
		}
	}
	else if(args->index[bucket].minTuples == S){

		for (int j = args->psumR[bucket]; j < args->psumR[bucket] + args->histR[bucket]; j++){

			int hashed_value = rel_R->tuples[j].payload % args->index[bucket].bucketSize;
			int possible_position = args->index[bucket].bucket[hashed_value];

			if(possible_position == -1){
				continue;
			}
			else{
				int actualPositionInS = args->psumS[bucket] + possible_position;

				if(rel_R->tuples[j].payload == rel_S->tuples[actualPositionInS].payload){
					list = add_to_buff(list, rel_R->tuples[j].key, rel_S->tuples[actualPositionInS].key);
					results_counter++;
				}

				possible_position = args->index[bucket].chain[possible_position];

				while(possible_position != -2){

					actualPositionInS = args->psumS[bucket] + possible_position;
					if(rel_R->tuples[j].payload == rel_S->tuples[actualPositionInS].payload){

						list = add_to_buff(list, rel_R->tuples[j].key, rel_S->tuples[actualPositionInS].key);
						results_counter++;
					}
					possible_position = args->index[bucket].chain[possible_position];

				}
			}
		}
	}

	struct result *join_result = args->join_result;
	//printf("results counter is %d\n",results_counter);
	if(results_counter > 0){
		join_result->rowIDsR = malloc(sizeof(int)*results_counter);
		join_result->rowIDsS = malloc(sizeof(int)*results_counter);
		join_result->numRows = results_counter;
		struct lnode *tmp;
		tmp = list->head;
		int resultsIterCounter;
		resultsIterCounter = 0;

		if (tmp != NULL)
		{
			while(tmp->key < list->current->key){
				for (int i = 0; i < tmp->counter; ++i)
				{
					join_result->rowIDsR[resultsIterCounter] = tmp->buffer[i].keyR;
					join_result->rowIDsS[resultsIterCounter] = tmp->buffer[i].keyS;
					resultsIterCounter++;
				}
				tmp = tmp->next;
			}
			for (int i = 0; i < tmp->counter; ++i)
			{
				join_result->rowIDsR[resultsIterCounter] = tmp->buffer[i].keyR;
				join_result->rowIDsS[resultsIterCounter] = tmp->buffer[i].keyS;
				resultsIterCounter++;
			}
		}

	}
	else{
		join_result->numRows = 0;
	}


	delete_list(list);


}
