#include "jobs.h"

void HistogramJob(void *arguments){

	HistJobArgs *args = arguments;

	args->HistR->arrayHist = calloc(NUMBUCKETS, sizeof(int));
	args->HistS->arrayHist = calloc(NUMBUCKETS, sizeof(int));
	int hashed_value;
	printf("startR %d endR %d\n",args->startR, args->endR);
	for(int i = args->startR; i < args->endR; i++){

		hashed_value = args->relationR->tuples[i].payload % NUMBUCKETS;
		args->HistR->arrayHist[hashed_value]++;
	}

	for(int i = args->startS; i < args->endS; i++){
		hashed_value = args->relationS->tuples[i].payload % NUMBUCKETS;
		args->HistS->arrayHist[hashed_value]++;
	}

}
///////////////////////////////////////////////////////////////////////////////////

void *PartitionJob(void *arguments){

	PartitionArgs *args = arguments;
	int hashed_value;

	for(int i = args->start; i < args->end; i++){

		hashed_value = args->relR->tuples[i].payload % NUMBUCKETS;
		args->reordered_R->tuples[args->psum[hashed_value]] = args->relR->tuples[i];
		args->psum[hashed_value]++;
	}
}
////////////////////////////////////////////////////////////////////////////////////
void *JoinJob(void *arguments){

	///////////////////ARGUMENTS//////////////////////////////////////////////////////
	struct 	JoinJobArgs *args = arguments;
	int bucket = args->bucket_index;
	struct relation *rel_R = args->reordered_R;
	struct relation *rel_S = args->reordered_S;
	struct my_list *list = args->new_list;
	//////////////////////////////////////////////////////////////////////////////////
	int results_counter;
	if(args->index[bucket].minTuples == R){

		for(int j = args->psumS[bucket]; j < args->psumS[bucket] + args->histS[bucket]; j++ ){

			int hashed_value = rel_S->tuples[j].payload % args->index[bucket].bucketSize;
			int possible_position = args->index[bucket].bucket[hashed_value];

			if(possible_position == -1){
				continue;
			}
			else{

				int actualPositionInR = args->psumR[bucket] + possible_position; 

				if(rel_S->tuples[j].payload = rel_R->tuples[actualPositionInR].payload){
					list = add_to_buff(list, rel_R->tuples[actualPositionInR].key, rel_S->tuples[j].key);
					results_counter++;
				}

				possible_position = args->index[bucket].chain[possible_position];

				while(possible_position != -2){
					actualPositionInR = args->psumR[bucket] + possible_position;

					if(rel_S->tuples[j].payload = rel_R->tuples[actualPositionInR].payload){

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

	//struct result *join_result;
	//result =  malloc(sizeof())

}


//////////////////////////////////////////////////////////////////////////////////
///////////These are helper functions not to be executed by threads///////////////
//////////////////////////////////////////////////////////////////////////////////

int *join_partitioned_hists(PartitionedHist *Hist, int size){

	int *totalHist;
	totalHist = calloc(NUMBUCKETS, sizeof(int));
	for(int i = 0; i < NUMBUCKETS; i++){
		for(int j = 0; j < size; j++){
			totalHist[i] += Hist[j].arrayHist[i];
		}
	}

	return totalHist;
}
