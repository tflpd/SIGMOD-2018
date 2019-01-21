#include "structs.h"
#include "myList.h"
#include "storage.h"
#include "jobscheduler.h"
#include "jobs.h"
#define NUMTHREAD 8

/*---------------------------- FIRST PART RE WRITTEN ----------------------------*/

// Checks whether the provided number is prime or not
int is_prime(int num){

     if(num <= 1)
     	return 0;
     if(num % 2 == 0 && num > 2)
     	return 0;

     for(int i = 3; i < num / 2; i+= 2){
     	if (num % i == 0)
        	return 0;
     }

     return 1;
}
struct result *RadixHashJoinParallel (struct relation *relationR, struct relation *relationS)
{

  JobScheduler *Scheduler = Scheduler_Init (NUMTHREAD, NUMTHREAD + 1, 0);
  HistJobArgs *h_args;
  int **histsR, **histsS;
  histsR = malloc (sizeof (int *) * NUMTHREAD);
  histsS = malloc (sizeof (int *) * NUMTHREAD);
  for (int i = 0; i < NUMTHREAD; i++)
    {
      histsR[i] = calloc (NUMBUCKETS, sizeof (int));
      histsS[i] = calloc (NUMBUCKETS, sizeof (int));
    }
  int *psumR = calloc (NUMBUCKETS, sizeof (int));
  int *psumS = calloc (NUMBUCKETS, sizeof (int));
  int *psumRR = calloc (NUMBUCKETS, sizeof (int));
  int *psumSS = calloc (NUMBUCKETS, sizeof (int));
  ////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////First part///////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////
  h_args = malloc (sizeof (HistJobArgs) * NUMTHREAD);
  int tuplesR = relationR->num_tuples;
  int tuplesS = relationS->num_tuples;
  int parted_r = tuplesR / NUMTHREAD;
  int parted_s = tuplesS / NUMTHREAD;
  int endr, ends;
  endr = parted_r;
  ends = parted_s;

  for (int i = 0; i < NUMTHREAD; i++)
    {
      h_args[i].relationR = relationR;
      h_args[i].relationS = relationS;
      h_args[i].startS = i * parted_s;
      h_args[i].startR = i * parted_r;
      h_args[i].endR = endr;
      h_args[i].endS = ends;
      h_args[i].HistR = histsR[i];
      h_args[i].HistS = histsS[i];
      if (i == NUMTHREAD - 1)
	{
	  int lastR = tuplesR;
	  int lastS = tuplesS;
	  h_args[i].endR = lastR;
	  h_args[i].endS = lastS;
	}

      endr += parted_r;
      ends += parted_s;
    }



  for (int i = 0; i < NUMTHREAD; i++)
    {

      add_new_job (Scheduler, &HistogramJob, (void *) &h_args[i]);
    }
  wait_all_tasks (Scheduler, 1);
  int *totalHistR, *totalHistS;
  totalHistR = calloc (NUMBUCKETS, sizeof (int));
  totalHistS = calloc (NUMBUCKETS, sizeof (int));
  for (int i = 0; i < NUMBUCKETS; i++)
    {
      for (int j = 0; j < NUMTHREAD; j++)
	{
	  totalHistR[i] += histsR[j][i];
	  totalHistS[i] += histsS[j][i];
	}
    }


  //here free the arguments
  destroy_scheduler (Scheduler, 1);
  for (int i = 0; i < NUMTHREAD; i++)
    {
      free (histsS[i]);
      free (histsR[i]);
    }
  free (h_args);
  free (histsS);
  free (histsR);
  ///////////////////////////////////////////////////////////////////////////
  //////////////////////////Second part//////////////////////////////////////
  ///////////////////////////////////////////////////////////////////////////
  Scheduler = Scheduler_Init (NUMTHREAD, NUMTHREAD + 1, 0);
  PartitionArgs *p_args = malloc (sizeof (PartitionArgs) * NUMTHREAD);
  struct relation *reorderedR = malloc (sizeof (struct relation));
  struct relation *reorderedS = malloc (sizeof (struct relation));

  reorderedR->tuples = malloc (sizeof (struct tuple) * tuplesR);
  reorderedS->tuples = malloc (sizeof (struct tuple) * tuplesS);
  int last = 0;
  for (int i = 0; i < NUMBUCKETS; ++i)
    {
      if (totalHistR[i] != 0)
	{
	  psumR[i] = last;
	  psumRR[i] = last;
	  last += totalHistR[i];
	}
    }
  last = 0;
  for (int i = 0; i < NUMBUCKETS; ++i)
    {
      if (totalHistS[i] != 0)
	{
	  psumS[i] = last;
	  psumSS[i] = last;
	  last += totalHistS[i];
	}
    }
  tuplesR = relationR->num_tuples;
  tuplesS = relationS->num_tuples;
  parted_r = tuplesR / NUMTHREAD;
  parted_s = tuplesS / NUMTHREAD;
  endr = parted_r;
  ends = parted_s;
  for (int i = 0; i < NUMTHREAD; i++)
    {
      p_args[i].relR = relationR;
      p_args[i].relS = relationS;
      p_args[i].reordered_R = reorderedR;
      p_args[i].reordered_S = reorderedS;
      p_args[i].startS = i * parted_s;
      p_args[i].startR = i * parted_r;
      p_args[i].endR = endr;
      p_args[i].endS = ends;
      p_args[i].psumR = psumR;
      p_args[i].psumS = psumS;
      if (i == NUMTHREAD - 1)
	{
	  int lastR = tuplesR;
	  int lastS = tuplesS;
	  p_args[i].endR = lastR;
	  p_args[i].endS = lastS;
	}
      endr += parted_r;
      ends += parted_s;
    }
  for (int i = 0; i < NUMTHREAD; i++)
    {
      add_new_job (Scheduler, &PartitionJob, (void *) &p_args[i]);
    }

  wait_all_tasks (Scheduler, 1);
  destroy_scheduler (Scheduler, 1);
  free (p_args);
  ///////////////////////////////////////////////////////////////////////////
  ////////////////////////////Third part/////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////
  Scheduler = Scheduler_Init (NUMBUCKETS, NUMBUCKETS + 1, 0);
  struct bucketIndex *index;
  index = malloc (sizeof (struct bucketIndex) * NUMBUCKETS);
  for (int i = 0; i < NUMBUCKETS; ++i)
    {
      index[i].minTuples = 0;
    }
  for (int i = 0; i < NUMBUCKETS; ++i)
    {
      // If it is a bucket with values fot both relations
      if ((totalHistR[i] != 0) && (totalHistS[i] != 0))
	{
	  // If R is the relation with the least tuples
	  if (totalHistR[i] <= totalHistS[i])
	    {
	      index[i].minTuples = R;
	      index[i].numTuples = totalHistR[i];
	    }
	  else
	    {
	      index[i].minTuples = S;
	      index[i].numTuples = totalHistS[i];
	    }

	  // Find the first prime after the size of the bucket to be used as size for the bucket array
	  int x = index[i].numTuples;
	  while (is_prime (x) == 0)
	    x++;
	  index[i].bucketSize = x;

	  // Allocate and initialize the bucket array
	  index[i].bucket = malloc (sizeof (int) * x);
	  for (int j = 0; j < x; ++j)
	    {
	      index[i].bucket[j] = -1;
	    }

	  // Allocate and initialize the chain array
	  index[i].chain = malloc (sizeof (int) * index[i].numTuples);	//ISWS +1 EDW?
	  for (int j = 0; j < index[i].numTuples; ++j)
	    {
	      index[i].chain[j] = -1;
	    }

	  // If relation R will have the index of that bucket
	  if (index[i].minTuples == R)
	    {
	      // For every tuple of that bucket
	      for (int j = psumRR[i]; j < psumRR[i] + totalHistR[i]; ++j)
		{
		  // Find its hash
		  int hashValue = reorderedR->tuples[j].payload % x;
		  int prevChainIndex = index[i].bucket[hashValue];
		  // If it is the first tuple to hash on that cell of bucket array
		  if (prevChainIndex == -1)
		    {
		      //printf("AXNE\n");
		      int virtualPosition = j - psumRR[i];
		      index[i].bucket[hashValue] = virtualPosition;
		      index[i].chain[virtualPosition] = -2;
		    }
		  else
		    {
		      int virtualPosition = j - psumRR[i];
		      //printf("MPIKA %d\n", virtualPosition);
		      index[i].chain[virtualPosition] = prevChainIndex;
		      index[i].bucket[hashValue] = virtualPosition;
		      //printf("Sto index %d thesi %d timi %d\n", i, hashValue, virtualPosition);
		    }

		}
	    }
	  else			// Else relation S will have the index for that bucket
	    {
	      //printf("EDW OXI\n");
	      // For every tuple of that bucket
	      for (int j = psumSS[i]; j < psumSS[i] + totalHistS[i]; ++j)
		{
		  // Find its hash
		  int hashValue = reorderedS->tuples[j].payload % x;
		  int prevChainIndex = index[i].bucket[hashValue];
		  // If it is the first tuple to hash on that cell of bucket array
		  if (prevChainIndex == -1)
		    {
		      int virtualPosition = j - psumSS[i];
		      index[i].bucket[hashValue] = virtualPosition;
		      index[i].chain[virtualPosition] = -2;
		    }
		  else
		    {
		      int virtualPosition = j - psumSS[i];
		      index[i].chain[virtualPosition] = prevChainIndex;
		      index[i].bucket[hashValue] = virtualPosition;
		    }

		}
	    }
	}
    }
  struct result *results;
  results = malloc (sizeof (struct result) * NUMBUCKETS);
  JoinArgs *j_args;
  j_args = malloc (sizeof (JoinArgs) * NUMBUCKETS);

  for (int i = 0; i < NUMBUCKETS; i++)
    {
      j_args[i].bucket_index = i;
      j_args[i].index = index;
      j_args[i].psumR = psumRR;
      j_args[i].psumS = psumSS;
      j_args[i].histR = totalHistR;
      j_args[i].histS = totalHistS;
      j_args[i].reordered_R = reorderedR;
      j_args[i].reordered_S = reorderedS;
      //j_args[i].new_list = &list[i];
      j_args[i].join_result = &results[i];
    }
  for (int i = 0; i < NUMBUCKETS; i++)
    {
      add_new_job (Scheduler, &JoinJob, (void *) &j_args[i]);
    }
  ////////////////////////////////////////////////////////////////////////
  //wait_all_tasks(Scheduler,1);
  destroy_scheduler (Scheduler, 1);
  int total_rows = 0;
  struct result *final_result = malloc (sizeof (struct result));
  for (int i = 0; i < NUMBUCKETS; i++)
    {
      total_rows += results[i].numRows;
    }
  final_result->rowIDsR = malloc (sizeof (int) * total_rows);
  final_result->rowIDsS = malloc (sizeof (int) * total_rows);
  final_result->numRows = total_rows;
  int final_index = 0;
  for (int i = 0; i < NUMBUCKETS; i++)
    {
      for (int j = 0; j < results[i].numRows; j++)
	{
	  final_result->rowIDsR[final_index] = results[i].rowIDsR[j];
	  final_result->rowIDsS[final_index] = results[i].rowIDsS[j];
	  final_index++;
	}
    }

  free (psumR);
  free (psumS);
  free (psumRR);
  free (psumSS);
// for(int i = 0; i < NUMBUCKETS; i++){
//   if(results[i].rowIDsR != NULL){
//
//     free(results[i].rowIDsR);
//   }
//   if(results[i].rowIDsS != NULL){
//
//     free(results[i].rowIDsS);
//   }
// }
  free (results);
  free (reorderedR->tuples);
  free (reorderedS->tuples);
// for (int i = 0; i < NUMBUCKETS; ++i)
// {
//   if (index[i].minTuples != 0)
//   {
//     free(index[i].bucket);
//     free(index[i].chain);
//   }
// }
// for(int i=0; i<NUMBUCKETS; i++){
//   if(results[i].numRows > 0){
//     free(results[i].rowIDsS);
//     free(results[i].rowIDsR);
//   }
// }
  free (index);
  free (j_args);
  return final_result;
}

struct result* RadixHashJoin(struct relation *relationR, struct relation *relationS){

	// Allocating and initializing with 0's the two histograms
	int *histR;
	int *histS;
	histR = calloc(NUMBUCKETS,sizeof(int));
	histS = calloc(NUMBUCKETS,sizeof(int));

	// Allocating and initializing a struct that will show for which bucket to build an index
	struct bucketIndex* index;
	index = malloc(sizeof(struct bucketIndex)*NUMBUCKETS);
	for (int i = 0; i < NUMBUCKETS; ++i)
	{
		index[i].minTuples = 0;
	}

	// Allocating and initializing with 0's the two psums
	int *psumR;
	int *psumS;
	psumR = calloc(NUMBUCKETS,sizeof(int));
	psumS = calloc(NUMBUCKETS,sizeof(int));

	// Allocating and initializing with 0's two copies of psums that we ll need later
	int *psumRR;
	int *psumSS;
	psumRR = calloc(NUMBUCKETS,sizeof(int));
	psumSS = calloc(NUMBUCKETS,sizeof(int));

	// Allocating the two reordered relations
	struct relation *reorderedR;
	struct relation *reorderedS;
	reorderedR = malloc(sizeof(struct relation));
	reorderedR->tuples = malloc(sizeof(struct tuple)*relationR->num_tuples);
	reorderedS = malloc(sizeof(struct relation));
	reorderedS->tuples = malloc(sizeof(struct tuple)*relationS->num_tuples);

	// Filling the two histograms
	for (int i = 0; i < relationR->num_tuples; ++i)
	{
		int hashValue = relationR->tuples[i].payload % NUMBUCKETS;
		histR[hashValue]++;
	}
	for (int i = 0; i < relationS->num_tuples; ++i)
	{
		int hashValue = relationS->tuples[i].payload % NUMBUCKETS;
		histS[hashValue]++;
	}

	// Filling the two psums
	int last = 0;
	for (int i = 0; i < NUMBUCKETS; ++i)
	{
		if (histR[i] != 0)
		{
			psumR[i] = last;
			psumRR[i] = last;
			last += histR[i];
		}
	}
	last = 0;
	for (int i = 0; i < NUMBUCKETS; ++i)
	{
		if (histS[i] != 0)
		{
			psumS[i] = last;
			psumSS[i] = last;
			last += histS[i];
		}
	}

	// Filling the two reordered relations
	for (int i = 0; i < relationR->num_tuples; ++i)
	{
		int hashValue = relationR->tuples[i].payload % NUMBUCKETS;
		reorderedR->tuples[psumR[hashValue]] = relationR->tuples[i];
		psumR[hashValue]++;
	}
	reorderedR->num_tuples = relationR->num_tuples;
	for (int i = 0; i < relationS->num_tuples; ++i)
	{
		int hashValue = relationS->tuples[i].payload % NUMBUCKETS;
		reorderedS->tuples[psumS[hashValue]] = relationS->tuples[i];
		psumS[hashValue]++;
	}
	reorderedS->num_tuples = relationS->num_tuples;

	/*--------------- End of first part -------------*/

	// Finding buckets with existing tuples and find for which to build an index and build it
	for (int i = 0; i < NUMBUCKETS; ++i)
	{
		// If it is a bucket with values fot both relations
		if ((histR[i] != 0) && (histS[i] != 0))
		{
			// If R is the relation with the least tuples
			if (histR[i] <= histS[i]){
				index[i].minTuples = R;
				index[i].numTuples = histR[i];
			}else{
				index[i].minTuples = S;
				index[i].numTuples = histS[i];
			}

			// Find the first prime after the size of the bucket to be used as size for the bucket array
			int x = index[i].numTuples;
			while(is_prime(x) == 0)
				x++;
			index[i].bucketSize = x;

			// Allocate and initialize the bucket array
			index[i].bucket = malloc(sizeof(int)*x);
			for (int j = 0; j < x; ++j)
			{
				index[i].bucket[j] = -1;
			}

			// Allocate and initialize the chain array
			index[i].chain = malloc(sizeof(int)*index[i].numTuples); //ISWS +1 EDW?
			for (int j = 0; j < index[i].numTuples; ++j)
			{
				index[i].chain[j] = -1;
			}

			// If relation R will have the index of that bucket
			if (index[i].minTuples == R)
			{
				// For every tuple of that bucket
				for (int j = psumRR[i]; j < psumRR[i] + histR[i]; ++j)
				{
					// Find its hash
					int hashValue = reorderedR->tuples[j].payload % x;
					int prevChainIndex = index[i].bucket[hashValue];
					// If it is the first tuple to hash on that cell of bucket array
					if (prevChainIndex == -1)
					{
						//printf("AXNE\n");
						int virtualPosition = j - psumRR[i];
						index[i].bucket[hashValue] = virtualPosition;
						index[i].chain[virtualPosition] = -2;
					}else{
						int virtualPosition = j - psumRR[i];
						//printf("MPIKA %d\n", virtualPosition);
						index[i].chain[virtualPosition] = prevChainIndex;
						index[i].bucket[hashValue] = virtualPosition;
						//printf("Sto index %d thesi %d timi %d\n", i, hashValue, virtualPosition);
					}

				}
			}else// Else relation S will have the index for that bucket
			{
				//printf("EDW OXI\n");
				// For every tuple of that bucket
				for (int j = psumSS[i]; j < psumSS[i] + histS[i]; ++j)
				{
					// Find its hash
					int hashValue = reorderedS->tuples[j].payload % x;
					int prevChainIndex = index[i].bucket[hashValue];
					// If it is the first tuple to hash on that cell of bucket array
					if (prevChainIndex == -1)
					{
						int virtualPosition = j - psumSS[i];
						index[i].bucket[hashValue] = virtualPosition;
						index[i].chain[virtualPosition] = -2;
					}else{
						int virtualPosition = j - psumSS[i];
						index[i].chain[virtualPosition] = prevChainIndex;
						index[i].bucket[hashValue] = virtualPosition;
					}

				}
			}
		}
	}

	/*------ Now that we have the indexes ready we are scanning each
	 bucket that does not have an index and we try to find values on the indexed bucket
	 in order to join them in the result -----*/

	struct my_list* list;
	list = list_init(NUMRESULTS); // NUMRESULTS is the size of the buffer of each list node holding the results
	int resultsCounter = 0; // The number of results that got added in the buffer

	// For each bucket
	for (int i = 0; i < NUMBUCKETS; ++i)
	{
		// Find wich relation has the index
		if (index[i].minTuples == R) // If it is R
		{
			/*for (int j = 0; j < index[i].numTuples; ++j)
			{
				printf("%d\n", index[i].chain[j]);
			}
			for (int j = 0; j < index[i].bucketSize; ++j)
			{
				printf("%d\n", index[i].bucket[j]);
			}*/
			// For each tuple of S that belongs to that bucket
			for (int j = psumSS[i]; j < psumSS[i] + histS[i]; ++j)
			{
				// Find where this tuple hashes
				int hashValue = reorderedS->tuples[j].payload % index[i].bucketSize;
				// And get its first possible position in R through array bucket
				int possiblePosition = index[i].bucket[hashValue];
				// If its -1 it means no value in R hashes there so move on
				if (possiblePosition == -1)
				{
					continue;
				}else// Else at least one value of R has hashed there
				{
					// Calculate the actual position in R of that tuple
					int actualPositionInR = psumRR[i] + possiblePosition;
					// Check if they match and if they do add them to results
					if (reorderedS->tuples[j].payload == reorderedR->tuples[actualPositionInR].payload)
					{
						list = add_to_buff(list, reorderedR->tuples[actualPositionInR].key, reorderedS->tuples[j].key);
						resultsCounter++;
					}
					// After that continue checking array chain
					possiblePosition = index[i].chain[possiblePosition];
					// As far you do not meet 0 (the last tuple of that hash chain) continue searching for matches
					while(possiblePosition != -2){
						actualPositionInR = psumRR[i] + possiblePosition;
						if (reorderedS->tuples[j].payload == reorderedR->tuples[actualPositionInR].payload)
						{
							list = add_to_buff(list, reorderedR->tuples[actualPositionInR].key, reorderedS->tuples[j].key);
							resultsCounter++;
						}
						possiblePosition = index[i].chain[possiblePosition];
					}
				}
			}
		}else if(index[i].minTuples == S) // Else if S has the index we do the exact opposite
		{
			for (int j = psumRR[i]; j < psumRR[i] + histR[i]; ++j)
			{
				int hashValue = reorderedR->tuples[j].payload % index[i].bucketSize;
				int possiblePosition = index[i].bucket[hashValue];

				if (possiblePosition == -1)
				{
					continue;
				}else
				{
					int actualPositionInS = psumSS[i] + possiblePosition;

					if (reorderedR->tuples[j].payload == reorderedS->tuples[actualPositionInS].payload)
					{
						list = add_to_buff(list, reorderedR->tuples[j].key, reorderedS->tuples[actualPositionInS].key);
						resultsCounter++;
					}

					possiblePosition = index[i].chain[possiblePosition];
					while(possiblePosition != -2){
						actualPositionInS = psumSS[i] + possiblePosition;
						if (reorderedR->tuples[j].payload == reorderedS->tuples[actualPositionInS].payload)
						{
							list = add_to_buff(list, reorderedR->tuples[j].key, reorderedS->tuples[actualPositionInS].key);
							resultsCounter++;
						}
						possiblePosition = index[i].chain[possiblePosition];
					}
				}
			}
		}else{// If none has an index
			continue;
		}
	}

	struct result *finalResult;
	finalResult = malloc(sizeof(struct result));
	finalResult->rowIDsR = malloc(sizeof(int)*resultsCounter);
	finalResult->rowIDsS = malloc(sizeof(int)*resultsCounter);
	finalResult->numRows = resultsCounter;

	struct lnode *tmp;
	tmp = list->head;
	int resultsIterCounter;
	resultsIterCounter = 0;

	if (tmp != NULL)
	{
		while(tmp->key < list->current->key){
			for (int i = 0; i < tmp->counter; ++i)
			{
				finalResult->rowIDsR[resultsIterCounter] = tmp->buffer[i].keyR;
				finalResult->rowIDsS[resultsIterCounter] = tmp->buffer[i].keyS;
				resultsIterCounter++;
			}
			tmp = tmp->next;
		}
		for (int i = 0; i < tmp->counter; ++i)
		{
			finalResult->rowIDsR[resultsIterCounter] = tmp->buffer[i].keyR;
			finalResult->rowIDsS[resultsIterCounter] = tmp->buffer[i].keyS;
			resultsIterCounter++;
		}
	}

	// Freeing up all the allocated space
	free(histR);
	free(histS);
	free(psumR);
	free(psumS);
	free(psumRR);
	free(psumSS);

	/*for (int i = 0; i < relationR->num_tuples; ++i)
	{
		free(reorderedR->tuples[i]);
	}*/
	free(reorderedR->tuples);
	free(reorderedR);

	/*for (int i = 0; i < relationS->num_tuples; ++i)
	{
		free(reorderedS->tuples[i]);
	}*/
	free(reorderedS->tuples);
	free(reorderedS);

	for (int i = 0; i < NUMBUCKETS; ++i)
	{
		if (index[i].minTuples != 0)
		{
			free(index[i].bucket);
			free(index[i].chain);
		}
	}
	free(index);

	delete_list(list);


	return finalResult;
}

/*----------------------------  END OF RE WRITTEN FIRST PART----------------------------*/



// Displays a message with all the possible options a user has
void print_welcome_msg(int beginning){

	if(beginning)
		printf("Welcome!\n");
	printf("Please, choose one of the following options:\n");
	printf("* Q: To retrieve data.\n");
	printf("* Exit: To exit the application.\n");
}

/***********************************/
/*** Communication with the user ***/
/***********************************/

struct query create_query(int* table_indeces, int size, char** filters, int size_1, int** sum, int size_2){
	// Construct a query struct
	struct query my_query;

	/*my_query = malloc(sizeof(struct query));
	if(my_query == NULL){
		perror("Memory allocation failed: ");
		exit(-1);
	}*/

	my_query.table_indeces = malloc(sizeof(int)*size);
	if(my_query.table_indeces == NULL){
		perror("Memory allocation failed: ");
		exit(-1);
	}
	for (int i = 0; i < size; ++i)
	{
		my_query.table_indeces[i] = table_indeces[i];
	}

	my_query.size1 = size;

	my_query.filters = malloc(sizeof(char *)*size_1);
	if(my_query.filters == NULL){
		perror("Memory allocation failed: ");
		exit(-1);
	}

	for (int i = 0; i < size_1; ++i)
	{
		my_query.filters[i] = malloc(sizeof(char)*(strlen(filters[i])+1));
		if(my_query.filters[i] == NULL){
			perror("Memory allocation failed: ");
			exit(-1);
		}
		strcpy(my_query.filters[i], filters[i]);
	}

	my_query.size2 = size_1;

	my_query.projections = malloc(sizeof(int *)*size_2);

	if(my_query.projections == NULL){
		perror("Memory allocation failed: ");
		exit(-1);
	}

	for (int i = 0; i < size_2; ++i)
	{
		my_query.projections[i] = malloc(sizeof(int)*2);
		if(my_query.projections[i] == NULL){
			perror("Memory allocation failed: ");
			exit(-1);
		}
		my_query.projections[i][0] = sum[i][0];
		my_query.projections[i][1] = sum[i][1];
	}

	my_query.size3 = size_2;

	return my_query;
}

void freeQuery(struct query myQuery){
	free(myQuery.table_indeces);
	for (int i = 0; i < myQuery.size2; ++i)
	{
		if (myQuery.filters[i] != NULL)
		{
			//free(myQuery.filters[i]);
		}
	}
	free(myQuery.filters);
	for (int i = 0; i < myQuery.size3; ++i)
	{
		free(myQuery.projections[i]);
	}
	free(myQuery.projections);
}

void freeBatch(struct batch *myBatch){
	for (int i = 0; i < myBatch->numQueries; ++i)
	{
		freeQuery(myBatch->queries[i]);
	}
	free(myBatch);
}

void freeResult(struct result *myResult){
	free(myResult->rowIDsR);
	free(myResult->rowIDsS);
	free(myResult);
}

void freeRelation(struct relation *myRelation){
	free(myRelation->tuples);
	free(myRelation);
}

void freeMiddle(struct middle_table *myMiddle){
	free(myMiddle->participants);
	if (myMiddle->rows_size)
	{
		for (int i = 0; i < myMiddle->numb_of_parts; ++i)
		{
			free(myMiddle->rows_id[i]);
		}
	}
	free(myMiddle->rows_id);
}

void freeMiddleTable(struct middle_table *middle, int size){
  for(int i = 0; i < size; i++){
    if(middle[i].numb_of_parts > 0){
      free(middle[i].participants);
      for(int j = 0; j < middle[i].rows_size; j++){
        free(middle[i].rows_id[j]);
      }
      free(middle[i].rows_id);
    }
  }
  free(middle);
}

/*************************************/
/*** Functions that release memory ***/
/*************************************/

/*
This creates a middle and initializes every "participants" member with 0
*/
struct middle_table * create_middle_table(int size)
{
	struct middle_table *middle;
	middle = malloc(sizeof(struct middle_table)*size);
	for (int i=0; i<size; i++)
	{
		//participants = NULL;
		middle[i].numb_of_parts =0;
	}
	return middle;
}

int find_relation(int relation, int *r_array, int size)
{
	for(int i=0; i<size; i++)
	{
		if (r_array[i] == relation)
				return i;
	}
	//if we reach this point the relation does not exist in the array
	return -1;
}

struct result *filterPredicate(struct relation *relationR, int comparingValue, int comparingMode){
	struct my_list* list;
	list = list_init(NUMRESULTS); // NUMRESULTS is the size of the buffer of each list node holding the results
	int resultsCounter = 0; // The number of results that got added in the buffer

	for (int i = 0; i < relationR->num_tuples; ++i)
	{
		if (comparingMode == BIGGER)
		{
			if (relationR->tuples[i].payload > comparingValue)
			{
				list = add_to_buff(list, relationR->tuples[i].key, 0);
				resultsCounter++;
			}
		}
		else if (comparingMode == LESS){
			if (relationR->tuples[i].payload < comparingValue)
			{
				list = add_to_buff(list, relationR->tuples[i].key, 0);
				resultsCounter++;
			}
		}
		else if (comparingMode == EQUAL){
			if (relationR->tuples[i].payload == comparingValue)
			{
				list = add_to_buff(list, relationR->tuples[i].key, 0);
				resultsCounter++;
			}
		}else{
			printf("WRONG COMPARING MODE CODE IN FILTER PREDICATE\n");
			return NULL;
		}
	}

	struct result *finalResult;
	finalResult = malloc(sizeof(struct result));
	finalResult->rowIDsR = malloc(sizeof(int)*resultsCounter);
	finalResult->rowIDsS = malloc(sizeof(int)*resultsCounter);
	finalResult->numRows = resultsCounter;

	struct lnode *tmp;
	tmp = list->head;
	int resultsIterCounter;
	resultsIterCounter = 0;

	if (tmp != NULL)
	{
		while(tmp->key < list->current->key){
			for (int i = 0; i < tmp->counter; ++i)
			{
				finalResult->rowIDsR[resultsIterCounter] = tmp->buffer[i].keyR;
				finalResult->rowIDsS[resultsIterCounter] = 0;
				resultsIterCounter++;
			}
			tmp = tmp->next;
		}
		for (int i = 0; i < tmp->counter; ++i)
		{
			finalResult->rowIDsR[resultsIterCounter] = tmp->buffer[i].keyR;
			finalResult->rowIDsS[resultsIterCounter] = 0;
			resultsIterCounter++;
		}
	}

	delete_list(list);

	return finalResult;
}

// Function to use instead of Join for the extra cases mentionedy
struct result *scanRelations(struct relation *relationR, struct relation *relationS){
	struct my_list* list;
	list = list_init(NUMRESULTS); // NUMRESULTS is the size of the buffer of each list node holding the results
	int resultsCounter = 0; // The number of results that got added in the buffer
	// Iterate through R
	for (int i = 0; i < relationR->num_tuples; ++i)
	{
		if (relationR->tuples[i].payload == relationS->tuples[i].payload)
		{
			// Add them to the results list
			list = add_to_buff(list, relationR->tuples[i].key, relationS->tuples[i].key);
			resultsCounter++;
		}
	}
	/*// If relation R has the least tuples
	if (relationR->num_tuples <= relationS->num_tuples)
	{
		// Iterate through R
		for (int i = 0; i < relationR->num_tuples; ++i)
		{
			if (relationR->tuples[i].payload == relationS->tuples[i].payload)
			{
				// Add them to the results list
				list = add_to_buff(list, relationR->tuples[i].key, relationS->tuples[i].key);
				resultsCounter++;
			}
		}
	}else{// If relation S has the least tuples
		for (int i = 0; i < relationS->num_tuples; ++i)
		{
			if (relationR->tuples[i].payload == relationS->tuples[i].payload)
			{
				// Add them to the results list
				list = add_to_buff(list, relationR->tuples[i].key, relationS->tuples[i].key);
				resultsCounter++;
			}
		}
	}*/

	struct result *finalResult;
	finalResult = malloc(sizeof(struct result));
	finalResult->rowIDsR = malloc(sizeof(int)*resultsCounter);
	finalResult->rowIDsS = malloc(sizeof(int)*resultsCounter);
	finalResult->numRows = resultsCounter;

	struct lnode *tmp;
	tmp = list->head;
	int resultsIterCounter;
	resultsIterCounter = 0;

	if (tmp != NULL)
	{
		while(tmp->key < list->current->key){
			for (int i = 0; i < tmp->counter; ++i)
			{
				finalResult->rowIDsR[resultsIterCounter] = tmp->buffer[i].keyR;
				finalResult->rowIDsS[resultsIterCounter] = tmp->buffer[i].keyS;
				resultsIterCounter++;
			}
			tmp = tmp->next;
		}
		for (int i = 0; i < tmp->counter; ++i)
		{
			finalResult->rowIDsR[resultsIterCounter] = tmp->buffer[i].keyR;
			finalResult->rowIDsS[resultsIterCounter] = tmp->buffer[i].keyS;
			resultsIterCounter++;
		}
	}

	delete_list(list);

	return finalResult;
}

// Function to figure out each projections index in the participants array
// For example if we have as participants array the 1 5 3 and we need to project
// 3.1 5.0 projectionsIndeces will be 2 1 (the indeces of the relations to be projected)
int *findProjectionsIndeces(int *participants, int numb_of_parts, int ** projections, int numProjections, int *table_indeces){
	int *projectionsIndeces;
	projectionsIndeces = malloc(sizeof(int)*numProjections);

	for (int i = 0; i < numProjections; ++i)
	{
		for (int j = 0; j < numb_of_parts; ++j)
		{
			if (projections[i][0] == participants[j])
			{
				projectionsIndeces[i] = j;
				break;
			}
			// CAREFUL MAYBE CORNER CASE TO BE ADDED IF PROJECTION REQUESTED NOT IN PARTICIPANTS
		}
	}

	return projectionsIndeces;
}

void middle_merge(struct middle_table *table1, struct middle_table *table2){
  int total_participants = table1->numb_of_parts + table2->numb_of_parts;
  int first_rows = table1->rows_size;
  int second_rows = table2->rows_size;
  printf("FIRST ROWS %d 2ND ROWS %d\n", first_rows, second_rows);
  int participants1 = table1->numb_of_parts;
  printf("1\n");
  int participants2 = table2->numb_of_parts;
  printf("2\n");
  int index = 0;
  int *merged_parts;
  int **temp_rows_id;
  merged_parts = malloc(sizeof(int)*total_participants);
  printf("3\n");
  temp_rows_id = malloc(sizeof(int*)*(second_rows*first_rows));
  printf("OI TOTAL PARTIC %d\n", total_participants);
  for(int i = 0; i < second_rows*first_rows; i++){
    temp_rows_id[i] = malloc(sizeof(int)*total_participants);
  }
  //NOTE: might seg
  memcpy(merged_parts, table1->participants, sizeof(int)*table1->numb_of_parts);
  memcpy(merged_parts + table1->numb_of_parts, table2->participants, sizeof(int)*table2->numb_of_parts);

  for(int i = 0; i < first_rows; i++){
  	for(int j = 0; j < second_rows; j++){
  		memcpy(temp_rows_id[index], table1->rows_id[i], sizeof(int)*participants1);
  		memcpy(temp_rows_id[index] + participants1, table2->rows_id[j], sizeof(int)*participants2);
  		index++;
  	}
  }

  for(int i = 0; i < first_rows; i++){
    free(table1->rows_id[i]);
  }
  free(table1->rows_id);
  for(int i = 0; i < second_rows; i++){
    free(table2->rows_id[i]);
  }
  free(table2->rows_id);
  free(table2->participants);
  free(table1->participants);
  table1->participants = merged_parts;
  table1->numb_of_parts = total_participants;
  table1->rows_id = temp_rows_id;
}

// Function that actually takes the last merged middle table and prints the payloads of the rows
// that are present and the final result. Noteworthy that only the columns mentioned in the projections
// will be printed and not all that are present in the final mergedMiddle
// Added extra functionality, calculating the checksums in the same time as printing
void printQueryAndCheckSumResult(struct middle_table *mergedMiddle, struct table *table, struct query currQuery){
	// Allocate a checkSum array as big as the number of projections
	u_int64_t *checkSum;
	checkSum = calloc(currQuery.size3, sizeof(u_int64_t));

	//printf("The results are:\n");
	//printf("PRINTING IS TURNED OF\n");

	// If there are no rows in the final middle table
	if (mergedMiddle->rows_size == 0)
	{
		for (int i = 0; i < currQuery.size3; ++i)
		{
			printf("NULL ");
		}
		printf("\n");
	}else{
		int *projectionsIndeces;
		// Find the indeces of the projections requested in the participants table
		projectionsIndeces = findProjectionsIndeces(mergedMiddle->participants, mergedMiddle->numb_of_parts, currQuery.projections, currQuery.size3, currQuery.table_indeces);
		// And for each row in the final middle table
		for (int i = 0; i < mergedMiddle->rows_size; ++i)
		{
			// For each projection that is asked to be made
			for (int j = 0; j < currQuery.size3; ++j)
			{
				// Find the id of the relation to be projected
				//int relationProjectionID = currQuery.table_indeces[currQuery.projections[j][0]];
				int relationProjectionID = currQuery.table_indeces[currQuery.projections[j][0]];
				// Find the id of the column of that relation to be projected
				int columnProjectionID = currQuery.projections[j][1];
				// Find the id of the row of that relation to be projected
				int rowProjectionID = mergedMiddle->rows_id[projectionsIndeces[j]][i];
				// And print it or I hope so
				//printf("APO REL %d APO COL %d ROW %d\n", relationProjectionID, columnProjectionID, rowProjectionID);
				int value = table[relationProjectionID].my_relation[columnProjectionID].tuples[rowProjectionID].payload;
				//printf("%d ", value);
				checkSum[j] += value;
			}
			//printf("\n");
		}
		//printf("The checkSum is:\n");
		for (int i = 0; i < currQuery.size3; ++i)
		{
			printf("%ld ", checkSum[i]);
		}
		printf("\n");
		free(projectionsIndeces);
		free(checkSum);
	}
}

void insert_to_middle(struct middle_table *middle, struct table *table, int size, struct column r1, struct column r2)

{
	int relation_position1 = -1, relation_position2 = -1;
	int tmp_relation_position1 = -1, tmp_relation_position2 = -1;
	int first_empty = 0;
	int flag = 0;
	int position1 = -1, position2 = -1;

	//general search for partcipants
	for (int i=0; i<size; i++)
	{
		/*Each relation is going in only one place of our middle array so we dont need to
		worry about the next itterations*/
		if (relation_position1 == -1)
		{
			tmp_relation_position1 = find_relation(r1.virtualRelation, middle[i].participants, middle[i].numb_of_parts);
			if(tmp_relation_position1 >= 0){
				relation_position1 = tmp_relation_position1;
				position1 = i;
			}
		}
		if (relation_position2 == -1)
		{
			tmp_relation_position2 = find_relation(r2.virtualRelation, middle[i].participants, middle[i].numb_of_parts);
			if(tmp_relation_position2 >= 0){
				relation_position2 = tmp_relation_position2;
				position2 = i;
			}
		}
		if(middle[i].numb_of_parts == 0){
			if(flag == 0){
				first_empty = i;
				flag = 1;
			}
		}

	}
	//variable position: the index of middle_table array of our relation!!!!!!!!!!
	//variable relation_position: the index of relation to int* participants!!!!!
	if((relation_position1 == -1) && (relation_position2 ==-1))
	{
		// If we have to join two columns of the same relation
		if (r1.virtualRelation == r2.virtualRelation)
		{
			//new relation wich means new cell in the middle array
			struct result *join_result;
			middle[first_empty].participants = malloc(sizeof(int));
			middle[first_empty].participants[0] = r1.virtualRelation;
			middle[first_empty].numb_of_parts = 1;

			// Noteworthy that in such a case the rows id's of both R and S are the same since we have only one relation
			join_result = scanRelations(&table[r1.table].my_relation[r1.column], &table[r2.table].my_relation[r2.column]);
			middle[first_empty].rows_id = malloc(sizeof(int *));
			middle[first_empty].rows_id[0] = malloc(sizeof(int)*join_result->numRows);
			memcpy(middle[first_empty].rows_id[0], join_result->rowIDsR, sizeof(int)*join_result->numRows);
			middle[first_empty].rows_size = join_result->numRows;
			freeResult(join_result);
		}else{
			//new relations wich means new cell in the middle array
			struct result *join_result;
			middle[first_empty].participants = malloc(sizeof(int)*2);
			middle[first_empty].participants[0] = r1.virtualRelation;
			middle[first_empty].participants[1] = r2.virtualRelation;
			middle[first_empty].numb_of_parts = 2;

			join_result = RadixHashJoin(&table[r1.table].my_relation[r1.column],&table[r2.table].my_relation[r2.column]);
			middle[first_empty].rows_id = malloc(sizeof(int *)*2);
			middle[first_empty].rows_id[0] = malloc(sizeof(int)*join_result->numRows);
			middle[first_empty].rows_id[1] = malloc(sizeof(int)*join_result->numRows);
			memcpy(middle[first_empty].rows_id[0], join_result->rowIDsR, sizeof(int)*join_result->numRows);
			memcpy(middle[first_empty].rows_id[1], join_result->rowIDsS, sizeof(int)*join_result->numRows);
			middle[first_empty].rows_size = join_result->numRows;
			free(join_result);
		}
	}
	/*in the following 2 else if statements we check if one of two relations
	have already participated in the join and we add the other in the same cell*/
	else if(relation_position1 == -1)
	{
		//printf("MPIKA STO DEN IPIRXE TO PRWTO\n");
		struct result *join_result;
		struct relation *temp_rel;
		middle[position2].numb_of_parts++;
		int index;
		int *temp;
		int participants = middle[position2].numb_of_parts;
		temp = malloc(sizeof(int)*middle[position2].numb_of_parts);
		memcpy(temp, middle[position2].participants, sizeof(int)*(participants-1));
		temp[participants - 1] = r1.virtualRelation;
		free(middle[position2].participants);
		middle[position2].participants = temp;
		temp_rel = malloc(sizeof(struct relation));
		temp_rel->tuples = malloc(sizeof(struct tuple)*middle[position2].rows_size);
		temp_rel->num_tuples = middle[position2].rows_size;
		for(int i=0; i<middle[position2].rows_size; i++)
		{
			index = middle[position2].rows_id[relation_position2][i];
			temp_rel->tuples[i].key = i;
			temp_rel->tuples[i].payload = table[r2.table].my_relation[r2.column].tuples[index].payload;

		}
		join_result = RadixHashJoin(&table[r1.table].my_relation[r1.column], temp_rel);
		freeRelation(temp_rel);
		int tempRowCounter = 0;
		int **temp_rows_id;
		int position_of_temp=0, data=0;
		temp_rows_id = malloc(sizeof(int *)*participants);
        for(int i=0; i<participants; i++)
	    {
	    	temp_rows_id[i] = malloc(sizeof(int)*join_result->numRows);
	    }
	    for (int i = 0; i < join_result->numRows; ++i)
	    {
	    	for (int j = 0; j < participants - 1; ++j)
	    	{
	    		temp_rows_id[j][i] = middle[position2].rows_id[j][join_result->rowIDsS[i]];
	    	}
	    	temp_rows_id[participants - 1][i] = join_result->rowIDsR[i];
	    }

		for(int i=0; i<participants-1; i++)
		{
		  free(middle[position2].rows_id[i]);
		}
		free(middle[position2].rows_id);
		middle[position2].rows_id = temp_rows_id;
  		middle[position2].rows_size = join_result->numRows;
  		freeResult(join_result);
	}

	else if(relation_position2 == -1)
	{
		//printf("MPIKA STO DEN IPIRXE TO DEFTERO\n");
		middle[position1].numb_of_parts++;
		int *temp;
		struct relation *temp_rel;
		int index;
		struct result* join_result;
		int participants = middle[position1].numb_of_parts;
		temp = malloc(sizeof(int)*participants);
		memcpy(temp, middle[position1].participants, sizeof(int)*(participants-1));
		temp[participants - 1] = r2.virtualRelation;
		free(middle[position1].participants);
		middle[position1].participants = temp;

		temp_rel = malloc(sizeof(struct relation));
		temp_rel->tuples = malloc(sizeof(struct tuple)*(middle[position1].rows_size));
		temp_rel->num_tuples = middle[position1].rows_size;

		for(int i=0; i<middle[position1].rows_size; i++)
		{
			index = middle[position1].rows_id[relation_position1][i];
			temp_rel->tuples[i].key = i;
			temp_rel->tuples[i].payload = table[r1.table].my_relation[r1.column].tuples[index].payload;

		}
		join_result = RadixHashJoin(temp_rel, &table[r2.table].my_relation[r2.column]);
		freeRelation(temp_rel);
		int **temp_rows_id;
		temp_rows_id = malloc(sizeof(int *)*participants);
	    for(int i=0; i<participants; i++)
	    {
	    	temp_rows_id[i] = malloc(sizeof(int)*join_result->numRows);
	    }

	    for (int i = 0; i < join_result->numRows; ++i)
	    {
	    	for (int j = 0; j < participants - 1; ++j)
	    	{
	    		temp_rows_id[j][i] = middle[position1].rows_id[j][join_result->rowIDsR[i]];
	    	}
	    	temp_rows_id[participants - 1][i] = join_result->rowIDsS[i];
	    }

		for(int i=0; i<participants-1; i++)
		{
			free(middle[position1].rows_id[i]);
		}
		free(middle[position1].rows_id);

		middle[position1].rows_id = temp_rows_id;
  		middle[position1].rows_size = join_result->numRows;
  		freeResult(join_result);
	}
			/*in this case both relations participated in a join
			and they are in the same cell  */
	else if(position1 == position2)
	{
		int *temp1, *temp2;
		struct relation *temp_rel1, *temp_rel2;
		int i;
		int index;
		struct result* join_result;
		int participants = middle[position1].numb_of_parts;
		// No need to add/merge anything to the middle.participants table because the relations are already in
		temp_rel1 = malloc(sizeof(struct relation));
		temp_rel1->tuples = malloc(sizeof(struct tuple)*middle[position1].rows_size);
		temp_rel1->num_tuples = middle[position1].rows_size;
		for(int i=0; i<middle[position1].rows_size; i++)
		{
			index = middle[position1].rows_id[relation_position1][i];
			temp_rel1->tuples[i].key = i;
			temp_rel1->tuples[i].payload = table[r1.table].my_relation[r1.column].tuples[index].payload;

		}
		temp_rel2 = malloc(sizeof(struct relation));
		temp_rel2->tuples = malloc(sizeof(struct tuple)*middle[position2].rows_size);
		temp_rel2->num_tuples = middle[position2].rows_size;
		for(int i=0; i<middle[position2].rows_size; i++)
		{
			index = middle[position2].rows_id[relation_position2][i];
			temp_rel2->tuples[i].key = i;
			temp_rel2->tuples[i].payload = table[r2.table].my_relation[r2.column].tuples[index].payload;

		}

		join_result = scanRelations(temp_rel1, temp_rel2);
		freeRelation(temp_rel1);
		freeRelation(temp_rel2);

		int **temp_rows_id;
		temp_rows_id = malloc(sizeof(int *)*participants);
		for(int i=0; i<participants; i++)
		{
			temp_rows_id[i] = malloc(sizeof(int)*(join_result->numRows));
		}

		for (int i = 0; i < join_result->numRows; ++i)
		{
			for (int j = 0; j < participants; ++j)
	    	{
	    		temp_rows_id[j][i] = middle[position1].rows_id[j][join_result->rowIDsR[i]];
	    	}
		}

		for(int i=0; i<participants; i++)
		{
			free(middle[position1].rows_id[i]);
		}
		free(middle[position1].rows_id);
		middle[position1].rows_id = temp_rows_id;
		middle[position1].rows_size = join_result->numRows;
		freeResult(join_result);
	}
	/*in this case both relations already participated in a join
	but they are in different cells of middle table*/
	else if(position1 != position2)
	{
		//printf("MPIKA STO IPIRXAN KAI TA DIO SE ALLO KELI\n");
		int *temp1, *temp2;
		struct relation *temp_rel1, *temp_rel2;
		int index;
		struct result* join_result;
		// No need to add/merge anything to the middle.participants table because the relations are already in
		temp_rel1 = malloc(sizeof(struct relation));
		temp_rel1->tuples = malloc(sizeof(struct tuple)*middle[position1].rows_size);
		temp_rel1->num_tuples = middle[position1].rows_size;
		for(int i=0; i<middle[position1].rows_size; i++)
		{
			index = middle[position1].rows_id[relation_position1][i];
			temp_rel1->tuples[i].key = i;
			temp_rel1->tuples[i].payload = table[r1.table].my_relation[r1.column].tuples[index].payload;

		}
		temp_rel2 = malloc(sizeof(struct relation));
		temp_rel2->tuples = malloc(sizeof(struct tuple)*middle[position2].rows_size);
		temp_rel2->num_tuples = middle[position2].rows_size;
		for(int i=0; i<middle[position2].rows_size; i++)
		{
			index = middle[position2].rows_id[relation_position2][i];
			temp_rel2->tuples[i].key = i;
			temp_rel2->tuples[i].payload = table[r2.table].my_relation[r2.column].tuples[index].payload;

		}
		join_result = RadixHashJoin(temp_rel1, temp_rel2);
		freeRelation(temp_rel1);
		freeRelation(temp_rel2);

		int **temp_rows_id;
		int numParticipants1 = middle[position1].numb_of_parts;
		int numParticipants2 = middle[position2].numb_of_parts;
		int numParticipants = numParticipants1 + numParticipants2;
		middle[position1].participants = realloc(middle[position1].participants, sizeof(int)*numParticipants);
		if(middle[position1].participants == NULL)
		{
			perror("Memory reallocation failed: ");
			exit(-1);
		}
		memcpy(middle[position1].participants + numParticipants1, middle[position2].participants, sizeof(int)*numParticipants2);

	    temp_rows_id = malloc(sizeof(int *)*numParticipants);
		for(int i=0; i<numParticipants; i++)
		{
			temp_rows_id[i] = malloc(sizeof(int)*join_result->numRows);
		}
		for (int i = 0; i < join_result->numRows; ++i)
		{
			for (int j = 0; j < numParticipants1; ++j)
			{
				temp_rows_id[j][i] = middle[position1].rows_id[j][join_result->rowIDsR[i]];
			}
			for (int j = numParticipants1; j < numParticipants2; ++j)
			{
				temp_rows_id[j][i] = middle[position2].rows_id[j - numParticipants1][join_result->rowIDsS[i]];
			}
		}

		for(int i=0; i<numParticipants1; i++)
		{
			free(middle[position1].rows_id[i]);
		}
		free(middle[position1].rows_id);
		for(int i=0; i<numParticipants2; i++)
		{
			free(middle[position2].rows_id[i]);
		}
		free(middle[position2].rows_id);
		/* EDW PREPEI NA KANEIS NULL TIN POSITION2 TOU MIDDLE TABLE WSTE MELLONTIKA NA FENETE ADEIA*/
    middle[position2].rows_id = NULL;
    middle[position2].numb_of_parts = 0;
    middle[position2].rows_size = 0;
    free(middle[position2].participants);
    middle[position2].participants = NULL;
    ///////////////////////////////////////////
		middle[position1].rows_id = temp_rows_id;
		middle[position1].rows_size = join_result->numRows;
		middle[position1].numb_of_parts = numParticipants;
		freeResult(join_result);
	}else{
		printf("KATIPIGELATHOSMAN\n");
	}

}

void insert_to_middle_predicate(struct middle_table * middle, struct table * table, int size, struct column r, int value, int mode)
{
  int relation_position = -1;
  int tmp_relation_position = -1;
  int first_empty = 0;
  int flag = 0;
  int position = -1;

  //general search for partcipants
  for (int i=0; i<size; i++)
  {
    if (relation_position == -1)
    {
    	tmp_relation_position = find_relation(r.virtualRelation, middle[i].participants, middle[i].numb_of_parts);
    	if(tmp_relation_position >= 0){
    		relation_position = tmp_relation_position;
    	    position = i;
    	    break;
    	}
    }

    if(middle[i].numb_of_parts == 0){
      if(flag == 0){
        first_empty = i;
        flag = 1;
      }
    }

  }

  if(relation_position == -1)
  {
    struct result *filter_result;
    middle[first_empty].participants = malloc(sizeof(int));
    middle[first_empty].participants[0] = r.virtualRelation;
    middle[first_empty].numb_of_parts = 1;

    filter_result = filterPredicate(&table[r.table].my_relation[r.column], value, mode);

    middle[first_empty].rows_id = malloc(sizeof(int *));
    middle[first_empty].rows_id[0] = malloc(sizeof(int)*(filter_result->numRows));
    middle[first_empty].rows_size = filter_result->numRows;
    memcpy(middle[first_empty].rows_id[0], filter_result->rowIDsR, sizeof(int)*filter_result->numRows);
    freeResult(filter_result);
  }
  else
  {
    struct result *filter_result;
    struct relation *temp_rel;
    int index;

    int tempRowCounter = 0;
    int **temp_rows_id;

	int participants = middle[position].numb_of_parts;

    temp_rows_id = malloc(sizeof(int *)*participants);
    for(int i=0; i<participants; i++)
    {
      temp_rows_id[i] = malloc(sizeof(int)*(middle[position].rows_size));
    }
    if (mode == BIGGER)
    {
    	for (int i = 0; i < middle[position].rows_size; ++i)
    	{
    		index = middle[position].rows_id[relation_position][i];
    		if (table[r.table].my_relation[r.column].tuples[index].payload > value)
    		{
    			for (int k = 0; k < participants; ++k)
    			{
    				temp_rows_id[k][tempRowCounter] = middle[position].rows_id[k][i];
    			}
    			tempRowCounter++;
    		}
    	}
    }
    else if (mode == LESS){
    	for (int i = 0; i < middle[position].rows_size; ++i)
    	{
    		index = middle[position].rows_id[relation_position][i];
    		if (table[r.table].my_relation[r.column].tuples[index].payload < value)
    		{
    			for (int k = 0; k < participants; ++k)
    			{
    				temp_rows_id[k][tempRowCounter] = middle[position].rows_id[k][i];
    			}
    			tempRowCounter++;
    		}
    	}
    }
    else if (mode == EQUAL){
    	for (int i = 0; i < middle[position].rows_size; ++i)
    	{
    		index = middle[position].rows_id[relation_position][i];
    		if (table[r.table].my_relation[r.column].tuples[index].payload == value)
    		{
    			for (int k = 0; k < participants; ++k)
    			{
    				temp_rows_id[k][tempRowCounter] = middle[position].rows_id[k][i];
    			}
    			tempRowCounter++;
    		}
    	}
    }else{
    	printf("WRONG COMPARING MODE CODE IN FILTER PREDICATE\n");
    }
    if (tempRowCounter)
    {
    	for (int i = 0; i < participants; ++i)
    	{
    		temp_rows_id[i] = realloc(temp_rows_id[i], sizeof(int)*tempRowCounter);
    		if(temp_rows_id[i] == NULL){
    			perror("Memory reallocation failed: ");
    			exit(-1);
    		}
    	}
    }else{
		for(int i=0; i<participants; i++)
		{
		  free(temp_rows_id[i]);
		}
		free(temp_rows_id);
		temp_rows_id = NULL;
	}
		for(int i=0; i<participants; i++)
		{
			free(middle[position].rows_id[i]);
		}
		free(middle[position].rows_id);
		middle[position].rows_id = temp_rows_id;
		middle[position].rows_size = tempRowCounter;
  }

}

void executeQuery(struct predicate queryToExecute, struct middle_table *middle, struct table *relations_table, int middlesSize){
	if (queryToExecute.predicateType == BIGGER)
	{
		//insert_to_middle_predicate(middle, relations_table, middlesSize, c1, comparison_value, BIGGER);
		insert_to_middle_predicate(middle, relations_table, middlesSize, queryToExecute.c1, queryToExecute.comparingValue, BIGGER);
	}else if (queryToExecute.predicateType == LESS)
	{
		insert_to_middle_predicate(middle, relations_table, middlesSize, queryToExecute.c1, queryToExecute.comparingValue, LESS);
	}else if (queryToExecute.predicateType == EQUAL)
	{
		insert_to_middle_predicate(middle, relations_table, middlesSize, queryToExecute.c1, queryToExecute.comparingValue, EQUAL);
	}else if (queryToExecute.predicateType == JOIN)
	{
		//insert_to_middle(middle, relations_table, middlesSize, c1, c2);
		insert_to_middle(middle, relations_table, middlesSize, queryToExecute.c1, queryToExecute.c2);
	}else{
		printf("WRONG TYPE OF PREDICATE IN executeQuery\n");
	}
}

void executeBatch(struct batch *my_batch,struct table *relations_table, struct statisticsRelation *sRelation){

	struct predicate *predicatesArray;
	int flag = 0;
	//printf("queries are %d\n",my_batch->numQueries);

	for (int i = 0; i < my_batch->numQueries; ++i)
	{
		struct statisticsRelation *tempStatsRelArray;
		tempStatsRelArray = malloc(sizeof(struct statisticsRelation)*sRelation[0].numRelations);
		copyStatsRelations(tempStatsRelArray, sRelation);
		//printf("TO NUM DATA EKEI EINAI %d\n", tempStatsRelArray[5].columnsStatistics[3].numData);


		struct middle_table *middle;
		int flag = 0;
		int first_middle;
		int middle_size = my_batch->queries[i].size2;
		int numJoins = 0;
		struct column **columnsToBeJoinedArray, **reorderedColumnsToBeJoinedArray;;
		middle = create_middle_table(my_batch->queries[i].size2);
		predicatesArray = string_parser(my_batch->queries[i], middle, relations_table, my_batch->queries[i].size2);
		for (int j = 0; j < my_batch->queries[i].size2; ++j)
		{
			if (predicatesArray[j].predicateType < 3)
			{
				executeQuery(predicatesArray[j], middle, relations_table, my_batch->queries[i].size2);
				if (predicatesArray[j].predicateType == BIGGER)
				{
					if (statistsicsInequal(&tempStatsRelArray[predicatesArray[j].c1.table], predicatesArray[j].c1.column, predicatesArray[j].comparingValue, BIGGER) == -1)
					{
						printf("ZERO DIVISION IN statistsicsInequal\n");
					}
				}else if (predicatesArray[j].predicateType == LESS)
				{
					if (statistsicsInequal(&tempStatsRelArray[predicatesArray[j].c1.table], predicatesArray[j].c1.column, predicatesArray[j].comparingValue, LESS) == -1)
					{
						printf("ZERO DIVISION IN statistsicsInequal\n");
					}
				}else if (predicatesArray[j].predicateType == EQUAL)
				{
					//printf("ZITAW TO %d.%d KAI VAL %d\n", predicatesArray[j].c1.table, predicatesArray[j].c1.column, predicatesArray[j].comparingValue);
					if (statisticsEqual(&tempStatsRelArray[predicatesArray[j].c1.table], predicatesArray[j].c1.column, predicatesArray[j].comparingValue, relations_table[predicatesArray[j].c1.table]) == -1)
					{
						printf("ZERO DIVISION IN statisticsEqual\n");
					}
				}else{
					printf("INVALID TYPE OF PREDICATE INTO executeBatch\n");
				}

			}else{
				numJoins++;
			}
		}
		if (numJoins)
		{
			columnsToBeJoinedArray = malloc(sizeof(struct column *)*(numJoins*2));
			reorderedColumnsToBeJoinedArray = malloc(sizeof(struct column *)*(numJoins*2));
			for (int j = 0; j < numJoins*2; ++j)
			{
				columnsToBeJoinedArray[j] = malloc(sizeof(struct column));
				reorderedColumnsToBeJoinedArray[j] = malloc(sizeof(struct column));
			}
			int joinsCounter = 0;
			for (int j = 0; j < my_batch->queries[i].size2; ++j)
			{
				if (predicatesArray[j].predicateType == 3)
				{
					*columnsToBeJoinedArray[2*joinsCounter] = predicatesArray[j].c1;
					*columnsToBeJoinedArray[2*joinsCounter + 1] = predicatesArray[j].c2;
					joinsCounter++;
				}
			}

			reorderColumns(columnsToBeJoinedArray, reorderedColumnsToBeJoinedArray, numJoins*2, tempStatsRelArray, predicatesArray, my_batch->queries[i].size2);

			struct predicate *joinPredicatesArray;
			//joinPredicatesArray = malloc(sizeof(struct predicate)*(numJoins*2 - 1));
			joinPredicatesArray = malloc(sizeof(struct predicate)*numJoins);

			for (int j = 0; j < numJoins; ++j)
			{
				/*if (j == 0)
				{
					joinPredicatesArray[j].predicateType = JOIN;
					joinPredicatesArray[j].c1.table = reorderedColumnsToBeJoinedArray[0]->table;
					joinPredicatesArray[j].c1.virtualRelation = reorderedColumnsToBeJoinedArray[0]->virtualRelation;
					joinPredicatesArray[j].c1.column = reorderedColumnsToBeJoinedArray[0]->column;
					joinPredicatesArray[j].c2.table = reorderedColumnsToBeJoinedArray[1]->table;
					joinPredicatesArray[j].c2.virtualRelation =  reorderedColumnsToBeJoinedArray[1]->virtualRelation;
					joinPredicatesArray[j].c2.column = reorderedColumnsToBeJoinedArray[1]->column;
				}else{
					joinPredicatesArray[j].predicateType = JOIN;
					joinPredicatesArray[j].c1.table = reorderedColumnsToBeJoinedArray[2*j]->table;
					joinPredicatesArray[j].c1.virtualRelation = reorderedColumnsToBeJoinedArray[2*j]->virtualRelation;
					joinPredicatesArray[j].c1.column = reorderedColumnsToBeJoinedArray[2*j]->column;
					joinPredicatesArray[j].c2.table = reorderedColumnsToBeJoinedArray[2*j + 1]->table;
					joinPredicatesArray[j].c2.virtualRelation =  reorderedColumnsToBeJoinedArray[2*j + 1]->virtualRelation;
					joinPredicatesArray[j].c2.column = reorderedColumnsToBeJoinedArray[2*j + 1]->column;
				}*/
				joinPredicatesArray[j].predicateType = JOIN;
				joinPredicatesArray[j].c1.table = reorderedColumnsToBeJoinedArray[2*j]->table;
				joinPredicatesArray[j].c1.virtualRelation = reorderedColumnsToBeJoinedArray[2*j]->virtualRelation;
				joinPredicatesArray[j].c1.column = reorderedColumnsToBeJoinedArray[2*j]->column;
				joinPredicatesArray[j].c2.table = reorderedColumnsToBeJoinedArray[2*j + 1]->table;
				joinPredicatesArray[j].c2.virtualRelation =  reorderedColumnsToBeJoinedArray[2*j + 1]->virtualRelation;
				joinPredicatesArray[j].c2.column = reorderedColumnsToBeJoinedArray[2*j + 1]->column;
				//printf("%d.%d = %d.%d\n", joinPredicatesArray[j].c1.virtualRelation,joinPredicatesArray[j].c1.column,joinPredicatesArray[j].c2.virtualRelation,joinPredicatesArray[j].c2.column);
				/*joinPredicatesArray[j].predicateType = JOIN;
				joinPredicatesArray[j].c1.table = reorderedColumnsToBeJoinedArray[j]->table;
				joinPredicatesArray[j].c1.virtualRelation = reorderedColumnsToBeJoinedArray[j]->virtualRelation;
				joinPredicatesArray[j].c1.column = reorderedColumnsToBeJoinedArray[j]->column;
				joinPredicatesArray[j].c2.table = reorderedColumnsToBeJoinedArray[j + 1]->table;
				joinPredicatesArray[j].c2.virtualRelation =  reorderedColumnsToBeJoinedArray[j + 1]->virtualRelation;
				joinPredicatesArray[j].c2.column = reorderedColumnsToBeJoinedArray[j + 1]->column;
				printf("%d.%d = %d.%d\n", joinPredicatesArray[j].c1.virtualRelation,joinPredicatesArray[j].c1.column,joinPredicatesArray[j].c2.virtualRelation,joinPredicatesArray[j].c2.column);*/
			}
			//printf("EDW\n");

			/*for (int j = 0; j < (numJoins*2 - 1); ++j)
			{
				executeQuery(joinPredicatesArray[j], middle, relations_table, my_batch->queries[i].size2);
			}*/
			for (int j = 0; j < numJoins; ++j)
			{
				executeQuery(joinPredicatesArray[j], middle, relations_table, my_batch->queries[i].size2);
			}
			//printf("EDW2\n");
		}
		/*for (int i = 0; i < sRelation[0].numRelations; ++i)
		{
			free(tempStatsRelArray[i].columnsStatistics);
		}
		free(tempStatsRelArray);*/
		/*for (int j = 0; j < my_batch->queries[i].size2; ++j)
		{
			if (predicatesArray[j].predicateType == 3)
			{
				executeQuery(predicatesArray[j], middle, relations_table, my_batch->queries[i].size2);
			}
		}*/
		for(int index = 0; index < middle_size; index++){
			if(middle[index].numb_of_parts > 0){
				if(flag == 0){
					first_middle = index;
					flag = 1;
				}
				else{
					middle_merge(&middle[first_middle], &middle[index]);
				}
			}
		}
		int mergedPosition = -1;
		for(int j=0; j < my_batch->queries[i].size2; j++)
		{
			if(middle[j].numb_of_parts > 0)
			{
				mergedPosition = j;
			}
		}

		printQueryAndCheckSumResult(&middle[mergedPosition], relations_table, my_batch->queries[i]);
		freeMiddle(&middle[mergedPosition]);
    	free(predicatesArray);
    //freeMiddleTable(middle, my_batch->queries[i].size2);
	}
}

void printQuery(struct query myQuery){
	for (int i = 0; i < myQuery.size1; ++i)
	{
		printf("%d ", myQuery.table_indeces[i]);
	}
	printf("|");
	for (int i = 0; i < myQuery.size2; ++i)
	{
		printf("%s&", myQuery.filters[i]);
	}
	printf("|");
	for (int i = 0; i < myQuery.size3; ++i)
	{
		printf("%d.%d ", myQuery.projections[i][0], myQuery.projections[i][1]);
	}
	printf("\n");
}
