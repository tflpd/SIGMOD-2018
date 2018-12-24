#include "structs.h"
#include "myList.h"
#include "storage.h"

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

struct query* create_query(int* table_indeces, int size, char** filters, int size_1, int** sum, int size_2){
	// Construct a query struct
	struct query* my_query;

	my_query = malloc(sizeof(struct query));
	if(my_query == NULL){
		perror("Memory allocation failed: ");
		exit(-1);
	}

	my_query->table_indeces = malloc(sizeof(int)*size);
	if(my_query->table_indeces == NULL){
		perror("Memory allocation failed: ");
		exit(-1);
	}
	for (int i = 0; i < size; ++i)
	{
		my_query->table_indeces[i] = table_indeces[i];
	}

	my_query->size1 = size;

	my_query->filters = malloc(sizeof(char *)*size_1);
	if(my_query->filters == NULL){
		perror("Memory allocation failed: ");
		exit(-1);
	}

	for (int i = 0; i < size_1; ++i)
	{
		my_query->filters[i] = malloc(sizeof(char)*(strlen(filters[i])+1));
		if(my_query->filters[i] == NULL){
			perror("Memory allocation failed: ");
			exit(-1);
		}
		strcpy(my_query->filters[i], filters[i]);
	}

	my_query->size2 = size_1;

	my_query->projections = malloc(sizeof(int *)*size_2);

	if(my_query->projections == NULL){
		perror("Memory allocation failed: ");
		exit(-1);
	}

	for (int i = 0; i < size_2; ++i)
	{
		my_query->projections[i] = malloc(sizeof(int)*2);
		if(my_query->projections[i] == NULL){
			perror("Memory allocation failed: ");
			exit(-1);
		}
		my_query->projections[i][0] = sum[i][0];
		my_query->projections[i][1] = sum[i][1];
	}

	my_query->size3 = size_2;

	return my_query;
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
	// If relation R has the least tuples
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
			if (table_indeces[projections[i][0]] == participants[j])
			{
				projectionsIndeces[i] = j;
				break;
			}
			// CAREFUL MAYBE CORNER CASE TO BE ADDED IF PROJECTION REQUESTED NOT IN PARTICIPANTS
		}
	}

	return projectionsIndeces;
}

// Function that actually takes the last merged middle table and prints the payloads of the rows
// that are present and the final result. Noteworthy that only the columns mentioned in the projections
// will be printed and not all that are present in the final mergedMiddle
// Added extra functionality, calculating the checksums in the same time as printing
void printQueryAndCheckSumResult(struct middle_table *mergedMiddle, struct table *table, struct query currQuery){
	// Allocate a checkSum array as big as the number of projections
	u_int64_t *checkSum;
	checkSum = calloc(currQuery.size3, sizeof(u_int64_t));

	printf("The results are:\n");
	printf("PRINTING IS TURNED OF\n");

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
		printf("The checkSum is:\n");
		for (int i = 0; i < currQuery.size3; ++i)
		{
			printf("%ld ", checkSum[i]);
		}
		printf("\n");
		free(checkSum);
	}
}

void insert_to_middle(struct middle_table *middle, struct table *table, int size, int relation1, int relation2, int c1, int c2)

{
		int relation_position1 = -1, relation_position2 = -1;
		int tmp_relation_position1 = -1, tmp_relation_position2 = -1;
		int first_empty = 0;
		int flag = 0;
		int position1 = -1, position2 = -1;
		/*first case: first join happened none of the tables ever used
		No need to Iterate over the table in first case
		just add the two arrays in middle*/
		//	AKOMA KAI EDW OTAN EINAI TO PRWTO EVER DEN PREPEI NA KALEITAI TO RADIX HASH JOIN KAI NA
		//	DIMIOURGOUME TO PRWTO ENDIAMESO ME TA APOTELESMATA TOU?
		/*if(middle[0].numb_of_parts == 0)
		{
			middle[0].participants = malloc(sizeof(int)*2);
			middle[0].participants[0] = relation1;
			middle[0].participants[1] = relation2;
			middle[0].numb_of_parts = 2;
			return;

		}*/
		//general search for partcipants
		for (int i=0; i<size; i++)
		{
			//relation_position1 = find_relation(relation1, middle[i].participants, middle[i].numb_of_parts)
			//relation_position2 = find_relation(relation2, middle[i].participants, middle[i].numb_of_parts)
			/*Each relation is going in only one place of our middle array so we dont need to
			worry about the next itterations*/
			if (relation_position1 == -1)
			{
				tmp_relation_position1 = find_relation(relation1, middle[i].participants, middle[i].numb_of_parts);
				if(tmp_relation_position1 >= 0){
					relation_position1 = tmp_relation_position1;
					position1 = i;
					printf("MPIKA1 se %d %d\n", position1, relation_position1);
					//relation_position1 = find_relation(relation1, middle[i].participants, middle[i].numb_of_parts);
				}
			}
			if (relation_position2 == -1)
			{
				tmp_relation_position2 = find_relation(relation2, middle[i].participants, middle[i].numb_of_parts);
				if(tmp_relation_position2 >= 0){
					relation_position2 = tmp_relation_position2;
					printf("MPIKA2\n");
					position2 = i;
					//relation_position2 = find_relation(relation2, middle[i].participants, middle[i].numb_of_parts);
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
		// parsing the middle array ended
		if((relation_position1 == -1) && (relation_position2 ==-1))
		{
			printf("MPIKA STO DEN IPIRXE KANENA APTA DIO\n");
			// If we have to join two columns of the same relation
			if (relation1 == relation2)
			{
				//new relation wich means new cell in the middle array
				struct result *join_result;
				middle[first_empty].participants = malloc(sizeof(int));
				middle[first_empty].participants[0] = relation1;
				middle[first_empty].numb_of_parts = 1;

				// Noteworthy that in such a case the rows id's of both R and S are the same since we have only one relation
				//printf("ARA THA STILW R %d S %d KAI C1 %d C2 %d\n", relation1, relation2, c1,c2);
				join_result = scanRelations(&table[relation1].my_relation[c1], &table[relation2].my_relation[c2]);
				middle[first_empty].rows_id = malloc(sizeof(int *));
				middle[first_empty].rows_id[0] = malloc(sizeof(int)*join_result->numRows);
				memcpy(middle[first_empty].rows_id[0], join_result->rowIDsR, sizeof(int)*join_result->numRows);
				middle[first_empty].rows_size = join_result->numRows;
			}else{
				//new relations wich means new cell in the middle array
				struct result *join_result;
				middle[first_empty].participants = malloc(sizeof(int)*2);
				middle[first_empty].participants[0] = relation1;
				middle[first_empty].participants[1] = relation2;
				middle[first_empty].numb_of_parts = 2;

				join_result = RadixHashJoin(&table[relation1].my_relation[c1],&table[relation2].my_relation[c2]);
				middle[first_empty].rows_id = malloc(sizeof(int *)*2);
				middle[first_empty].rows_id[0] = malloc(sizeof(int)*join_result->numRows);
				middle[first_empty].rows_id[1] = malloc(sizeof(int)*join_result->numRows);
				memcpy(middle[first_empty].rows_id[0], join_result->rowIDsR, sizeof(int)*join_result->numRows);
				memcpy(middle[first_empty].rows_id[1], join_result->rowIDsS, sizeof(int)*join_result->numRows);
				middle[first_empty].rows_size = join_result->numRows;
			}
		}
		/*in the following 2 else if statements we check if one of two relations
		have already participated in the join and we add the other in the same cell*/
		else if(relation_position1 == -1)
		{
			printf("MPIKA STO DEN IPIRXE TO PRWTO\n");
			struct result *join_result;
			struct relation *temp_rel;
			middle[position2].numb_of_parts++;
			int index;
			int *temp;
			int i;
			int participants = middle[position2].numb_of_parts;
			temp = malloc(sizeof(int)*middle[position2].numb_of_parts);
			memcpy(temp, middle[position2].participants, sizeof(int)*(participants-1));
			temp[participants - 1] = relation1;
			free(middle[position2].participants);
			middle[position2].participants = temp;
			temp_rel = malloc(sizeof(struct relation));
			temp_rel->tuples = malloc(sizeof(struct tuple)*middle[position2].rows_size);
			temp_rel->num_tuples = middle[position2].rows_size;
			for(int i=0; i<middle[position2].rows_size; i++)
			{
				index = middle[position2].rows_id[relation_position2][i];
				//temp_rel->tuples[i].key = index;
				temp_rel->tuples[i].key = i;
				temp_rel->tuples[i].payload = table[relation2].my_relation[c2].tuples[index].payload;

			}
			join_result = RadixHashJoin(&table[relation1].my_relation[c1], temp_rel);
			int tempRowCounter = 0;
			int **temp_rows_id;
			int position_of_temp=0, data=0;
			//int currThreshold = join_result->numRows;
			temp_rows_id = malloc(sizeof(int *)*participants);
            for(int i=0; i<participants; i++)
		    {
		      //temp_rows_id[i] = malloc(sizeof(int)*currThreshold);
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
			/*for (int i = 0; i < join_result->numRows; ++i)
			{
				for (int j = 0; j < middle[position2].rows_size; ++j)
				{
					if (join_result->rowIDsS[i] == middle[position2].rows_id[relation_position2][j])
					{
						for (int k = 0; k < participants - 1; ++k)
						{
							temp_rows_id[k][tempRowCounter] = middle[position2].rows_id[k][j];

						}
						temp_rows_id[participants - 1][tempRowCounter] = join_result->rowIDsR[i];
						tempRowCounter++;
						if (tempRowCounter == currThreshold)
                        {
                        	currThreshold *=2;
                        	for(int k = 0; k < participants; k++)
                        	{
                        		temp_rows_id[k] = realloc(temp_rows_id[k], sizeof(int)*currThreshold);
                        		if(temp_rows_id[k] == NULL)
                        		{
                        			perror("Memory reallocation failed: ");
                        			exit(-1);
                        		}
                        	}
                        }
					}
				}
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
			}*/
/*			for(i=0; i<middle[position2].rows_size; i++)
			{
			  if(middle[position2].rows_id[relation_position2][i] == join_result->rowIDsR[position_of_temp])
			  {
			    for(int j=0; j<participants-1; j++)
			    {
			      temp_rows_id[j][position_of_temp] = middle[position2].rows_id[j][i];
			      //data++;
			    }
          position_of_temp++;
			  }
        //position_of_temp = 0;
			}*/
			for(i=0; i<participants-1; i++)
			{
			  free(middle[position2].rows_id[i]);
			}
			free(middle[position2].rows_id);
			//memcpy(temp_rows_id[participants], join_result->rowIDsS, sizeof(int)*join_result->numRows);
			middle[position2].rows_id = temp_rows_id;
      		//middle[position2].rows_size = tempRowCounter;
      		middle[position2].rows_size = join_result->numRows;
		}

		else if(relation_position2 == -1)
		{
			printf("MPIKA STO DEN IPIRXE TO DEFTERO\n");
			middle[position1].numb_of_parts++;
			int *temp;
			struct relation *temp_rel;
			int i;
			int index;
			struct result* join_result;
			int participants = middle[position1].numb_of_parts;
			//printf("OI PART EINAI %d\n", participants);
			temp = malloc(sizeof(int)*participants);
			memcpy(temp, middle[position1].participants, sizeof(int)*(participants-1));
			temp[participants - 1] = relation2;
			free(middle[position1].participants);
			middle[position1].participants = temp;
			///////////////////////////////////////////////////////////////
			temp_rel = malloc(sizeof(struct relation));
			temp_rel->tuples = malloc(sizeof(struct tuple)*(middle[position1].rows_size));
			temp_rel->num_tuples = middle[position1].rows_size;
			//printf("EDW TA IPARXONTA\n");
			for(int i=0; i<middle[position1].rows_size; i++)
			{
				index = middle[position1].rows_id[relation_position1][i];
				//temp_rel->tuples[i].key = index;
				temp_rel->tuples[i].key = i;
				//printf("%d\n", index);
				temp_rel->tuples[i].payload = table[relation1].my_relation[c1].tuples[index].payload;

			}
			//////////////////////////////JOIN///////////////////////////////////////////////
			int tempRowCounter = 0;
			/*for(int i=0; i<middle[position1].rows_size; i++)
			{
			  printf("%d %d\n", temp_rel->tuples[i].key, temp_rel->tuples[i].payload);
			}*/
			join_result = RadixHashJoin(temp_rel, &table[relation2].my_relation[c2]);
			/*printf("TA TOU RAD AFTA\n");
			for (int i = 0; i < join_result->numRows; ++i)
			{
				printf("%d %d\n", join_result->rowIDsR[i], join_result->rowIDsS[i]);
			}*/
			int **temp_rows_id;
			int position_of_temp=0, data=0;
			temp_rows_id = malloc(sizeof(int *)*participants);
			int currThreshold = join_result->numRows;
		    for(int i=0; i<participants; i++)
		    {
		      //temp_rows_id[i] = malloc(sizeof(int)*currThreshold);
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

			/*for(int i = 0; i < join_result->numRows; i++)
			{
				for(int j = 0; j < middle[position1].rows_size; j++)
				{
					//printf("MPIKA J %d\n",j);
					if(join_result->rowIDsR[i] == middle[position1].rows_id[relation_position1][j])
					{
						for(int k = 0; k < participants-1; k++)
						{
							//printf("MPIKA K %d\n", k);
							//printf("%d %d %d %d\n",k,tempRowCounter,position1,j);
							temp_rows_id[k][tempRowCounter] = middle[position1].rows_id[k][j];

						}
						temp_rows_id[participants-1][tempRowCounter] = join_result->rowIDsS[i];
                        tempRowCounter++;
                        if (tempRowCounter == currThreshold)
                        {
                        	currThreshold *=2;
                        	for(int k = 0; k < participants; k++)
                        	{
                        		temp_rows_id[k] = realloc(temp_rows_id[k], sizeof(int)*currThreshold);
                        		if(temp_rows_id[k] == NULL)
                        		{
                        			perror("Memory reallocation failed: ");
                        			exit(-1);
                        		}
                        	}
                        }
					}
				}
			}
			if (tempRowCounter)
			{
				for(int i = 0; i < participants; i++)
				{
					temp_rows_id[i] = realloc(temp_rows_id[i], sizeof(int)*tempRowCounter);
					if(temp_rows_id[i] == NULL)
					{
                        perror("Memory reallocation failed: ");
                        exit(-1);
                    }
				}
			}*/

			for(i=0; i<participants-1; i++)
			{
			free(middle[position1].rows_id[i]);
			}
			free(middle[position1].rows_id);

			middle[position1].rows_id = temp_rows_id;
      		//middle[position1].rows_size = tempRowCounter;
      		middle[position1].rows_size = join_result->numRows;
		}
				/*in this case both relations participated in a join
				and they are in the same cell  */
		else if(position1 == position2)
		{
			printf("MPIKA STO IPIRXAN KAI TA DIO STO IDIO KELI\n");
			int *temp1, *temp2;
			struct relation *temp_rel1, *temp_rel2;
			int i;
			int index;
			struct result* join_result;
			int participants = middle[position1].numb_of_parts;
			// No need to add/merge anything to the middle.participants table because the relations are already in
			temp_rel1 = malloc(sizeof(struct relation));
			temp_rel1->tuples = malloc(sizeof(struct tuple)*middle[position1].rows_size);
			for(int i=0; i<middle[position1].rows_size; i++)
			{
				index = middle[position1].rows_id[relation_position1][i];
				temp_rel1->tuples[i].key = index;
				temp_rel1->tuples[i].payload = table[relation1].my_relation[c1].tuples[index].payload;

			}
			temp_rel2 = malloc(sizeof(struct relation));
			temp_rel2->tuples = malloc(sizeof(struct tuple)*middle[position2].rows_size);
			for(int i=0; i<middle[position2].rows_size; i++)
			{
				index = middle[position2].rows_id[relation_position2][i];
				temp_rel2->tuples[i].key = index;
				temp_rel2->tuples[i].payload = table[relation2].my_relation[c2].tuples[index].payload;

			}

			join_result = scanRelations(temp_rel1, temp_rel2);

			int **temp_rows_id;
			temp_rows_id = malloc(sizeof(int *)*participants);
			for(int i=0; i<participants; i++)
			{
				temp_rows_id[i] = malloc(sizeof(int)*(join_result->numRows));
			}

			for (int i = 0; i < join_result->numRows; ++i)
			{
				for (int j = 0; j < middle[position1].rows_size; ++j)
				{
					if ((join_result->rowIDsS[i] == middle[position2].rows_id[relation_position2][j]) && (join_result->rowIDsR[i] == middle[position1].rows_id[relation_position1][j]))
					{
						for (int k = 0; k < middle[position1].numb_of_parts; ++k)
						{
							temp_rows_id[k][i] = middle[position1].rows_id[k][j];

						}
					}
				}
			}
			for(i=0; i<participants-1; i++)
			{
				free(middle[position1].rows_id[i]);
			}
			free(middle[position1].rows_id);
			middle[position1].rows_id = temp_rows_id;
			middle[position1].rows_size = join_result->numRows;
		}
		/*in this case both relations already participated in a join
		but they are in different cells of middle table*/
		else if(position1 != position2)
		{
			printf("MPIKA STO IPIRXAN KAI TA DIO SE ALLO KELI\n");
		int *temp1, *temp2;
		struct relation *temp_rel1, *temp_rel2;
		int i;
		int position_of_temp;
		int index;
		int data =0;
		struct result* join_result;
		// No need to add/merge anything to the middle.participants table because the relations are already in
		temp_rel1 = malloc(sizeof(struct relation));
		temp_rel1->tuples = malloc(sizeof(struct tuple)*middle[position1].rows_size);
		for(int i=0; i<middle[position1].rows_size; i++)
		{
			index = middle[position1].rows_id[relation_position1][i];
			temp_rel1->tuples[i].key = index;
			temp_rel1->tuples[i].payload = table[relation1].my_relation[c1].tuples[index].payload;

		}
		temp_rel2 = malloc(sizeof(struct relation));
		temp_rel2->tuples = malloc(sizeof(struct tuple)*middle[position2].rows_size);
		for(int i=0; i<middle[position2].rows_size; i++)
		{
			index = middle[position2].rows_id[relation_position2][i];
			temp_rel2->tuples[i].key = index;
			temp_rel2->tuples[i].payload = table[relation2].my_relation[c2].tuples[index].payload;

		}
		join_result = RadixHashJoin(temp_rel1, temp_rel2);
		
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
		memcpy(middle[position1].participants + sizeof(int)*numParticipants1, middle[position2].participants, sizeof(int)*numParticipants2);
		position_of_temp =0;
		int tempRowCounter = 0;
		int currThreshold = join_result->numRows;
	    temp_rows_id = malloc(sizeof(int *)*numParticipants);
		for(int i=0; i<numParticipants; i++)
		{
			temp_rows_id[i] = malloc(sizeof(int)*currThreshold);
		}
		for (int i = 0; i < join_result->numRows; i++)
        {
            for (int j = 0; j < numParticipants1; j++)
            {
                if (join_result->rowIDsR[i] == middle[position1].rows_id[relation_position1][j])
                {
                	for (int l = 0; l < numParticipants2; ++l)
                	{
                		if (join_result->rowIDsS[i] == middle[position2].rows_id[relation_position2][l])
                		{
                			for (int k = 0; k < numParticipants1; ++k)
                			{
                			    temp_rows_id[k][tempRowCounter] = middle[position1].rows_id[k][j];

                			}
                			for (int k = numParticipants1; k < numParticipants2; ++k)
                			{
                			    temp_rows_id[k][tempRowCounter] = middle[position2].rows_id[k][l];

                			}
                			tempRowCounter++;
                			if (tempRowCounter == currThreshold)
                			{
                				currThreshold *=2;
                				for(int k = 0; k < numParticipants; k++)
                				{
                					temp_rows_id[i] = realloc(temp_rows_id[i], sizeof(int)*currThreshold);
                					if(temp_rows_id[i] == NULL)
                					{
                						perror("Memory reallocation failed: ");
                						exit(-1);
                					}
                				}
                			}
                		}
                	}
                }
            }
         }

        if (tempRowCounter)
        {
        	for (int i = 0; i < numParticipants; ++i)
			{
				temp_rows_id[i] = realloc(temp_rows_id[i], sizeof(int)*tempRowCounter);
				if(temp_rows_id[i] == NULL)
				{
					perror("Memory reallocation failed: ");
					exit(-1);
				}
            }   
        }   
		for(i=0; i<numParticipants1; i++)
		{
			free(middle[position1].rows_id[i]);
		}
		free(middle[position1].rows_id);
		for(i=0; i<numParticipants2; i++)
		{
			free(middle[position2].rows_id[i]);
		}
		free(middle[position2].rows_id);
		/* EDW PREPEI NA KANEIS NULL TIN POSITION2 TOU MIDDLE TABLE WSTE MELLONTIKA NA FENETE ADEIA*/
		middle[position1].rows_id = temp_rows_id;
		middle[position1].rows_size = tempRowCounter;
		middle[position1].numb_of_parts = numParticipants;
	}else{
		printf("KATIPIGELATHOSMAN\n");
	}

}

void insert_to_middle_predicate(struct middle_table * middle, struct table * table, int size, int relation, int column, int value, int mode)
{
  int relation_position = -1;
  int tmp_relation_position = -1;
  int first_empty = 0;
  int flag = 0;
  int position = -1;
  //	AKOMA KAI EDW OTAN EINAI TO PRWTO EVER DEN PREPEI NA KALEITAI TO RADIX HASH JOIN KAI NA
  //	DIMIOURGOUME TO PRWTO ENDIAMESO ME TA APOTELESMATA TOU?
  //general search for partcipants
  for (int i=0; i<size; i++)
  {
    //relation_position1 = find_relation(relation1, middle[i].participants, middle[i].numb_of_parts)
    //relation_position2 = find_relation(relation2, middle[i].participants, middle[i].numb_of_parts)
    /*Each relation is going in only one place of our middle array so we dont need to
    worry about the next itterations*/
    if (relation_position == -1)
    {
    	tmp_relation_position = find_relation(relation, middle[i].participants, middle[i].numb_of_parts);
    	if(tmp_relation_position >= 0){
    		relation_position = tmp_relation_position;
    	    position = i;
    	    break;
    	    //relation_position = find_relation(relation, middle[i].participants, middle[i].numb_of_parts);
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
  	printf("MPIKA STO DEN IPIRXE I ANISOTITA\n");
    struct result *filter_result;
    middle[first_empty].participants = malloc(sizeof(int));
    middle[first_empty].participants[0] = relation;
    middle[first_empty].numb_of_parts = 1;
    ////////////////////////////////////////////////////////
    filter_result = filterPredicate(&table[relation].my_relation[column], value, mode);
    /*printf("TIPWNW TO APOTELESMA TOU FILTER1\n");
    for (int i = 0; i < filter_result->numRows; ++i)
    {
    	printf("%d\n", filter_result->rowIDsR[i]);
    }*/
    middle[first_empty].rows_id = malloc(sizeof(int *));
    middle[first_empty].rows_id[0] = malloc(sizeof(int)*(filter_result->numRows));
    middle[first_empty].rows_size = filter_result->numRows;
    memcpy(middle[first_empty].rows_id[0], filter_result->rowIDsR, sizeof(int)*filter_result->numRows);

  }
  else
  {
  	printf("MPIKA STO IPIRXE I ANISOTITA\n");
    //printf("first_empty = %d \n",first_empty);
    struct result *filter_result;
    struct relation *temp_rel;
    int index;
    //temp_rel = malloc(sizeof(struct relation));
    //temp_rel->tuples = malloc(sizeof(struct tuple)*middle[position].rows_size);
    //temp_rel->num_tuples = middle[position].rows_size;
    int tempRowCounter = 0;
    int **temp_rows_id;
    int data = 0, position_of_temp=0;
    int i;
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
    		if (table[relation].my_relation[column].tuples[index].payload > value)
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
    		if (table[relation].my_relation[column].tuples[index].payload < value)
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
    		if (table[relation].my_relation[column].tuples[index].payload == value)
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
    	//return NULL;
    }
    /*for(int i=0; i<middle[position].rows_size; i++)
    {
      index = middle[position].rows_id[relation_position][i];
      temp_rel->tuples[i].key = index;
      temp_rel->tuples[i].payload = table[relation].my_relation[column].tuples[index].payload;


    }*/
    /*printf("TIPWNW TON IDI ENDIAMSO\n");
    for(int i=0; i<middle[position].rows_size; i++)
    {
      printf("%d %d\n", temp_rel->tuples[i].key, temp_rel->tuples[i].payload);
    }*/
    /*printf("SE AFTA THA KANW %d %d\n", mode, value);*/
    /*filter_result = filterPredicate(temp_rel, value, mode);
    printf("TA RESULTS %d ENW I ARXIKI %d\n",filter_result->numRows, middle[position].rows_size);*/
    /*printf("TIPWNW TO APOTELESMA TOU FILTER2\n");
    for (int i = 0; i < filter_result->numRows; ++i)
    {
    	printf("%d\n", filter_result->rowIDsR[i]);
    }*/
    /*int tempRowCounter = 0;
    int **temp_rows_id;
    int data = 0, position_of_temp=0;
    int i;
	int participants = middle[position].numb_of_parts;
	position_of_temp =0;
    temp_rows_id = malloc(sizeof(int *)*participants);
    for(int i=0; i<participants; i++)
    {
      temp_rows_id[i] = malloc(sizeof(int)*(middle[position].rows_size));
    }*/
    /*for (int i = 0; i < filter_result->numRows; ++i)
    {
    	for (int j = 0; j < middle[position].rows_size; ++j)
    	{
    		if (filter_result->rowIDsR[i] == middle[position].rows_id[relation_position][j])
    		{
    			for (int k = 0; k < participants; ++k)
    			{
    				temp_rows_id[k][tempRowCounter] = middle[position].rows_id[k][j];
    			}
    			tempRowCounter++;
    			if (tempRowCounter > middle[position].rows_size)
    			{
    				exit(-1);
    			}
    		}
    	}
    }*/
    /*for (int i = 0; i < middle[position].rows_size; ++i)
    {
    	for (int j = 0; j < filter_result->numRows; ++j)
    	{
    		if (middle[position].rows_id[relation_position][i] == filter_result->rowIDsR[j])
    		{
    			for (int k = 0; k < participants; ++k)
    			{
    				temp_rows_id[k][tempRowCounter] = middle[position].rows_id[k][i];
    			}
    			tempRowCounter++;
    			break;
    		}
    	}
    }*/
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
		/*for(i=0; i<middle[position].rows_size; i++)
			{
				if(middle[position].rows_id[position][i] == filter_result->rowIDsR[position_of_temp])
				{
					for(int j=0; j<participants; j++)
					{
						temp_rows_id[j][position_of_temp] = middle[position].rows_id[j][i];
						//data++;
					}
					position_of_temp++;
				}
			}*/
		for(i=0; i<participants; i++)
		{
			free(middle[position].rows_id[i]);
		}
		free(middle[position].rows_id);
		middle[position].rows_id = temp_rows_id;
		middle[position].rows_size = tempRowCounter;
    /*
    free(middle[position].rows_id[0]);
    //free(middle[position].rows_id);
    middle[position].rows_id[0] = malloc(sizeof(int)*filter_result->numRows);
    memcpy(middle[first_empty].rows_id[0], filter_result->rowIDsR, sizeof(int)*filter_result->numRows);
    */
  }

}

void executeBatch(struct batch *my_batch,struct table *relations_table){

  int *predicates_array;

	for (int i = 0; i < my_batch->numQueries; ++i)
	{
		// ISWS NA TO STELNOUME ME &MIDDLE
		struct middle_table *middle;
		middle = create_middle_table(my_batch->queries[i].size2);
		predicates_array = string_parser(my_batch->queries[i], middle, relations_table, my_batch->queries[i].size2);
		struct middle_table mergedMiddle;
		for(int j=0; j < my_batch->queries[i].size2; j++)
		{
			if(middle[j].numb_of_parts > 0)
			{
				mergedMiddle = middle[j];
				//printf("EVRIKA TON %d\n", j);
			}
		}
		// 	TO MERGED MIDDLE EINAI TO KELI STO OPIO EXOUN SIGLINEI OLA TA MIDDLE TABLES STO TELOS
		// NOTE TO SELF NA PROSTHESW MERGERER LATER
		printQueryAndCheckSumResult(&mergedMiddle, relations_table, my_batch->queries[i]);
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
