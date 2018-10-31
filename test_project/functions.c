#include "functions.h"

// Checks the arguments that were provided
int check_args(int argc, char **argv, int *N, int *buckets){

	if(argc != 2){
		printf("Error: Invalid number of arguments\n");
		return -1;
	}

	*N = atoi(argv[1]);
	*buckets = pow(2,*N);

	return 0;
}

// Creates the two tables that participate in the join operation
int create_table(relation ***tables, int no){

	long curtime = time(NULL);
	srand((unsigned int)curtime);

	*tables = (relation **)malloc(no*sizeof(relation *));
	if(*tables == NULL){
		perror("Memory allocation failed: ");
		return -1;
	}

	for(int i = 0; i < no; i++){

		tables[0][i] = (relation *)malloc(sizeof(relation));
		if(tables[0][i] == NULL){
			perror("Memory allocation failed: ");
			return -1;
		}

		tables[0][i][0].num_tuples = ((rand()%10/*00*/)+1);

		tables[0][i][0].tuples = (tuple *)malloc(tables[0][i][0].num_tuples*
			sizeof(tuple));
		if(tables[0][i][0].tuples == NULL){
			perror("Memory allocation failed: ");
			return -1;
		}

		for(int j = 0; j < tables[0][i][0].num_tuples; j++){
			tables[0][i][0].tuples[j].key = rand() % 1024;
			tables[0][i][0].tuples[j].payload = 0;
		}
	}

	return 0;
}

// Creates both of the necessary histograms
int create_histograms(int ***hist, int ***psum, int buckets, int no){

	*hist = (int **)malloc(no*sizeof(int *));
	if(*hist == NULL){
		perror("Memory allocation failed: ");
		return -1;
	}

	for(int i = 0; i < no; i++){
		hist[0][i] = (int *)malloc(buckets*sizeof(int));
		if(hist[0][i] == NULL){
			perror("Memory allocation failed: ");
			return -1;
		}

		for(int j = 0; j < buckets; j++)
			hist[0][i][j] = 0;
	}

	*psum = (int **)malloc(no*sizeof(int *));
	if(*psum == NULL){
		perror("Memory allocation failed: ");
		return -1;
	}

	for(int i = 0; i < no; i++){

		psum[0][i] = (int *)malloc(buckets*sizeof(int));
		if(psum[0][i] == NULL){
			perror("Memory allocation failed: ");
			return -1;
		}

		for(int j = 0; j < buckets; j++)
			psum[0][i][j] = -1;
	}

	return 0;
}

// The first hash function we'll be using
int H1(int value, int buckets){
	return(value%buckets);
}

// Filling in hist for each table
void fill_hist(int buckets, relation **tables, int **hist, int no){

	for(int i = 0; i < no; i++){
		for(int j = 0; j < tables[i][0].num_tuples; j++){
			int hash_value = H1(tables[i][0].tuples[j].key,buckets);
			hist[i][hash_value]++;
		}
	}
}
	
// Filling in psum for each table
void fill_psum(int buckets, int **hist, int **psum, int no){

	for(int i = 0; i < no; i++){		
		int last = 0;
		for(int j = 0; j < buckets; j++){

			if(hist[i][j] != 0){
				psum[i][j] = last;
				last += hist[i][j];
			}
		}
	}
}

// Releases every piece of memory we previously allocated
void free_memory(relation ***tables, relation ***final_tables, 
	int ***hist, int ***psum, int no){

	for(int i = 0; i < no; i++){
		free(tables[0][i][0].tuples);
		free(tables[0][i]);

		free(final_tables[0][i][0].tuples);
		free(final_tables[0][i]);
	}

	free(*tables);
	free(*final_tables);

	for(int i = 0; i < no; i++){
		free(hist[0][i]);
		free(psum[0][i]);
	}

	free(*hist);
	free(*psum);
}

// Prints the values of both tables
void print_tables(relation **tables, int no){
	for(int i = 0; i < no; i++){
		for(int j = 0; j < tables[i][0].num_tuples; j++){
			printf("table[%d][%d]: %d\n",i,j,tables[i][0].tuples[j].key);
		}
		printf("\n\n");
	}
}

// Re-ordered versions of the original tables
int create_final_table(relation ***final_tables, relation ***tables, int no, 
	int **psum, int buckets){

	*final_tables = (relation **)malloc(no*sizeof(relation *));
	if(final_tables == NULL){
		perror("Memory allocation failed: ");
		return -1;
	}

	for(int i = 0; i < no; i++){

		final_tables[0][i] = (relation *)malloc(sizeof(relation));
		if(final_tables[0][i] == NULL){
			perror("Memory allocation failed: ");
			return -1;
		}

		final_tables[0][i][0].num_tuples = tables[0][i][0].num_tuples;

		final_tables[0][i][0].tuples = (tuple *)malloc(final_tables[0][i][0].num_tuples*
			sizeof(tuple));
		if(final_tables[0][i][0].tuples == NULL){
			perror("Memory allocation failed: ");
			return -1;
		}

		for(int j = 0; j < final_tables[0][i][0].num_tuples; j++){
			final_tables[0][i][0].tuples[j].key = -1;
			final_tables[0][i][0].tuples[j].payload = 0;
		}

		for(int j = 0; j < tables[0][i][0].num_tuples; j++){

			int hash_value;
			int index; 
			
			hash_value = H1(tables[0][i][0].tuples[j].key,buckets);
			index = psum[i][hash_value];

			while(final_tables[0][i][0].tuples[index].key != -1){
				index++;
			}

			final_tables[0][i][0].tuples[index].key = tables[0][i][0].tuples[j].key;
		}
	}
}