#include "structs.h"

// Checks the arguments that were provided in the command line
int check_args(int argc, char **argv, int *buckets, int *N){

	if(argc != 2){
		printf("Error! Invalid arguments\n");
		return -1;
	}

	*N = atoi(argv[1]);
	*buckets = pow(2,*N);

	return 0;
}

int create_hist(int ***hist, int size){

	*hist = (int **)malloc(size*sizeof(int *));
	if(*hist == NULL){
		perror("Memory allocation failed: ");
		return -1;
	}

	for(int i = 0; i < size; i++){

		hist[0][i] = (int *)malloc(2*sizeof(int));
		if(hist[0][i] == NULL){
			perror("Memory allocation failed: ");
			return -1;
		}

		hist[0][i][0] = -1;
		hist[0][i][1] = 0;
	}

	return 0;
}

int create_psum(int ***psum, int size){

	*psum = (int **)malloc(size*sizeof(int *));
	if(*psum == NULL){
		perror("Memory allocation failed: ");
		return -1;
	}

	for(int i = 0; i < size; i++){

		psum[0][i] = (int *)malloc(2*sizeof(int));
		if(psum[0][i] == NULL){
			perror("Memory allocation failed: ");
			return -1;
		}

		psum[0][i][0] = -1;
		psum[0][i][1] = -1;
	}

	return 0;
}

int create_table(struct relation **table, int records){

	*table = (struct relation *)malloc(sizeof(relation));
	if(*table == NULL){
		perror("Memory allocation failed: ");
		return -1;
	}

	table[0]->num_tuples = records;
	table[0]->tuples = (struct tuple *)malloc(records*sizeof(struct tuple));

	for(int i = 0; i < records; i++){
		table[0]->tuples[i].key = rand()%records;
	}

	return 0;
}

void fill_hist(int records, int buckets, struct relation *table, int **hist){

	for(int i = 0; i < records; ++i){
		
		int hash = table->tuples[i].key%buckets;
		if(hist[hash][0] == -1){
			hist[hash][0] = hash;
		}
		
		hist[hash][1] += 1;
	}
}

void fill_psum(int **hist, int **psum, int buckets){

	int sum = 0;
	for (int i = 0; i < buckets; ++i){
		
		if (hist[i][0] != -1){
			psum[i][0] = hist[i][0];
			psum[i][1] = sum;
			sum += hist[i][1];
		}
	}
}

void print_table(struct relation *table){

	for(int i = 0; i < table->num_tuples; i++)
		printf("record[%d]: %d\n",i,table->tuples[i].key);
}

void print_hist(int **hist, int buckets){

	for(int i = 0; i < buckets; i++)
		printf("hist[%d]: %d\n",i,hist[i][1]);
}

void print_psum(int **psum, int buckets){

	for(int i = 0; i < buckets; i++)
		printf("psum[%d]: %d\n",i,psum[i][1]);
}

void create_reordered(struct relation *table, struct relation *final_table, 
	int **psum, int buckets){

	for(int i = 0; i < table->num_tuples; i++){
		
		int hash;
		hash = table->tuples[i].key%buckets;

		final_table->num_tuples = table->num_tuples;
		final_table->tuples[psum[hash][1]] = table->tuples[i];
		psum[hash][1]++;
	}
}

void free_memory(struct relation *table, struct relation *final_table, 
	int buckets, int **hist, int **psum){

	free(table->tuples);
	free(final_table->tuples);
	free(table);
	free(final_table);
	
	for(int i = 0; i < buckets; ++i){
		free(hist[i]);
	}
	free(hist);
	
	for(int i = 0; i < buckets; ++i){
		free(psum[i]);
	}
	free(psum);
}