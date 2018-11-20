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

// Allocates memory for the histogram array of a table and initializes it
int allocate_hist(int **hist_ptr, int buckets){

	// Allocating memory for the histogram array of the table
	*hist_ptr = (int *)malloc(buckets*sizeof(int));
	if(*hist_ptr == NULL){
		perror("Memory allocation failed: ");
		return -1;
	}

	// Filling in the histogram array with zeros
	for(int i = 0; i < buckets; i++)
		hist_ptr[0][i] = 0;
	
	return 0;
}

// Allocates memory for the psum array of a table and initializes it
int allocate_psum(int **psum_ptr, int buckets){

	*psum_ptr = (int *)malloc(buckets*sizeof(int));
	if(*psum_ptr == NULL){
		perror("Memory allocation failed: ");
		return -1;
	}

	for(int i = 0; i < buckets; i++)
		psum_ptr[0][i] = -1;

	return 0;
}

int allocate_histograms(int ***hist, int ***psum, int tables, int buckets){

	/* Allocating memory for a table of pointers. Each pointer points to the
	   hist array of the corresponding table */ 
	*hist = (int **)malloc(tables*sizeof(int *));
	if(*hist == NULL){
		perror("Memory allocation failed: ");
		return -1;
	}

	*psum = (int **)malloc(tables*sizeof(int *));
	if(*psum == NULL){
		perror("Memory allocation failed: ");
		return -1;
	}

	// Working for one table at a time
	for(int i = 0; i < tables; i++){
		if((allocate_hist(&hist[0][i],buckets) < 0) || 
			allocate_psum(&psum[0][i],buckets) < 0)
			return -1;
	}

	return 0;
}
	
// Creates a table which has random values
int allocate_a_table(struct relation **table_ptr, int records){

	*table_ptr = (struct relation *)malloc(sizeof(struct relation));
	if(*table_ptr == NULL){
		perror("Memory allocation failed: ");
		return -1;
	}

	table_ptr[0]->num_tuples = ((rand()%10/*00*/)+1);/*records*/;
	table_ptr[0]->tuples = (struct tuple *)malloc(table_ptr[0]->num_tuples*
		sizeof(struct tuple));

	if(table_ptr[0]->tuples == NULL){
		perror("Memory allocation failed: ");
		return -1;
	}

	for(int i = 0; i < table_ptr[0]->num_tuples; i++){
		table_ptr[0]->tuples[i].key = rand() % records;
		table_ptr[0]->tuples[i].payload = 0;
	}

	return 0;
}

int allocate_tables(struct relation ***table, int records, int tables){

	// Initially, we're filling in the tables with random integers
	long curtime = time(NULL);
	srand((unsigned int)curtime);

	// Creating an array of pointers. Each one of them points to a table
	*table = (struct relation **)malloc(tables*sizeof(struct relation *));
	if(*table == NULL){
		perror("Memory allocation failed: ");
		return -1;
	}

	// Creating a table
	for(int i = 0; i < tables; i++){
		if(allocate_a_table(&table[0][i],records) < 0)
			return -1;
	}

	return 0;
}

// Working with one table at a time
int allocate_a_final_table(struct relation **final_table_ptr, int num_tuples){
	
	*final_table_ptr = (struct relation *)malloc(sizeof(struct relation));
	if(*final_table_ptr == NULL){
		perror("Memory allocation failed: ");
		return -1;
	}

	final_table_ptr[0]->num_tuples = num_tuples;
	final_table_ptr[0]->tuples = (struct tuple *)malloc(sizeof(struct tuple)*
		final_table_ptr[0]->num_tuples);

	if(final_table_ptr[0]->tuples == NULL){
		perror("Memory allocation failed: ");
		return -1;
	}

	for(int i = 0; i < final_table_ptr[0]->num_tuples; i++){
		final_table_ptr[0]->tuples[i].key = -1;
		final_table_ptr[0]->tuples[i].payload = -1;
	}

	return 0;
}

// Allocates memory for the final tables
int allocate_final_tables(struct relation ***final_table, 
	struct relation **input_table, int total_tables){

	*final_table = (struct relation **)malloc(total_tables*
		sizeof(struct relation *));
	if(*final_table == NULL){
		perror("Memory allocation failed: ");
		return -1;
	}

	for(int i = 0; i < total_tables; i++){
		if(allocate_a_final_table(&final_table[0][i],
			input_table[i]->num_tuples) < 0)
			return -1;
	}

	return 0;
}

// Fills in the hist array of a table with the correct values
void fill_hist(struct relation *table, int *hist, int buckets){

	for(int i = 0; i < table->num_tuples; i++){
		int hash_value = table->tuples[i].key % buckets;
		hist[hash_value]++;
	}
}

// Fills in the psum array of a table with the correct values
void fill_psum(int *hist, int *psum, int buckets){
	
	int last = 0;

	for(int i = 0; i < buckets; i++){

		// If the bucket has at least one element
		if(hist[i] != 0){
			psum[i] = last;
			last += hist[i];
		}
	}
}

// Fills in both of the histogram arrays with the correct values
void fill_histograms(struct relation **input_tables, int **hist, int **psum,
	int buckets, int total_tables){

	for(int i = 0; i < total_tables; i++){
		fill_hist(input_tables[i],hist[i],buckets);
		fill_psum(hist[i],psum[i],buckets);
	}
}

/*******************************/
/*** Rearrangement Functions ***/
/*******************************/

void rearrange_a_table(struct relation *final_table,
	struct relation *input_table, int *psum, int buckets){

	// Iterate the values one by one
	for(int i = 0; i < final_table->num_tuples; i++){

		// Find the index of the bucket whom the value belongs to
		int hash_value = input_table->tuples[i].key % buckets;

		// Find out where the bucket begins
		int index = psum[hash_value];

		// Iterate the final table until you find an empty spot
		while(final_table->tuples[index].key != -1){
			index++;
		}

		final_table->tuples[index].key = input_table->tuples[i].key;
	}	
}

void rearrange_tables(int total_tables, struct relation **input_tables,
	struct relation **final_tables, int **psum, int buckets){

	for(int i = 0; i < total_tables; i++){
		rearrange_a_table(final_tables[i],input_tables[i],psum[i],buckets);
	}
}

/*********************************/
/*** Functions for the Indeces ***/
/*********************************/

/* Compares all of the buckets which have the same index, in order to find the 
   one that has the least amount of data. Then it returns the value
   that represents the least amount of data along with the index of the table 
   it belongs to */
void get_min_data(int total_tables, int bucket_index, int **hist, 
	int *table_index, int *total_data){

	int min_value;
	int min_table;
	int has_min_value = 0;

	// Traverse each one the hist histograms
	for(int i = 0; i < total_tables; i++){

		if(!has_min_value && hist[i][bucket_index] != 0) {
			has_min_value = 1;
			min_value = hist[i][bucket_index];
			min_table = i;
		}
		else if(has_min_value && hist[i][bucket_index] < min_value &&
			hist[i][bucket_index] != 0){
			min_value = hist[i][bucket_index];
			min_table = i;
		}
	}

	/* All of the buckets with index = bucket_index don't have
	   any data in them */
	if(!has_min_value){
		min_value = -1;
		min_table = -1;
	}

	*total_data = min_value;
	*table_index = min_table;
}

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

// Allocating memory for the chain array of a bucket
int allocate_chain(struct index_array *ptr){

	ptr->chain = (int *)malloc((ptr->total_data+1)*sizeof(int));
	if(ptr->chain == NULL){
		perror("Memory allocation failed: ");
		return -1;
	}

	for(int i = 0; i < ptr->total_data+1; i++)
		ptr->chain[i] = -1;

	return 0;
}

int allocate_bucket(struct index_array *ptr){

	int j = ptr->total_data;
	while(is_prime(j) == 0)
		j++;
	ptr->bucket_size = j;

	ptr->bucket = (int *)malloc((ptr->bucket_size)*sizeof(int));
	if(ptr->bucket == NULL){
		perror("Memory allocation failed: ");
		return -1;
	}

	for(int i = 0; i < ptr->bucket_size; i++)
		ptr->bucket[i] = -1;

	return 0;
}

int fill_an_index(struct index_array *ptr, int bucket_index, int total_tables,
	int **hist){

	get_min_data(total_tables,bucket_index,hist,&(ptr->table_index),
		&(ptr->total_data));

	// The bucket whose index is (bucket_index) doesn't have any data
	if(ptr->table_index == -1 && ptr->total_data == -1){
		ptr->bucket_size = -1;
		ptr->chain = NULL;
		ptr->bucket = NULL;
	}
	else{
		if((allocate_chain(ptr) < 0) || (allocate_bucket(ptr) < 0))
			return -1;
	}

	return 0;
}

int allocate_index_array(struct index_array **my_array, int buckets, 
	int total_tables, int **hist){

	*my_array = (struct index_array *)malloc(buckets*sizeof(struct index_array));
	if(*my_array == NULL){
		perror("Memory allocation failed: ");
		return -1;
	}

	for(int i = 0; i < buckets; i++){
		if(fill_an_index(&my_array[0][i],i,total_tables,hist) < 0)
			return -1;
	}

	return 0;
}

void fill_indeces(struct index_array *my_array, int buckets, int **psum, 
	struct relation **final_table){
		
	for(int i = 0; i < buckets; i++){

		// The bucket whose index is (i) doesn't have any data. Move on
		if(my_array[i].table_index == -1)
			continue;

		// The table that the bucket belongs to
		int current_table = my_array[i].table_index;

		// Where the bucket begins in the reordered table
		int start_index = psum[current_table][i];

		// Where the bucket ends in the reordered table
		int end_index = start_index + my_array[i].total_data -1;

		for(int j = end_index; j > start_index -1; j--){

			int hash_value = final_table[current_table]->tuples[j].key % my_array[i].bucket_size;

			/* IMPORTANT NOTE: We have to think of each bucket as an autonomous
			   array, i.e starting from 0 and ending at total_data - 1. 
			   (Match: actual_index - start_index) */

			// This is the first time we've come across such hash value
			if(my_array[i].bucket[hash_value] == -1){
				my_array[i].bucket[hash_value] = ((j+1) - start_index);
				my_array[i].chain[((j+1) - start_index)] = 0;
			}	
			else{

				int tmp = my_array[i].bucket[hash_value];
				while(my_array[i].chain[tmp] != 0){
					tmp = my_array[i].chain[tmp];
				}
				
				my_array[i].chain[tmp] = ((j+1) - start_index);
				my_array[i].chain[((j+1) - start_index)] = 0;
			}
		}
	}
}

/**************************/
/*** Printing Functions ***/
/**************************/

// Prints the histogram array of every table
void print_hist(int **hist, int tables, int buckets){

	for(int i = 0; i < tables; i++){
		for(int j = 0; j < buckets; j++)
			printf("hist[%d][%d]: %d\n",i,j,hist[i][j]);
		
		printf("\n");
	}
}	

// Prints the psum array of every table
void print_psum(int **psum, int tables, int buckets){

	for(int i = 0; i < tables; i++){
		for(int j = 0; j < buckets; j++)
			printf("psum[%d][%d]: %d\n",i,j,psum[i][j]);
		
		printf("\n");
	}
}

// Prints all of the records of every table that's available
void print_tables(struct relation **tables, int total_tables){

	for(int i = 0; i < total_tables; i++){
		for(int j = 0; j < tables[i]->num_tuples; j++){
			printf("table[%d][%d]: %d\n",i,j,tables[i]->tuples[j].key);
		}
		printf("\n");
	}
}

void print_index_array(int buckets, struct index_array *my_array){

	for(int i = 0; i < buckets; i++){
		printf("my_array[%d].table_index: %d\n",i,my_array[i].table_index);
		printf("my_array[%d].total_data: %d\n",i,my_array[i].total_data);
		printf("my_array[%d].bucket_size: %d\n",i,my_array[i].bucket_size);

		if(my_array[i].table_index != -1){
			printf("\n");
			for(int j = 0; j < (my_array[i].total_data)+1; j++)
				printf("my_array[%d].chain[%d]: %d\n",i,j,my_array[i].chain[j]);
		
			printf("\n");
			for(int j = 0; j < my_array[i].bucket_size; j++)
				printf("my_array[%d].bucket[%d]: %d\n",i,j,my_array[i].bucket[j]);
		}
		printf("\n");
	}
}

/**********************************/
/*** Functions that free memory ***/
/**********************************/

void free_histograms(int ***hist, int ***psum, int total_tables){
	for(int i = 0; i < total_tables; i++){
		free(hist[0][i]);
		free(psum[0][i]);
	}

	free(*hist);
	free(*psum);
}

void free_tables(struct relation ***tables, struct relation ***final_tables, 
	int total_tables){
	for(int i = 0; i < total_tables; i++){
		free(tables[0][i]->tuples);
		free(tables[0][i]);

		free(final_tables[0][i]->tuples);
		free(final_tables[0][i]);
	}

	free(*tables);
	free(*final_tables);
}

void free_indeces(struct index_array **my_array, int buckets){

	for(int i = 0; i < buckets; i++){
		if(my_array[0][i].table_index != -1){
			free(my_array[0][i].chain);
			free(my_array[0][i].bucket);
		}
	}

	free(*my_array);
}

// Frees every piece of memory we previously allocated
void free_memory(int ***hist, int ***psum, int total_tables, 
	struct relation ***tables, struct relation ***final_tables,
	struct index_array **my_array, int buckets){

	free_histograms(hist,psum,total_tables);
	free_tables(tables,final_tables,total_tables);
	free_indeces(my_array,buckets);
}