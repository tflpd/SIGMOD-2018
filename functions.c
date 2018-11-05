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

int create_hist(int ****hist, int buckets, int no){

	// Creating one hist for each one of the no tables
	*hist = (int ***)malloc(no*sizeof(int **));
	if(*hist == NULL){
		perror("Memory allocation failed: ");
		return -1;
	}

	for(int i = 0; i < no; i++){
		hist[0][i] = (int **)malloc(buckets*sizeof(int *));
		if(hist[0][i] == NULL){
			perror("Memory allocation failed: ");
			return -1;
		}

		for(int j = 0; j < buckets; j++){
			hist[0][i][j] = (int *)malloc(2*sizeof(int));
			if(hist[0][i][j] == NULL){
				perror("Memory allocation failed: ");
				return -1;
			}

			hist[0][i][j][0] = -1;
			hist[0][i][j][1] = 0;
		}
	}

	return 0;
}

int create_psum(int ****psum, int buckets, int no){

	*psum = (int ***)malloc(no*sizeof(int **));
	if(*psum == NULL){
		perror("Memory allocation failed: ");
		return -1;
	}

	for(int i = 0; i < no; i++){
		psum[0][i] = (int **)malloc(buckets*sizeof(int *));
		if(psum[0][i] == NULL){
			perror("Memory allocation failed: ");
			return -1;
		}

		for(int j = 0; j < buckets; j++){
			psum[0][i][j] = (int *)malloc(2*sizeof(int));
			if(psum[0][i][j] == NULL){
				perror("Memory allocation failed: ");
				return -1;
			}

			psum[0][i][j][0] = -1;
			psum[0][i][j][1] = -1;
		}
	}

	return 0;
}

int create_histograms(int ****hist, int ****psum, int buckets, int no){

	if((create_hist(hist,buckets,no) < 0) ||
		(create_psum(psum,buckets,no) < 0))
		return -1;
	else
		return 0;
}

int create_table(struct relation ***table, int records, int no){

	long curtime = time(NULL);
	srand((unsigned int)curtime);

	*table = (struct relation **)malloc(no*sizeof(struct relation *));
	if(*table == NULL){
		perror("Memory allocation failed: ");
		return -1;
	}

	for(int i = 0; i < no; i++){
		table[0][i] = (struct relation *)malloc(sizeof(relation));
		if(table[0][i] == NULL){
			perror("Memory allocation failed: ");
			return -1;
		}

		table[0][i]->num_tuples = ((rand()%10/*00*/)+1);/*records*/;
		table[0][i]->tuples = (struct tuple *)malloc(table[0][i]->num_tuples/*records*/*
			sizeof(struct tuple));

		if(table[0][i]->tuples == NULL){
			perror("Memory allocation failed: ");
			return -1;
		}

		for(int j = 0; j < table[0][i]->num_tuples/*records*/; j++){
			table[0][i]->tuples[j].key = rand()%records;
			table[0][i]->tuples[j].payload = 0;
		}
	}

	return 0;
}

int create_final_table(struct relation ***final_table,
	struct relation **orig_table, int no){

	*final_table = (struct relation **)malloc(no*sizeof(struct relation *));
	if(*final_table == NULL){
		perror("Memory allocation failed: ");
		return -1;
	}

	for(int i = 0; i < no; i++){

		final_table[0][i] = (struct relation *)malloc(sizeof(struct relation));
		if(final_table[0][i] == NULL){
			perror("Memory allocation failed: ");
			return -1;
		}

		final_table[0][i]->num_tuples = orig_table[i]->num_tuples;
		final_table[0][i]->tuples = (struct tuple *)malloc(sizeof(struct tuple)*
			final_table[0][i]->num_tuples);
		if(final_table[0][i]->tuples == NULL){
			perror("Memory allocation failed: ");
			return -1;
		}

		for(int j = 0; j < final_table[0][i]->num_tuples; j++){
			final_table[0][i]->tuples[j].key = -1;
			final_table[0][i]->tuples[j].payload = -1;
		}
	}

	return 0;
}

// Copies the psum array of every table
int copy_psum(int ****psum_copy, int ***psum, int buckets, int no){

	*psum_copy = (int ***)malloc(no*sizeof(int ***));
	if(*psum_copy == NULL){
		perror("Memory allocation failed: ");
		return -1;
	}

	for(int i = 0; i < no; i++){
		psum_copy[0][i] = (int **)malloc(buckets*sizeof(int *));
		if(psum_copy[0][i] == NULL){
			perror("Memory allocation failed: ");
			return -1;
		}

		for(int j = 0; j < buckets; j++){
			psum_copy[0][i][j] = (int *)malloc(2*sizeof(int));
			if(psum_copy[0][i][j] == NULL){
				perror("Memory allocation failed: ");
				return -1;
			}

			psum_copy[0][i][j][0] = psum[i][j][0];
			psum_copy[0][i][j][1] = psum[i][j][1];
		}
	}

	return 0;
}

// Rearranges the values of the tables according to their bucket
void rearrange_table(struct relation **table, struct relation **final_table,
	int ***psum, int buckets, int no){

	for(int i = 0; i < no; i++){
		for(int j = 0; j < table[i]->num_tuples; j++){
			int hash;
			hash = table[i]->tuples[j].key%buckets;

			final_table[i]->tuples[psum[i][hash][1]] = table[i]->tuples[j];
			psum[i][hash][1]++;
		}
	}
}

/* Creating the histogram. Each row of it has the hash of the bucket on the
   left and the number of appearences on the right */
void fill_hist(int buckets, struct relation **table, int ***hist, int no){

	for(int i = 0; i < no; i++){
		for(int j = 0; j < table[i]->num_tuples; j++){

			int hash = table[i]->tuples[j].key % buckets;
			if(hist[i][hash][0] == -1){
				hist[i][hash][0] = hash;
			}

			hist[i][hash][1] += 1;
		}
	}
}

/* Creating the accumulative histogram. Each row has the hash of the bucket
   on the left and the base of the bucket on the right */
void fill_psum(int ***hist, int ***psum, int buckets, int no){

	for(int i = 0; i < no; i++){
		int sum = 0;
		for(int j = 0; j < buckets; j++){
			if(hist[i][j][0] != -1){
				psum[i][j][0] = hist[i][j][0];
				psum[i][j][1] = sum;
				sum += hist[i][j][1];
			}
		}
	}
}

void fill_histograms(struct relation **table, int ***hist, int ***psum,int no,
	int buckets){

	fill_hist(buckets,table,hist,no);
	fill_psum(hist,psum,buckets,no);
}

// Prints all of the records of every table that's available
void print_table(struct relation **table, int no){

	for(int i = 0; i < no; i++){
		printf("** TABLE %d **\n",i);
		for(int j = 0; j < table[i]->num_tuples; j++){
			printf("record[%d].key = %d\n",j,table[i]->tuples[j].key);
			// printf("record[%d].payload = %d\n",j,table[i]->tuples[j].payload);
		}
		printf("\n\n");
	}
}

// Prints the values of every hist histogram
void print_hist(int ***hist, int buckets, int no){

	for(int i = 0; i < no; i++){
		printf("** TABLE %d **\n",i);
		for(int j = 0; j < buckets; j++){
			// printf("hist[0] for bucket[%d]: %d\n",j,hist[i][j][0]);
			printf("hist[1] for bucket[%d]: %d\n",j,hist[i][j][1]);
		}
		printf("\n\n");
	}
}

// Prints the values of every psum histogram
void print_psum(int ***psum, int buckets, int no){

	for(int i = 0; i < no; i++){
		printf("** TABLE %d **\n",i);
		for(int j = 0; j < buckets; j++){
			// printf("psum[0] for bucket[%d]: %d\n",j,psum[i][j][0]);
			printf("psum[1] for bucket[%d]: %d\n",j,psum[i][j][1]);
		}
		printf("\n\n");
	}
}

void free_table(struct relation **table, int no){
	for(int i = 0; i < no; i++){
		free(table[i]->tuples);
		free(table[i]);
	}
	free(table);
}

void free_histogram(int ***histogram, int no, int buckets){
	for(int i = 0; i < no; i++){
		for(int j = 0; j < buckets; j++)
			free(histogram[i][j]);
		free(histogram[i]);
	}
	free(histogram);
}

void free_chain(int buckets, struct index_array *my_array){
	for(int i = 0; i < buckets; i++){
		if(my_array[i].chain != NULL)
			free(my_array[i].chain);
	}
}

void free_bucket(int buckets, struct index_array *my_array){
	for(int i = 0; i < buckets; i++){
		if(my_array[i].bucket != NULL)
			free(my_array[i].bucket);
	}
}

void free_match(int **match, int buckets){

	for(int i = 0; i < buckets; i++){
		if(match[i] != NULL)
			free(match[i]);
	}

	free(match);
}

// Frees every piece of memory that we previously allocated
void free_memory(struct relation **table, struct relation **final_table,
	int buckets, int ***hist, int ***psum, int ***psum_copy, int no,
	struct index_array *my_array, int **match){

	free_table(table,no);
	free_table(final_table,no);
	free_histogram(hist,no,buckets);
	free_histogram(psum,no,buckets);
	free_histogram(psum_copy,no,buckets);
	free_chain(buckets,my_array);
	free_bucket(buckets,my_array);
	free_match(match,buckets);
	free(my_array);
}

// Checks whether the provided number is a prime or not
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

// Creates the chain array for a certain bucket of a table
int create_chain(int **chain_array, int chain_size){

	*chain_array = (int *)malloc(chain_size*sizeof(int));
	if(*chain_array == NULL){
		perror("Memory allocation failed: ");
		return -1;
	}

	for(int i = 0; i < chain_size; i++)
		chain_array[0][i] = -1;
}

int create_bucket(int **bucket_array, int bucket_size){

	*bucket_array = (int *)malloc(bucket_size*sizeof(int));
	if(*bucket_array == NULL){
		perror("Memory allocation failed: ");
		return -1;
	}

	for(int i = 0; i < bucket_size; i++)
		bucket_array[0][i] = -1;
}

// Finds the right size for the bucket array.
int get_bucket_size(int size_of_bucket){

	int i;
	for(i = size_of_bucket;; i++){
		if(is_prime(i))
			break;
	}

	return i;
}

int H2(int data_value, int bucket_size){
	return data_value%bucket_size;
}

// Creates the array which takes us to the indexes of each set of buckets
int create_index_array(struct index_array **my_array, int buckets, int no,
	int ***hist){

	*my_array = (struct index_array *)malloc(buckets*sizeof(struct index_array));
	if(*my_array == NULL){
		perror("Memory allocation failed: ");
		return -1;
	}

	for(int i = 0; i < buckets; i++){

		/* The above values will remain the same only if the bucket,
		   whose index is bucket_index, doesn't have any values */
		my_array[0][i].table_index = get_min_index(no,i,hist);
		my_array[0][i].total_data = get_min_data(no,i,hist);
		my_array[0][i].bucket_size = -1;
		my_array[0][i].chain = NULL;
		my_array[0][i].bucket = NULL;

		// At least one bucket whose index is i, has been created
		if(my_array[0][i].table_index != -1){
			if(create_chain(&my_array[0][i].chain,my_array[0][i].total_data+1) < 0)
				return -1;

			int j = my_array[0][i].total_data;
			while(is_prime(j) == 0)
				j++;
			my_array[0][i].bucket_size = j;

			if(create_bucket(&my_array[0][i].bucket,my_array[0][i].bucket_size) < 0)
				return -1;
		}
	}

	return 0;
}

// Prints all values for each one of the elements of the index array
void print_index_array(int buckets, struct index_array *my_array){

	for(int i = 0; i < buckets; i++){
		printf("my_array[%d]: \n",i);
		printf("* table_index: %d\n",my_array[i].table_index);
		printf("* total_data: %d\n",my_array[i].total_data);
		printf("* bucket_size: %d\n",my_array[i].bucket_size);
		printf("* chain: %p\n",my_array[i].chain);
		printf("* bucket: %p\n\n",my_array[i].bucket);
	}
}

void get_min(int no, int bucket_index, int ***hist, int *table_index, int *min){

	int min_value;
	int min_table;
	int has_min_value = 0;

	// Traverse each one the hist histograms
	for(int i = 0; i < no; i++){

		if(!has_min_value && hist[i][bucket_index][1] != 0) {
			has_min_value = 1;
			min_value = hist[i][bucket_index][1];
			min_table = i;
		}
		else if(has_min_value && hist[i][bucket_index][1] < min_value &&
			hist[i][bucket_index][1] != 0){
			min_value = hist[i][bucket_index][1];
			min_table = i;
		}
	}

	if(has_min_value == 0){
		*min = -1;
		*table_index = -1;
	}
	else{
		*min = min_value;
		*table_index = min_table;
	}
}

/* Compares sets of buckets whose index is the same, in order to find the one
   with the least data along with the table it belongs to */
int get_min_data(int no, int bucket_index, int ***hist){

	int min_value;
	int min_table;
	int has_min_value = 0;

	// Traverse each one the hist histograms
	for(int i = 0; i < no; i++){

		if(!has_min_value && hist[i][bucket_index][1] != 0) {
			has_min_value = 1;
			min_value = hist[i][bucket_index][1];
			min_table = i;
		}
		else if(has_min_value && hist[i][bucket_index][1] < min_value &&
			hist[i][bucket_index][1] != 0){
			min_value = hist[i][bucket_index][1];
			min_table = i;
		}
	}

	/* All of the buckets with index = bucket_index don't have
	   any data in them */
	if(!has_min_value)
		min_value = -1;

	return min_value;
}

/* Returns the index of the table whose bucket_index bucket has
   the least elements */
int get_min_index(int no, int bucket_index, int ***hist){

	int min_value;
	int min_table;
	int has_min_value = 0;

	// Traverse each one the hist histograms
	for(int i = 0; i < no; i++){

		if(!has_min_value && hist[i][bucket_index][1] != 0) {
			has_min_value = 1;
			min_value = hist[i][bucket_index][1];
			min_table = i;
		}
		else if(has_min_value && hist[i][bucket_index][1] < min_value &&
			hist[i][bucket_index][1]){
			min_value = hist[i][bucket_index][1];
			min_table = i;
		}
	}

	/* All of the buckets with index = bucket_index don't have
	   any data in them */
	if(!has_min_value)
		min_table = -1;

	return min_table;
}

// Allocates the necessary memory for the match structure
int create_match(int ***match, int buckets, struct index_array *my_array){

	*match = (int **)malloc(buckets*sizeof(int *));
	if(*match == NULL){
		perror("Memory allocation failed: ");
		return -1;
	}

	for(int i = 0; i < buckets; i++){

		// This bucket doesn't exist
		if(my_array[i].chain == NULL)
			match[0][i] = NULL;
		else{
			match[0][i] = (int *)malloc(my_array[i].total_data*sizeof(int));
			if(match[0][i] == NULL){
				perror("Memory allocation failed: ");
				return -1;
			}
		}
	}

	return 0;
}

int fill_indeces(int buckets, struct index_array *my_array, int ***psum, 
	struct relation **final_table, int **bucket_copy, int **match){

	// Iterate the buckets
	for(int i = 0; i < buckets; i++){
		
		/* We won't be creating an index for this bucket, 
		   because it doesn't have any data */
		if(my_array[i].chain == NULL)
			continue;
		
		printf("Working on bucket[%d]\n",i);

		int current_table = my_array[i].table_index;
		int start_index = psum[current_table][i][1];
		int end_index = start_index + my_array[i].total_data - 1;

		/* Matching the indeces of each value of the bucket to the indeces of
		   the match array */
		int tmp = start_index;
		for(int z = 0; z < my_array[i].total_data; z++){
			match[i][z] = tmp;
			tmp++;
		}

		// We will be working with a copy of the bucket
		*bucket_copy = (int *)malloc(my_array[i].total_data*sizeof(int));
		if(*bucket_copy == NULL){
			perror("Memory allocation failed: ");
			return -1;
		}
		
		int current_index = my_array[i].total_data - 1;
		
		// Iterate the data of the bucket and copy it
		for(int j = end_index; j >= start_index; j--){
			int value = final_table[current_table][0].tuples[j].key;
			bucket_copy[0][current_index] = value;
			current_index--;
		}

		/*** DOES NOT WORK PROPERLY ***/

		/* Iterate the data of the bucket in descending order to fill both of 
		   the chain and bucket arrays */
		for(int j = my_array[i].total_data - 1; j >= 0; j--){
			
			int hash_value = bucket_copy[0][j] % my_array[i].bucket_size;

			// First time we've come across this hash value
			if(my_array[i].bucket[hash_value] == -1){

				my_array[i].bucket[hash_value] = j+1;
				my_array[i].chain[j+1] = 0;
			}
			else{

				int index_to_chain = my_array[i].bucket[hash_value];

				while(my_array[i].chain[index_to_chain] != 0){
					index_to_chain = my_array[i].chain[index_to_chain];
				}

				my_array[i].chain[index_to_chain] = j+1;
			}
		}	

		free(*bucket_copy);
	}
	return 0;
}