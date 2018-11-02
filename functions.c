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

		table[0][i]->num_tuples = records;
		table[0][i]->tuples = (struct tuple *)malloc(records*
			sizeof(struct tuple));

		if(table[0][i]->tuples == NULL){
			perror("Memory allocation failed: ");
			return -1;
		}

		for(int j = 0; j < records; j++){
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

// Frees every piece of memory that we previously allocated
void free_memory(struct relation **table, struct relation **final_table, 
	int buckets, int ***hist, int ***psum, int no){

	free_table(table,no);
	free_table(final_table,no);
	free_histogram(hist,no,buckets);
	free_histogram(psum,no,buckets);
}