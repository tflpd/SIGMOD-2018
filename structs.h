#ifndef STRUCTS_H
#define STRUCTS_H

#include <time.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>

struct tuple {
  int32_t key;
  int32_t payload;
} tuple;

struct relation{
  struct tuple *tuples;
  int32_t num_tuples;
} relation;

struct index_array{
	int table_index; // the index of the table whose bucket we're building an index for
	int total_data;	 // the total number of data in the bucket
	int bucket_size; // the size of the bucket array we'll be building
	int *chain;		 // pointer to the first element of the chain array
	int *bucket;     // pointer to the first element of the bucket array
};

struct result{
  struct tuple *tuples;
  int num_tuples;
};

int check_args(int, char **, int *, int *);

/*** Phase 1 Functions ***/

int create_hist(int ****, int, int);
int create_psum(int ****, int, int);
int create_histograms(int ****, int ****, int, int);
int create_table(struct relation ***, int, int);
int create_final_table(struct relation ***, struct relation **, int);

int copy_psum(int ****, int ***, int, int);

void rearrange_table(struct relation **, struct relation **, int ***, int, int);

void fill_hist(int, struct relation **, int ***, int);
void fill_psum(int ***, int ***, int, int);
void fill_histograms(struct relation **, int ***, int ***, int, int);

void print_table(struct relation **, int);
void print_hist(int ***, int, int);
void print_psum(int ***, int, int);

void free_table(struct relation **, int);
void free_histogram(int ***, int, int);
void free_chain(int, struct index_array *);
void free_bucket(int, struct index_array *);
void free_match(int **, int);
void free_memory(struct relation **, struct relation **, int, int ***, int ***,
	int ***, int, struct index_array *, int **);


/*** Phase 2 Functions ***/

int is_prime(int);

int create_chain(int **, int);
int create_bucket(int **, int);

int get_bucket_size(int);
int create_index_array(struct index_array **, int, int, int ***);
void print_index_array(int, struct index_array *);

void get_min(int, int, int ***, int *, int *);
int get_min_data(int, int, int ***);
int get_min_index(int, int, int ***);

int create_match(int ***, int, struct index_array *);
int fill_indeces(int, struct index_array *, int ***, struct relation **, int **, int **);

void print_chain(int, struct index_array *);
void print_bucket(int, struct index_array *);

#endif //STRUCTS
