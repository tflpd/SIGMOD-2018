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
	int table_index;
	int total_data;
	int bucket_size;
	int *chain;
	int *bucket;
};

struct result{
  struct tuple *tuples;
  int num_tuples;
};

int check_args(int, char **, int *, int *);

int create_hist(int ****, int, int);
int create_psum(int ****, int, int);
int create_histograms(int ****, int ****, int, int);
int create_table(struct relation ***, int, int);
int create_final_table(struct relation ***, struct relation **, int);

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
void free_memory(struct relation **, struct relation **, int, int ***, int ***,
	int, struct index_array *);


/*** Phase 2 Functions ***/

int is_prime(int);
int create_chain(int **, int);
int get_bucket_size(int);
int create_index_array(struct index_array **, int, int, int ***);
void print_index_array(int, struct index_array *);
void get_min(int, int, int ***, int *, int *);
int get_min_data(int, int, int ***);
int get_min_index(int, int, int ***);
int create_bucket(int **, int);
int create_indeces(struct index_array *, struct relation **, int, int ***);
int H2(int, int);
#endif //STRUCTS
