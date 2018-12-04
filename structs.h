#ifndef STRUCTS_H
#define STRUCTS_H

#include <time.h>
#include <math.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

struct tuple {
  int32_t key;
  int32_t payload;
} tuple;

struct relation{
  struct tuple *tuples; // Pointer to the first tuple
  int32_t num_tuples;   // Total number of tuples in the table
} relation;

struct index_array{
	int table_index; // The table whose bucket we've built an index for
	int total_data;  // The total number of data in the bucket
	int bucket_size; // The size of the bucket array in the index
	int *chain;      // Pointer to the first element of the chain array in the index
	int *bucket;     // Pointer to the first element of the bucket array in the index
};

struct result{
  struct tuple *buffer; // Pointer to the first index of the buffer 
  int elements; // Number of elements in the buffer
  struct result *next;
};

int check_args(int, char **, int *, int *);
int allocate_hist(int **, int);
int allocate_psum(int **, int);
int allocate_histograms(int ***, int ***, int, int);

int allocate_a_table(struct relation **, int);
int allocate_tables(struct relation ***, int, int);

int allocate_a_final_table(struct relation **, int);
int allocate_final_tables(struct relation ***, struct relation **, int);

void fill_hist(struct relation *, int *, int);
void fill_psum(int *, int *, int);
void fill_histograms(struct relation **, int **, int **, int, int);

void rearrange_a_table(struct relation *, struct relation *, int *, int);
void rearrange_tables(int, struct relation **, struct relation **, int **, int);

void get_min_data(int, int, int **, int *, int *);
int is_prime(int);
int allocate_chain(struct index_array *);
int allocate_bucket(struct index_array *);
int fill_an_index(struct index_array *, int, int, int **);
int allocate_index_array(struct index_array **, int, int, int **);
void fill_indeces(struct index_array *, int, int **, struct relation **);

void print_hist(int **, int, int);
void print_psum(int **, int, int);
void print_tables(struct relation **, int);
void print_index_array(int, struct index_array *);
void print_records_no(int, struct relation **);
void print_welcome_msg(int);

int get_user_input(char **, size_t *);

void free_histograms(int ***, int ***, int);
void free_tables(struct relation ***, struct relation ***,int);
void free_indeces(struct index_array **, int);
void free_memory(int ***, int ***, int, struct relation ***, 
  struct relation ***, struct index_array **, int, char **);

#endif