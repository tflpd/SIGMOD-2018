#ifndef STRUCTS_H
#define STRUCTS_H

#define NUMBUCKETS 16 // Number of buckets to be used in 1st part
#define R 1
#define S 2

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

struct bucketIndex{
    int minTuples; // The relation with the minimun tuples for which we have an index: R or S
    int numTuples; // The numbe of tuples in that bucket - index
    int *bucket;
    int bucketSize;
    int *chain;
  };

struct result{
  int *rowIDsR;
  int *rowIDsS;
  int numRows;
};

struct column{
  int table;
  int column;
};

struct table{
  struct relation *my_relation;
  u_int64_t tuples;
  u_int64_t columns;
};

// One result of join coupling two row ID's
struct rowIDtuple {
  int keyR;
  int keyS;
};

struct middle_table{
  int *participants;
  int **rows_id;
  int numb_of_parts;
};

// A list node
struct lnode {
  int32_t key; // They key/id of that node
  struct rowIDtuple* buffer; // The buffer to be filled with tuples
  int32_t counter; // Counter of the current tuples in the buffer
  struct lnode *next; // Pointer to the next list node
};

// A list struct
struct my_list {
  struct lnode* head; // Pointer to the first node of the list
  struct lnode* current; // Pointer to the last node of the list
  int32_t nodes_counter; // Counter of the nodes of the list
  int32_t buffer_max_cap; // Max number of tuples that should fit in each node
};

struct query {
  int* table_indeces;
  int size1;
  char** filters;
  int size2;
  int** projections;
  int size3;
};

struct batch {
  struct query* queries;
  int numQueries;
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
struct middle_table * create_middle_table(int );
int find_relation(int relation, int *, int);
void insert_to_middle(struct middle_table *, struct table *, int, int, int, int, int);
/*-----*/

struct query* create_query(int* table_indeces, int size, char** filters, int size_1, int** sum, int size_2);
struct result* RadixHashJoin(struct relation *relationR, struct relation *relationS);
#endif
