#ifndef STRUCTS_H
#define STRUCTS_H

#define NUMBUCKETS 16 // Number of buckets to be used in 1st part
#define NUMRESULTS 1024 // Number of results per buffer in the list nodes of results of join
#define BIGGER 0 // values used in filterPredicate function to determine the comparing mode
#define LESS 1 // values used in filterPredicate function to determine the comparing mode
#define EQUAL 2 // values used in filterPredicate function to determine the comparing mode
#define JOIN 3 //values used in filterPredicate function to determine the comparing mode
#define R 1
#define S 2

#include <time.h>
#include <math.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

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
  int virtualRelation;
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
  int rows_size;
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
  int* table_indeces; // The relations which we need to load for that query
  int size1; // And their amount
  char** filters; // The filters expressed as strings each one tha we have to resolve
  int size2; // And their amount
  int** projections; // The final projections expresses as int tuples i.e. 1.1 -> 1 1
  int size3; // And their amount
};

struct batch {
  struct query* queries;
  int numQueries;
};


int is_prime(int);

void print_welcome_msg(int);

struct middle_table * create_middle_table(int );
int find_relation(int relation, int *, int);
void insert_to_middle(struct middle_table *middle, struct table *table, int size, struct column r1, struct column r2);
void insert_to_middle_predicate(struct middle_table * middle, struct table * table, int size, struct column r, int value, int mode);
void freeMiddleTable(struct middle_table *, int);
void middle_merge(struct middle_table *, struct middle_table *);
/*-----*/

struct query create_query(int* table_indeces, int size, char** filters, int size_1, int** sum, int size_2);
struct result* RadixHashJoin(struct relation *relationR, struct relation *relationS);
struct result *scanRelations(struct relation *relationR, struct relation *relationS);
struct result *filterPredicate(struct relation *relationR, int comparingValue, int comparingMode);
int *findProjectionsIndeces(int *participants, int numb_of_parts, int ** projections, int numProjections, int *table_indeces);
void printQueryAndCheckSumResult(struct middle_table *mergedMiddle, struct table *table, struct query currQuery);
void executeBatch(struct batch *my_batch,struct table *relations_table);
void printQuery(struct query myQuery);
void freeQuery(struct query myQuery);
void freeBatch(struct batch *myBatch);
void freeResult(struct result *myResult);
void freeRelation(struct relation *myRelation);
void freeMiddle(struct middle_table *myMiddle);
#endif
