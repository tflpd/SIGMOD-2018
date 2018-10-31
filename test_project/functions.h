#ifndef FUNCTIONS_H
#define FUNCTIONS_H

#include <time.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>

typedef struct tuple{
	int32_t key;
	int32_t payload;
}tuple;
	
typedef struct relation{
	tuple *tuples;
	int32_t num_tuples;
}relation;

int check_args(int, char **, int *, int *);
int create_table(relation ***, int);
int create_histograms(int ***, int ***, int, int);
int H1(int, int);
void fill_hist(int, relation **, int **, int);
void fill_psum(int, int **, int **, int);
void free_memory(relation ***, relation ***, int ***, int ***, int);
void print_tables(relation **, int);
int create_final_table(relation ***, relation ***,int,int**,int);

#endif