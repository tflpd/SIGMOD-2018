#ifndef STRUCTS_H
#define STRUCTS_H

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

int check_args(int, char **, int *, int *);
int create_hist(int ***, int);
int create_psum(int ***, int);
int create_table(struct relation **, int);
void fill_hist(int, int, struct relation *, int **);
void fill_psum(int **, int **, int);
void print_table(struct relation *);
void print_hist(int **, int);
void print_psum(int **, int);
void create_reordered(struct relation *, struct relation *, int **, int);
void free_memory(struct relation *, struct relation *, int, int **, int **);

#endif //STRUCTS