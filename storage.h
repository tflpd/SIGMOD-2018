#ifndef STORAGE_H
#define STORAGE_H

#include <stdio.h>
#include <stdlib.h>
#include "structs.h"
#include <ctype.h>
#include <string.h>

struct relation * store_data(char *);
void free_relation(struct relation *);
int parse_workloads(char *, struct table *);
int count_lines(char *);
struct table * create_table_new(char *);
void free_table_new(struct table *, int);
struct predicate *string_parser(struct query currQuery, struct middle_table *middle, struct table *relations_table, int filterIndex);

#endif
