#ifndef MYLIST_H
#define MYLIST_H

#include <stdio.h>
#include <stdlib.h>
#include "structs.h"

struct my_list* list_init(int32_t); // List struct initialization
struct my_list* add_node(struct my_list*); // Adding a new node when needed
struct my_list* add_to_buff(struct my_list*, int keyR, int keyS); // Adding a tuple to the list
void delete_list(struct my_list*);
void print_node(struct lnode*); // Printing a specific node
void print_list(struct my_list*); // Printing the whole list

#endif