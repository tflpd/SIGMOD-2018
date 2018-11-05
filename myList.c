#ifndef MYLIST_H
#define MYLIST_H

#include <stdio.h>
#include <stdlib.h>
#include "structs.h"

struct lnode {
	int32_t key;
	struct tuple* buffer;
	int32_t counter;
	struct lnode *next;
}

struct my_list{
	struct lnode* head;
	struct lnode* current;
	int32_t nodes_counter;
	//TO MAX CAP METRIETE KSEKINONTAS APO 0 ARA PX GIA CAP 2 EXW MAX CAP 1
	int32_t buffer_max_cap;
}

struct my_list* list_init(int32_t buffer_max_cap){
	struct my_list* tmp;
	tmp = malloc(sizeof(struct my_list));
	tmp->head = NULL;
	tmp->current = NULL;
	tmp->nodes_counter = 0;
	tmp->buffer_max_cap = buffer_max_cap;
	return tmp;
}

struct my_list* add_node(struct my_list* list_pointer){
	struct lnode* tmp;
	tmp = malloc(sizeof(struct lnode));
	// TO KEY EDW DEN KSERW TI PREPEI NA NAI TO VAZE SE OLA 0 GIA TWRA
	tmp->key = 0;
	tmp->buffer = malloc(sizeof(tuple)*(list_pointer->buffer_max_cap));
	tmp->counter = 0;
	tmp->next = NULL;

	if (list_pointer->head == NULL)
	{
		list_pointer->head = tmp;
		list_pointer->current = list_pointer->head;
	}else{
		list_pointer->current->next = tmp;
		list_pointer->current = list_pointer->current->next;
	}
	list_pointer->nodes_counter++;
	return list_pointer;
}

struct my_list* add_to_buff(struct my_list* list_pointer, struct tuple new_tuple){
	if ((list_pointer->head == NULL) || (list_pointer->current->counter == list_pointer->buffer_max_cap))
	{
		list_pointer = add_node(list_pointer);
	}
	list_pointer->current->buffer[list_pointer->current->counter] = new_tuple;
	list_pointer->current->counter++;

void delete_list(struct my_list* list_pointer){
	struct node* tmp;
	while(list_pointer->head != list_pointer->current){
		tmp = list_pointer->head;
		list_pointer->head = list_pointer->head->next;

		tmp->next = NULL;
		free(tmp->buffer);
		free(tmp);
	}
	tmp = list_pointer->head;
	list_pointer->head = NULL;
	list_pointer->current = NULL;

	free(tmp->buffer);
	free(tmp);
	free(list_pointer);
}

void print_list(struct my_list* list_pointer){
	struct node* tmp;
	tmp = list_pointer->head;
	while(tmp != list_pointer->current){
		print_node(tmp);
		tmp = tmp->next;
	}
	print_node(tmp);
}

void print_node(struct lnode* node){
	printf("Now printing node with key: %d\n", node->key);
	for (int i = 0; i < node->counter; ++i)
	{
		printf("%d\n", node->buffer[i].);
	}
	printf("\n");
}

}

#endif