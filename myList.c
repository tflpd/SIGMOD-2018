#include "myList.h"

struct my_list* list_init(int32_t buffer_max_cap){
	//printf("Creating new list.\n");
	struct my_list* tmp;
	tmp = malloc(sizeof(struct my_list));
	tmp->head = NULL;
	tmp->current = NULL;
	tmp->nodes_counter = -1;
	tmp->buffer_max_cap = buffer_max_cap;
	return tmp;
}

struct my_list* add_node(struct my_list* list_pointer){
	struct lnode* tmp;
	tmp = malloc(sizeof(struct lnode));
	//printf("Creating new node.\n");
	list_pointer->nodes_counter++;
	tmp->key = list_pointer->nodes_counter;
	tmp->buffer = malloc(sizeof(struct rowIDtuple)*(list_pointer->buffer_max_cap));
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
	return list_pointer;
}

struct my_list* add_to_buff(struct my_list* list_pointer, int keyR, int keyS){
	if ((list_pointer->head == NULL) || (list_pointer->current->counter == list_pointer->buffer_max_cap))
	{
		list_pointer = add_node(list_pointer);
	}
	//printf("Added new tuple.\n");
	list_pointer->current->buffer[list_pointer->current->counter].keyR = keyR;
	list_pointer->current->buffer[list_pointer->current->counter].keyS = keyS;
	list_pointer->current->counter++;
	return list_pointer;
}

void delete_list(struct my_list* list_pointer){
	struct lnode* tmp;
	tmp  = NULL;
	//printf("Initating deleting.\n");
	if (list_pointer->head != NULL)
	{
		while(list_pointer->head->key != list_pointer->current->key){
				tmp = list_pointer->head;
				list_pointer->head = list_pointer->head->next;

				tmp->next = NULL;
				free(tmp->buffer);
				free(tmp);
			}
			tmp = list_pointer->head;
			list_pointer->head = NULL;
			list_pointer->current = NULL;
	}
	if (tmp != NULL)
	{
		free(tmp->buffer);
		free(tmp);
	}
	free(list_pointer);
}

void print_node(struct lnode* node){
	//printf("Now printing node with key: %d\n", node->key);
	for (int i = 0; i < node->counter; ++i)
	{
		printf("%d %d\n", node->buffer[i].keyR, node->buffer[i].keyS);
	}
	printf("\n");
}

void print_list(struct my_list* list_pointer){
	struct lnode* tmp;
	tmp = list_pointer->head;
	//printf("Initiating printing.\n");
	while(tmp->key != list_pointer->current->key){
		print_node(tmp);
		tmp = tmp->next;
	}
	print_node(tmp);
}