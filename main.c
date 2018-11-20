#include "structs.h"

#define RECORDSNUM 100//1024

int main(int argc, char **argv){

	int N;			// The exponent
	int buckets;	// The total number of buckets (=2^N)
	
	int **hist;		// Pointer to the hist arrays
	int **psum;		// Pointer to the psum arrays

	struct relation **testInputArray;
	struct relation **finalArray;

	struct index_array *my_array;

	/*THEWRITIKI KLISI LISTAS
	na ginei include arxika to arxio
	struct my_list* list;
	list = list_init(ARITHMOS TUPLE ANA NODE SE ARITMISI APO 0);
	list = add_to_buff(list, NEO TUPLE POY THELW NA PROSTETHEI);
	print_list(list);
	delete_list(list);
	*/

	if(check_args(argc,argv,&buckets,&N) < 0)
		return -1;

	if(allocate_histograms(&hist,&psum,2,buckets) < 0)
		return -1;

	if(allocate_tables(&testInputArray,RECORDSNUM,2) < 0)
		return -1;

	for(int i = 0; i < 2; i++)
		printf("Table %d has %d records\n",i,testInputArray[i]->num_tuples);
	printf("\n");
	print_tables(testInputArray,2);
	
	if(allocate_final_tables(&finalArray,testInputArray,2) < 0)
		return -1;
	print_tables(finalArray,2);

	fill_histograms(testInputArray,hist,psum,buckets,2);
	print_hist(hist,2,buckets);
	print_psum(psum,2,buckets);

	rearrange_tables(2,testInputArray, finalArray,psum,buckets);
	print_tables(finalArray,2);

	if(allocate_index_array(&my_array,buckets,2,hist) < 0)
		return -1;
	print_index_array(buckets,my_array);

	fill_indeces(my_array,buckets,psum,finalArray);
	print_index_array(buckets,my_array);

	free_memory(&hist,&psum,2,&testInputArray,&finalArray,&my_array,buckets);
	return 0;
}	