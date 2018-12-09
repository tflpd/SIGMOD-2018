#include "structs.h"

#define RECORDSNUM 10//1024

int main(int argc, char **argv){

	int N;			// The exponent
	int buckets;	// The total number of buckets (=2^N)
	
	int **hist;		// Pointer to the hist arrays
	int **psum;		// Pointer to the psum arrays

	struct relation **testInputArray;
	struct relation **finalArray;

	struct index_array *my_array;

	struct result *results_list = NULL;

	/* Variables for the getline() function that will be used 
	   to get input from the user. */
	char *input = NULL; // The input that the user provides
	size_t n = 0;

	if(check_args(argc,argv,&buckets,&N) < 0)
		return -1;

	if(allocate_histograms(&hist,&psum,2,buckets) < 0)
		return -1;

	if(allocate_tables(&testInputArray,RECORDSNUM,2) < 0)
		return -1;

	print_records_no(2,testInputArray);
	print_tables(testInputArray,2);
	
	if(allocate_final_tables(&finalArray,testInputArray,2) < 0)
		return -1;
	// print_tables(finalArray,2);

	fill_histograms(testInputArray,hist,psum,buckets,2);
	// print_hist(hist,2,buckets);
	// print_psum(psum,2,buckets);

	rearrange_tables(2,testInputArray, finalArray,psum,buckets);
	print_tables(finalArray,2);

	if(allocate_index_array(&my_array,buckets,2,hist) < 0)
		return -1;
	// print_index_array(buckets,my_array);

	fill_indeces(my_array,buckets,psum,finalArray);
	print_index_array(buckets,my_array);
	
	// Getting input from the user
	//if(get_user_input(&input,&n) < 0)
	//	return -1;

	// Releasing memory
	free_memory(&hist,&psum,2,&testInputArray,&finalArray,&my_array,buckets,
		&input);
	return 0;
}	