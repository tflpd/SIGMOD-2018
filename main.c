#include "structs.h"

/* Considering n as 8 we ll use 2^10 records so we ll have around 3 records per 
one of hte 2^8 buckets */
#define RECORDSNUM 1024
// #define BUCKETSNUM 256 // Should be given as an argument, since it can change

int main(int argc, char **argv)
{	
	int N;
	int buckets;
	int ***histogramArray;
	int ***accumulativeHistogramArray;
	
	struct relation **testInputArray;
	struct relation **finalArray;
		
	if(check_args(argc,argv,&buckets,&N) < 0)
		return -1;
	
	// Allocating and initializing both histograms, 
	// two (psum and hist) for each table
	if(create_histograms(&histogramArray,&accumulativeHistogramArray,
		buckets,2) < 0)
		return -1;
	// print_hist(histogramArray,buckets,2);
	// print_psum(accumulativeHistogramArray,buckets,2);
		
	//Allocating and initializing the test input array
	if(create_table(&testInputArray,RECORDSNUM, 2) < 0)
		return -1;
	// print_table(testInputArray,2);

	// Allocating the re-ordered final array
	if(create_final_table(&finalArray,testInputArray,2) < 0)
		return -1;
	// print_table(finalArray,2);

	fill_histograms(testInputArray,histogramArray,accumulativeHistogramArray,2,
		buckets);
	// print_hist(histogramArray,buckets,2);
	// print_psum(accumulativeHistogramArray,buckets,2);

	//Creating the reordered array
	rearrange_table(testInputArray,finalArray,accumulativeHistogramArray,
		buckets,2);
	// print_table(finalArray,2);

	//Freeing the allocated memory
	free_memory(testInputArray,finalArray,buckets,&histogramArray[0],
		&accumulativeHistogramArray[0],2);

	return 0;
}