#include "structs.h"

/* Considering n as 8 we ll use 2^10 records so we ll have around 3 records per 
one of hte 2^8 buckets */
#define RECORDSNUM 1024
// #define BUCKETSNUM 256 // Should be given as an argument, since it can change

int main(int argc, char **argv)
{	
	int N;
	int buckets;
	int **histogramArray, **accumulativeHistogramArray;
	struct relation *testInputArray;
	struct relation *finalArray;
		
	if(check_args(argc,argv,&buckets,&N) < 0)
		return -1;

	// Allocating and initializing both histograms
	if((create_hist(&histogramArray,buckets) < 0) || 
		(create_psum(&accumulativeHistogramArray,buckets) < 0))
		return -1;

	//Allocating and initializing the test input array
	if(create_table(&testInputArray,RECORDSNUM) < 0)
		return -1;
	// print_table(testInputArray);

	// Allocating the re-ordered final array
	finalArray = malloc(sizeof(relation));
	finalArray->tuples = malloc(sizeof(tuple) * RECORDSNUM); // to kanw me struct relation , pinaka apo tuples
	
	/* Creating the histogram. Each row of it has the hash of the bucket on the 
	   left and the number of appearences on the right */
	fill_hist(RECORDSNUM,buckets,testInputArray,histogramArray);
	// print_hist(histogramArray,buckets);

	/* Creating the accumulative histogram. Each row has the hash of the bucket 
	   on the left and the base of the bucket on the right */
	fill_psum(histogramArray,accumulativeHistogramArray,buckets);
	// print_psum(accumulativeHistogramArray,buckets);

	//Creating the reordered array
	create_reordered(testInputArray,finalArray,accumulativeHistogramArray,buckets);
	// print_table(finalArray);

	//Freeing the allocated memory
	free_memory(testInputArray,finalArray,buckets,histogramArray,
		accumulativeHistogramArray);

	return 0;
}