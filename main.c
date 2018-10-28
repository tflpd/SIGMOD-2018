#include <stdio.h>
#include <stdlib.h>

//Considering n as 8 we ll use 2^10 records so we ll have around 3 records per one of hte 2^8 buckets
#define RECORDSNUM 1024
#define BUCKETSNUM 128
//#define N 8

int main(void)
{
	int32_t i;
	//Allocating and initializing the histogram array
	int **histogramArray = malloc(sizeof(int*) * BUCKETSNUM);
	for (i = 0; i < BUCKETSNUM; ++i)
	{
		histogramArray[i] = malloc(sizeof(int) * 2);
		histogramArray[i][0] = -1;
		histogramArray[i][1] = 0;
	}
	//Allocating and initializing the test input array
	int *testInputArray = malloc(sizeof(int) * RECORDSNUM);
	for (i = 0; i < RECORDSNUM; ++i)
	{
		testInputArray[i] = rand() % RECORDSNUM;
	}
	//Creating the histogram. Each row of it has the hash of the record on the left and the number of appearences on the right
	for (i = 0; i < RECORDSNUM; ++i)
	{
		//Getting the hash of the specif record
		int hash = testInputArray[i]%BUCKETSNUM;
		//If its the first time we meet that hash then initialize it
		if (histogramArray[hash][0] == -1)
		{
			histogramArray[hash][0] = hash;
		}
		//Add one more appearence one that hash
		histogramArray[hash][1] += 1;
	}
	//For the whole size of our histogram
	for (i = 0; i < BUCKETSNUM; ++i)
	{
		if (histogramArray[i][1] != 0)
		{
			//Print only the hashes that have at least one appearence and the number of their appearences
			printf("%d %d\n", histogramArray[i][0], histogramArray[i][1] );
		}
	}
	//Free the allocated memory
	free(testInputArray);
	for (i = 0; i < BUCKETSNUM; ++i)
	{
		free(histogramArray[i]);
	}
	free(histogramArray);


	return 0;
}
	