#ifndef JOBS_H
#define JOBS_H

#include "structs.h"
#include "jobscheduler.h"
#include "myList.h"

typedef struct PartitionedHist{
  int *arrayHist;
  int size;
}PartitionedHist;

/////////////////////////////////////////
//////////////Jobs Arguments/////////////
////////////////////////////////////////
typedef struct HistogramJobArgs{

	struct relation *relationR;
	struct relation *relationS;
	int *HistR;
	int *HistS;
	int startR;
	int endR;
	int startS;
	int endS;

}HistJobArgs;

typedef struct PartitionJobArgs{

	struct relation *relR;
	struct relation *reordered_R;
	struct relation *relS;
	struct relation *reordered_S;
	int *hist;
	int *psumR;
	int *psumS;
	int startR;//  start and end variables indicate the part of the relation each thread is responsible for
	int endR;// start is the index we begin e.g. relationR->tuples[startR] and end variable is where we finish
	int startS;//
	int endS;//
}PartitionArgs;

typedef struct JoinJobArgs{

	struct bucketIndex *index;
	int bucket_index;
	int *psumR;
	int *psumS;
	int *histR;
	int *histS;
	struct relation *reordered_R;
	struct relation *reordered_S;
	struct my_list *new_list;
	struct result *join_result;
}JoinArgs;

///////////////////////////////////////////////////////////////////////
/////////////////////////Job definitions//////////////////////////////
/////////////////////////////////////////////////////////////////////


void HistogramJob(void *);
void PartitionJob(void *);
void JoinJob(void *);



#endif
