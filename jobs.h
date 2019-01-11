#ifndef JOBS_H
#define JOBS_H

#include "structs.h"
#include "threadpool.h"
#include "myList.h"

/////////////////////////////////////////
//////////////Jobs Arguments/////////////
////////////////////////////////////////
typedef struct HistogramJobArgs{
	
	struct relation *relationR;
	struct relation *relationS; 
	PartitionedHist *HistR;
	PartitionedHist *HistS;
	int startR;
	int endR;
	int startS;
	int endS;
	
}HistJobArgs;

typedef struct PartitionJobArgs{

	struct relation *relR;
	struct relation *reordered_R;
	int *hist;
	int *psum;
	int *copy_psum;
	int start;
	int end;
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
}JoinArgs;
///////////////////////////////////////////////////////////////////////
/////////////////////////Job definitions//////////////////////////////
/////////////////////////////////////////////////////////////////////


void HistogramJob(void *);
void *PartitionJob(void *);
void *JoinJob(void *);

//////////////////////////////////////////////////////////////////////////////////
///////////These are helper functions not to be executed by threads///////////////
//////////////////////////////////////////////////////////////////////////////////

int *join_partitioned_hists(PartitionedHist *, int);

#endif