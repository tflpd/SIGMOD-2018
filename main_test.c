#include "structs.h"
#include "storage.h"
#include "statistics.h"
#include "threadpool.h"
#include "jobs.h"

int main(void){
	struct table *relations_table;
	relations_table = create_table_new("small.init");

	threadpool *pool;
	pool = create_threadpool(10,11);
	PartitionedHist *p_histR = malloc(sizeof(PartitionedHist)*10);
	PartitionedHist *p_histS = malloc(sizeof(PartitionedHist)*10);

	HistJobArgs h_args;
	h_args.relationR = &relations_table[1].my_relation[0];
	h_args.relationS = &relations_table[2].my_relation[0];
	h_args.startR = 0;
	h_args.startS = 0;
	int tuplesR = relations_table[0].my_relation[0].num_tuples;
	int tuplesS = relations_table[1].my_relation[0].num_tuples;
	int parted_r = tuplesR / 10;
	int parted_s = tuplesS / 10;

	for(int i = 0; i < 10; i++){

		h_args.startS = i*parted_s;
		h_args.startR = i*parted_r;
		h_args.endR = parted_r;
		h_args.endS = parted_s;
		h_args.HistR = &p_histR[i];
		h_args.HistS = &p_histS[i];
		if(i = 9){
			int lastR = tuplesR - h_args.endR;
			int lastS = tuplesS - h_args.endS;
			h_args.endR = lastR;
			h_args.endS	= lastS;	
		}
		add_task(pool, &HistogramJob, (void *)&h_args, 0);
		h_args.endR += parted_r;
		h_args.endS +=parted_s;
	}
	for(int i=0; i<10; i++){
		for(int j=0; j<NUMBUCKETS; j++){
			//printf("hist %d position %d value %d\n",i, j, p_histR[i].arrayHist[j]);
		}
	}
	destroy_threadpool(pool ,0);

}