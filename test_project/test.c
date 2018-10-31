#include "functions.h"

int main(int argc, char **argv){

	int N, buckets;
	relation **tables, **final_tables;
	int **hist, **psum;


	if(check_args(argc,argv,&N,&buckets) < 0)
		return -1;

	if(create_table(&tables,2) < 0)
		return -1;

	if(create_histograms(&hist,&psum,buckets,2) < 0)
		return -1;

	fill_hist(buckets,tables,hist,2);
	
	fill_psum(buckets,hist,psum,2);

	if(create_final_table(&final_tables,&tables,2,psum,buckets) < 0)
		return -1;
	
	free_memory(&tables,&final_tables,&hist,&psum,2);	
	return 0;
}