#include "storage.h"

int main(void)
{
	struct relation * myr;
	int x = count_lines("small.init");
	printf("lines are %d\n",x);
	struct table *t;
	t = create_table_new("small.init");
	for (int i=0; i<14; i++)
	{
		printf("table %ld %ld\n",t[i].tuples, t[i].columns);
	}
	printf("haha %d \n",t[1].my_relation[1].tuples[3].payload);
	free_table_new(t,x);
	string_parser("1.1 > 400000");
}
