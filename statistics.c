#include "statistics.h"

struct statisticsRelation *createStatisticsRelations(struct table *myTable, int numRelations){
	struct statisticsRelation *sRelation;
	sRelation = malloc(sizeof(struct statisticsRelation)*numRelations);
	for (int i = 0; i < numRelations; ++i)
	{
		sRelation[i].columnsStatistics = malloc(sizeof(struct statistic)*(myTable[i].columns));
		for (int j = 0; j < myTable[i].columns; ++j)
		{
			sRelation[i].columnsStatistics[j] = *myTable[i].my_relation[j].statistics;
		}
		sRelation[i].numColumns = myTable[i].columns;
	}
	return sRelation;
}

int statisticsEqual(struct statisticsRelation *sRelation, int targetColID, int targetValue, struct table myTable){
	char flag = 0;
	int prevNumData = sRelation->columnsStatistics[targetColID].numData;
	sRelation->columnsStatistics[targetColID].min = targetValue;
	sRelation->columnsStatistics[targetColID].max = targetValue;
	for (int i = 0; i < myTable.tuples; ++i)
	{
		if (myTable.my_relation[targetColID].tuples[i].payload == targetValue)
		{
			if (sRelation->columnsStatistics[targetColID].numDiscreteData)
			{
				sRelation->columnsStatistics[targetColID].numData = sRelation->columnsStatistics[targetColID].numData / sRelation->columnsStatistics[targetColID].numDiscreteData;	
			}else{
				sRelation->columnsStatistics[targetColID].numData = 0;
			}
			sRelation->columnsStatistics[targetColID].numDiscreteData = 1;
			flag = 1;
			break;
		}
	}
	if (!flag)
	{
		sRelation->columnsStatistics[targetColID].numData = 0;
		sRelation->columnsStatistics[targetColID].numDiscreteData = 0;
	}

	for (int i = 0; i < sRelation->numColumns; ++i)
	{
		if (i != targetColID)
		{
			double x = 1 - sRelation->columnsStatistics[targetColID].numData / prevNumData;
			double x2 = pow(x, sRelation->columnsStatistics[i].numData / sRelation->columnsStatistics[i].numDiscreteData);
			x = 1 - x2;
			sRelation->columnsStatistics[i].numDiscreteData *= x;
			sRelation->columnsStatistics[i].numData = sRelation->columnsStatistics[targetColID].numData;
		}
	}
	return 1;
}

/*int statistsicsInequal(struct statisticsRelation *sRelation, int targetColID, int targetValue, int operationType, struct table myTable){
	int k1, k2;

	
}*/