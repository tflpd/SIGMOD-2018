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

int statistsicsInequal(struct statisticsRelation *sRelation, int targetColID, int targetValue, int operationType, struct table myTable){
	int k1, k2;
	int prevNumData = sRelation->columnsStatistics[targetColID].numData;

	if (operationType == LESS)
	{
		k1 = sRelation->columnsStatistics[targetColID].min;
		k2 = targetValue;
	}
	if (operationType == BIGGER)
	{
		k1 = targetValue;
		k2 = sRelation->columnsStatistics[targetColID].max;
	}
	if (k2 > sRelation->columnsStatistics[targetColID].max)
	{
		k2 = sRelation->columnsStatistics[targetColID].max;
	}
	if (k1 < sRelation->columnsStatistics[targetColID].min)
	{
		k1 = sRelation->columnsStatistics[targetColID].min;
	}

	
	sRelation->columnsStatistics[targetColID].numDiscreteData *= (k2 - k1) / (sRelation->columnsStatistics[targetColID].max - sRelation->columnsStatistics[targetColID].min);
	sRelation->columnsStatistics[targetColID].numData *= (k2 - k1) / (sRelation->columnsStatistics[targetColID].max - sRelation->columnsStatistics[targetColID].min);
	sRelation->columnsStatistics[targetColID].min = k1;
	sRelation->columnsStatistics[targetColID].max = k2;

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

int statisticsSameRelationJoin(struct statisticsRelation *sRelation, int targetColIDA, int targetColIDB, struct table myTable){
	int prevNumData = sRelation->columnsStatistics[targetColIDA].numData;
	if (sRelation->columnsStatistics[targetColIDA].min > sRelation->columnsStatistics[targetColIDB].min)
	{
		sRelation->columnsStatistics[targetColIDB].min = sRelation->columnsStatistics[targetColIDA].min;
	}else{
		sRelation->columnsStatistics[targetColIDA].min = sRelation->columnsStatistics[targetColIDB].min;
	}

	if (sRelation->columnsStatistics[targetColIDA].max < sRelation->columnsStatistics[targetColIDB].max)
	{
		sRelation->columnsStatistics[targetColIDB].max = sRelation->columnsStatistics[targetColIDA].max;
	}else{
		sRelation->columnsStatistics[targetColIDA].max = sRelation->columnsStatistics[targetColIDB].max;
	}

	int n = sRelation->columnsStatistics[targetColIDA].max - sRelation->columnsStatistics[targetColIDA].min + 1;
	sRelation->columnsStatistics[targetColIDA].numData /= n;
	sRelation->columnsStatistics[targetColIDB].numData = sRelation->columnsStatistics[targetColIDA].numData;
	/*EPISIS NA KOITAKSW TIS DIERESEIS ME 0*/

	double x = 1 - sRelation->columnsStatistics[targetColIDA].numData / prevNumData;
	double x2 = pow(x, prevNumData / sRelation->columnsStatistics[targetColIDA].numDiscreteData);
	x = 1 - x2;
	sRelation->columnsStatistics[targetColIDA].numDiscreteData *= x;

	sRelation->columnsStatistics[targetColIDB].numDiscreteData = sRelation->columnsStatistics[targetColIDA].numDiscreteData;

	for (int i = 0; i < sRelation->numColumns; ++i)
	{
		if ((i != targetColIDA) && (i != targetColIDB))
		{
			double x = 1 - sRelation->columnsStatistics[targetColIDA].numData / prevNumData;
			double x2 = pow(x, sRelation->columnsStatistics[i].numData / sRelation->columnsStatistics[i].numDiscreteData);
			x = 1 - x2;
			sRelation->columnsStatistics[i].numDiscreteData *= x;
			sRelation->columnsStatistics[i].numData = sRelation->columnsStatistics[targetColIDA].numData;
		}
	}

}

