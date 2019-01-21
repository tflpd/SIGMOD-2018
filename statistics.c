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
		sRelation[i].numRelations = numRelations;
	}
	return sRelation;
}

void freeStatistiscsRelations(struct statisticsRelation *sRelation){
	for (int i = 0; i < sRelation[0].numRelations; ++i)
	{
		free(sRelation[i].columnsStatistics);
	}
	free(sRelation);
}

int statisticsEqual(struct statisticsRelation *sRelation, int targetColID, int targetValue, struct table myTable){
	char flag = 0;
	int prevNumData = sRelation->columnsStatistics[targetColID].numData;
	if (prevNumData == 0)
	{
		return -1;
	}
	sRelation->columnsStatistics[targetColID].min = targetValue;
	sRelation->columnsStatistics[targetColID].max = targetValue;
	for (int i = 0; i < myTable.tuples; ++i)
	{
		if (myTable.my_relation[targetColID].tuples[i].payload == targetValue)
		{
			if (sRelation->columnsStatistics[targetColID].numDiscreteData)
			{
				if (sRelation->columnsStatistics[targetColID].numDiscreteData == 0)
				{
					return -1;
				}
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
			if (sRelation->columnsStatistics[i].numDiscreteData == 0)
			{
				return -1;
			}
			double x2 = pow(x, sRelation->columnsStatistics[i].numData / sRelation->columnsStatistics[i].numDiscreteData);
			x = 1 - x2;
			sRelation->columnsStatistics[i].numDiscreteData *= x;
			sRelation->columnsStatistics[i].numData = sRelation->columnsStatistics[targetColID].numData;
		}
	}
	return 1;
}

int statistsicsInequal(struct statisticsRelation *sRelation, int targetColID, int targetValue, int operationType){
	int k1, k2;
	int prevNumData = sRelation->columnsStatistics[targetColID].numData;
	if (prevNumData == 0)
	{
		return -1;
	}

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

	if ((sRelation->columnsStatistics[targetColID].max - sRelation->columnsStatistics[targetColID].min) == 0)
	{
		return -1;
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
			if (sRelation->columnsStatistics[i].numDiscreteData == 0)
			{
				return -1;
			}
			double x2 = pow(x, sRelation->columnsStatistics[i].numData / sRelation->columnsStatistics[i].numDiscreteData);
			x = 1 - x2;
			sRelation->columnsStatistics[i].numDiscreteData *= x;
			sRelation->columnsStatistics[i].numData = sRelation->columnsStatistics[targetColID].numData;
		}
	}
	return 1;
}

int statisticsSameRelationJoin(struct statisticsRelation *sRelation, int targetColIDA, int targetColIDB){
	int prevNumData = sRelation->columnsStatistics[targetColIDA].numData;
	if (prevNumData == 0)
	{
		return -1;
	}
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
			if (sRelation->columnsStatistics[i].numDiscreteData)
			{
				return -1;
			}
			double x2 = pow(x, sRelation->columnsStatistics[i].numData / sRelation->columnsStatistics[i].numDiscreteData);
			x = 1 - x2;
			sRelation->columnsStatistics[i].numDiscreteData *= x;
			sRelation->columnsStatistics[i].numData = sRelation->columnsStatistics[targetColIDA].numData;
		}
	}
	return sRelation->columnsStatistics[targetColIDA].numData;
}

int statisticsJoin(struct statisticsRelation *sRelationA, struct statisticsRelation *sRelationB, int targetColIDA, int targetColIDB){
	if (sRelationA->columnsStatistics[targetColIDA].min > sRelationB->columnsStatistics[targetColIDB].min)
	{
		sRelationB->columnsStatistics[targetColIDB].min = sRelationA->columnsStatistics[targetColIDA].min;
	}else{
		sRelationA->columnsStatistics[targetColIDA].min = sRelationB->columnsStatistics[targetColIDB].min;
	}

	if (sRelationA->columnsStatistics[targetColIDA].max < sRelationB->columnsStatistics[targetColIDB].max)
	{
		sRelationB->columnsStatistics[targetColIDB].max = sRelationA->columnsStatistics[targetColIDA].max;
	}else{
		sRelationA->columnsStatistics[targetColIDA].max = sRelationB->columnsStatistics[targetColIDB].max;
	}

	int n = sRelationA->columnsStatistics[targetColIDA].max - sRelationA->columnsStatistics[targetColIDA].min + 1;
	int newNumData = (sRelationA->columnsStatistics[targetColIDA].numData * sRelationB->columnsStatistics[targetColIDB].numData) / n;
	if (newNumData == 0)
	{
		return -1;
	}
	int newNumDiscrData = (sRelationA->columnsStatistics[targetColIDA].numDiscreteData * sRelationB->columnsStatistics[targetColIDB].numDiscreteData) / n;
	int prevNumDiscrDataA = sRelationA->columnsStatistics[targetColIDA].numDiscreteData;
	if (prevNumDiscrDataA == 0)
	{
		return -1;
	}
	int prevNumDiscrDataB = sRelationB->columnsStatistics[targetColIDB].numDiscreteData;
	if (prevNumDiscrDataB == 0)
	{
		return -1;
	}

	sRelationA->columnsStatistics[targetColIDA].numData = newNumData;
	sRelationB->columnsStatistics[targetColIDB].numData = newNumData;

	sRelationA->columnsStatistics[targetColIDA].numDiscreteData = newNumDiscrData;
	sRelationB->columnsStatistics[targetColIDB].numDiscreteData = newNumDiscrData;

	for (int i = 0; i < sRelationA->numColumns; ++i)
	{
		if (i != targetColIDA)
		{
			double x = 1 - sRelationA->columnsStatistics[targetColIDA].numDiscreteData / prevNumDiscrDataA;
			if (sRelationA->columnsStatistics[i].numDiscreteData == 0)
			{
				return -1;
			}
			double x2 = pow(x, sRelationA->columnsStatistics[i].numData / sRelationA->columnsStatistics[i].numDiscreteData);
			x = 1 - x2;
			sRelationA->columnsStatistics[i].numDiscreteData *= x;
			sRelationA->columnsStatistics[i].numData = sRelationA->columnsStatistics[targetColIDA].numData;
		}
	}

	for (int i = 0; i < sRelationB->numColumns; ++i)
	{
		if (i != targetColIDB)
		{
			double x = 1 - sRelationB->columnsStatistics[targetColIDB].numDiscreteData / prevNumDiscrDataB;
			if (sRelationB->columnsStatistics[i].numDiscreteData == 0)
			{
				return -1;
			}
			double x2 = pow(x, sRelationB->columnsStatistics[i].numData / sRelationB->columnsStatistics[i].numDiscreteData);
			x = 1 - x2;
			sRelationB->columnsStatistics[i].numDiscreteData *= x;
			sRelationB->columnsStatistics[i].numData = sRelationA->columnsStatistics[targetColIDA].numData;
		}
	}
	return newNumData;
}

int statisticsInnerJoin(struct statisticsRelation *sRelation, int targetColID){
	int n = sRelation->columnsStatistics[targetColID].max - sRelation->columnsStatistics[targetColID].min + 1;
	sRelation->columnsStatistics[targetColID].numData = sRelation->columnsStatistics[targetColID].numData * sRelation->columnsStatistics[targetColID].numData / n;

	for (int i = 0; i < sRelation->numColumns; ++i)
	{
		if (i != targetColID)
		{
			sRelation->columnsStatistics[i].numData = sRelation->columnsStatistics[targetColID].numData;
		}
	}
	return sRelation->columnsStatistics[targetColID].numData;
}

void copyStatsColumns(struct statistic *tempCol, struct statistic *sCol, int numColumns){
	for (int i = 0; i < numColumns; ++i)
	{
		tempCol[i].min = sCol[i].min;
		tempCol[i].max = sCol[i].max;
		tempCol[i].numData = sCol[i].numData;
		tempCol[i].numDiscreteData = sCol[i].numDiscreteData;
	}
}

void copyStatsRelations(struct statisticsRelation *tempStatsRelations, struct statisticsRelation *sRelations){
	for (int i = 0; i < sRelations[0].numRelations; ++i)
	{
		tempStatsRelations[i].columnsStatistics = malloc(sizeof(struct statistic)*sRelations[i].numColumns);
		copyStatsColumns(tempStatsRelations[i].columnsStatistics, sRelations[i].columnsStatistics, sRelations[i].numColumns);
		tempStatsRelations[i].numColumns = sRelations[i].numColumns;
		tempStatsRelations[i].numRelations = sRelations[i].numRelations;
	}
}

int connected(int virtualLastRelationID, int virtualNewRelationID, struct predicate *predicatesArray, int numPredicates){
	for (int i = 0; i < numPredicates; ++i)
	{
		if (predicatesArray[i].predicateType == JOIN)
		{
			if((predicatesArray[i].c1.virtualRelation == virtualLastRelationID) && (predicatesArray[i].c2.virtualRelation == virtualNewRelationID)){
				return 1;
			}
			if((predicatesArray[i].c1.virtualRelation == virtualNewRelationID) && (predicatesArray[i].c2.virtualRelation == virtualLastRelationID)){
				return 1;
			}
		}
	}
	return 0;
}

void reorderColumns(struct column **columnsToBeJoinedArray, struct column **reorderedColumnsToBeJoinedArray, int numColumns, struct statisticsRelation *sRelations, struct predicate *predicatesArray, int numPredicates){
	int *columnsCost;
	columnsCost = malloc(sizeof(int)*numColumns);
	for (int i = 0; i < numColumns; ++i)
	{
		columnsCost[i] = sRelations[columnsToBeJoinedArray[i]->table].columnsStatistics[columnsToBeJoinedArray[i]->column].numData;
	}
	int minCost = columnsCost[0];
	int minCostIndex = 0;
	for (int i = 0; i < numColumns; ++i)
	{
		if (columnsCost[i] < minCost)
		{
			minCost = columnsCost[i];
			minCostIndex = i;
		}
	}

	int reorderedArrayCounter = 0;
	reorderedColumnsToBeJoinedArray[reorderedArrayCounter] = columnsToBeJoinedArray[minCostIndex];
	columnsToBeJoinedArray[minCostIndex] = NULL;
	reorderedArrayCounter++;

	for (int i = 1; i < numColumns; ++i)
	{
		struct statisticsRelation **tempStatsRelArray;
		tempStatsRelArray = malloc(sizeof(struct statisticsRelation*)*(numColumns - i));
		for (int j = 0; j < numColumns - i; ++j)
		{
			tempStatsRelArray[j] = malloc(sizeof(struct statisticsRelation)*sRelations[0].numRelations);
			copyStatsRelations(tempStatsRelArray[j], sRelations);
		}

		int *costsArray;
		costsArray = malloc(sizeof(int)*(numColumns - i));

		int tempArrayCounter = 0;
		for (int j = 0; j < numColumns; ++j)
		{
			if (columnsToBeJoinedArray[j] != NULL)
			{
				int lastRelationID = reorderedColumnsToBeJoinedArray[i - 1]->table;
				int newRelationID = columnsToBeJoinedArray[j]->table;
				int virtualLastRelationID = reorderedColumnsToBeJoinedArray[i - 1]->virtualRelation;
				int virtualNewRelationID = columnsToBeJoinedArray[j]->virtualRelation;
				int lastColumnID = reorderedColumnsToBeJoinedArray[i - 1]->column;
				int newColumnID = columnsToBeJoinedArray[j]->column;
				if (connected(virtualLastRelationID, virtualNewRelationID, predicatesArray, numPredicates))
				{
					if (virtualLastRelationID == virtualNewRelationID)
					{
						if (lastColumnID == newColumnID)
						{
							costsArray[tempArrayCounter] = statisticsInnerJoin(&tempStatsRelArray[tempArrayCounter][lastRelationID], lastColumnID);
						}else{
							costsArray[tempArrayCounter] = statisticsSameRelationJoin(&tempStatsRelArray[tempArrayCounter][lastRelationID], lastColumnID, newColumnID);
						}
					}else{
						costsArray[tempArrayCounter] = statisticsJoin(&tempStatsRelArray[tempArrayCounter][lastRelationID], &tempStatsRelArray[tempArrayCounter][newRelationID], lastColumnID, newColumnID);
					}
					tempArrayCounter++;
				}
			}
		}

		minCost = costsArray[0];
		minCostIndex = 0;
		for (int j = 0; j < tempArrayCounter; ++j)
		{
			if (costsArray[j] < minCost)
			{
				minCost = costsArray[j];
				minCostIndex = j;
			}
		}

		tempArrayCounter = 0;
		for (int j = 0; j < numColumns; ++j)
		{
			if (columnsToBeJoinedArray[j] != NULL)
			{
				if (tempArrayCounter == minCostIndex)
				{
					freeStatistiscsRelations(sRelations);
					sRelations = tempStatsRelArray[tempArrayCounter];
					reorderedColumnsToBeJoinedArray[reorderedArrayCounter] = columnsToBeJoinedArray[j];
					reorderedArrayCounter++;
					columnsToBeJoinedArray[j] = NULL;
				}else{
					freeStatistiscsRelations(tempStatsRelArray[tempArrayCounter]);
				}
				tempArrayCounter++;
			}
		}
	}
}