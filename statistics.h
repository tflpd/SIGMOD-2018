#ifndef STATISTICS_H
#define STATISTICS_H

#include "structs.h"

struct table;
struct column;
struct predicate;

struct statistic
{
  int32_t min;
  int32_t max;
  int32_t numData;
  int32_t numDiscreteData;
};

struct statisticsRelation
{
	struct statistic *columnsStatistics;
	int numColumns;
	int numRelations;
};

struct statisticsRelation *createStatisticsRelations(struct table *myTable, int numRelations);
int statisticsEqual(struct statisticsRelation *sRelation, int targetColID, int targetValue, struct table myTable);
int statistsicsInequal(struct statisticsRelation *sRelation, int targetColID, int targetValue, int operationType);
int statisticsSameRelationJoin(struct statisticsRelation *sRelation, int targetColIDA, int targetColIDB);
int statisticsJoin(struct statisticsRelation *sRelationA, struct statisticsRelation *sRelationB, int targetColIDA, int targetColIDB);
void freeStatistiscsRelations(struct statisticsRelation *sRelation);
void copyStatsColumns(struct statistic *tempCol, struct statistic *sCol, int numColumns);
void copyStatsRelations(struct statisticsRelation *tempStatsRelations, struct statisticsRelation *sRelations);
void reorderColumns(struct column **columnsToBeJoinedArray, struct column **reorderedColumnsToBeJoinedArray, int numColumns, struct statisticsRelation *sRelations, struct predicate *predicatesArray, int numPredicates);
int connected(int virtualLastRelationID, int virtualNewRelationID, struct predicate *predicatesArray, int numPredicates);

#endif