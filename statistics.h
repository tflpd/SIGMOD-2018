#ifndef STATISTICS_H
#define STATISTICS_H

#include "structs.h"

struct table;

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
};

struct statisticsRelation *createStatisticsRelations(struct table *myTable, int numRelations);
int statisticsEqual(struct statisticsRelation *sRelation, int targetColID, int targetValue, struct table myTable);
int statistsicsInequal(struct statisticsRelation *sRelation, int targetColID, int targetValue, int operationType);
int statisticsSameRelationJoin(struct statisticsRelation *sRelation, int targetColIDA, int targetColIDB);
int statisticsJoin(struct statisticsRelation *sRelationA, struct statisticsRelation *sRelationB, int targetColIDA, int targetColIDB);

#endif