#include "storage.h"

struct relation * store_data(char * my_file) {
    FILE * fp;
    u_int64_t relations, n_tuples;
    u_int64_t pload;
    //int32_t j;
    struct tuple * t;
    struct relation * r;
    int32_t tmpMin = -1;
    int32_t tmpMax = -1;
    int32_t range = 0;

    int32_t numDiscreteData = 0;

    fp = fopen(my_file, "r");

    fread( & n_tuples, sizeof(u_int64_t), 1, fp);
    //printf("PEKOS\n\t%ld\n",n_tuples);
    fread( & relations, sizeof(u_int64_t), 1, fp);
    //printf("PEKOS\n\t%ld\n",relations);
    r = malloc(sizeof(struct relation) * relations);
    //printf("\n\t%d\n",n_tuples);
    for (int i = 0; i < relations; i++) {
        r[i].tuples = malloc(sizeof(struct tuple) * n_tuples);
        r[i].num_tuples = n_tuples;
        r[i].statistics = malloc(sizeof(struct statistic));
        r[i].statistics->numData = n_tuples;
        for (int j = 0; j < n_tuples; j++) {
            fread( & pload, sizeof(u_int64_t), 1, fp);
            if (!j)
            {
            	tmpMin = pload;
            	tmpMax = pload;
            }

            if (pload < tmpMin)
            {
            	tmpMin = pload;
            }
            if (pload > tmpMax)
            {
            	tmpMax = pload;
            }
            //printf("\n\t%d\n",pload);
            r[i].tuples[j].key = j;
            r[i].tuples[j].payload = pload;
        }
        r[i].statistics->min = tmpMin;
        r[i].statistics->max = tmpMax;
        range = tmpMax - tmpMin + 1;
        char *boolDiscreteTable;
        if (range > N)
        {
        	range = N;
        	boolDiscreteTable = calloc(range, sizeof(char));
        	for (int j = 0; j < n_tuples; ++j)
        	{
        		if (!boolDiscreteTable[(r[i].tuples[j].payload - r[i].statistics->min) % N])
        		{
        			boolDiscreteTable[(r[i].tuples[j].payload - r[i].statistics->min) % N] = 1;
        			numDiscreteData++;	
        		}
        	}

        }else{
        	boolDiscreteTable = calloc(range, sizeof(char));
        	for (int j = 0; j < n_tuples; ++j)
        	{
        		if (!boolDiscreteTable[r[i].tuples[j].payload - r[i].statistics->min])
        		{
        			boolDiscreteTable[r[i].tuples[j].payload - r[i].statistics->min] = 1;
        			numDiscreteData++;	
        		}
        	}
        }
        r[i].statistics->numDiscreteData = numDiscreteData;
        free(boolDiscreteTable);
    }
    fclose(fp);
    return r;

}

int parse_workloads(char * name, struct table * new_table) {
    char temp[10];
    FILE * fp;
    FILE * fp_temp;
    int i = 0;
    u_int64_t tuples, column;
    fp = fopen(name, "r");
    while (fscanf(fp, "%10s", temp) == 1) {
        //printf("%d\n",i);
        fp_temp = fopen(temp, "r");
        fread( & new_table[i].tuples, sizeof(u_int64_t), 1, fp_temp);
        fread( & new_table[i].columns, sizeof(u_int64_t), 1, fp_temp);
        fclose(fp_temp);
        new_table[i].my_relation = store_data(temp);
        i++;
    }
    fclose(fp);
    return 1;
}

int count_lines(char * name) {
    FILE * fp;
    fp = fopen(name, "r");
    int ch = 0;
    int lines = 0;
    //0lines++;
    while ((ch = fgetc(fp)) != EOF) {
        if (ch == '\n')
            lines++;
    }
    fclose(fp);
    return lines;
}

struct table * create_table_new(char * name) {
    char table[10];
    int lines;
    FILE * fp;
    fp = fopen(name, "r");
    struct table * new_table;
    lines = count_lines(name);
    //fseek(fp, 0, SEEK_SET);
    new_table = malloc(sizeof(struct table) * lines);
    int x = parse_workloads(name, new_table);
    fclose(fp);
    return new_table;
}

void free_table_new(struct table * my_table, int lines) {
    //printf("greinn\n");
    //printf("my_t %d\n",my_table[1].columns);
    for (int i = 0; i < lines; i++) {
        //printf("i = %d",i);
        for (int j = 0; j < my_table[i].columns; j++) {
            free(my_table[i].my_relation[j].tuples);
        }
        free(my_table[i].my_relation);
    }
    free(my_table);
}

struct predicate *string_parser(struct query currQuery, struct middle_table * middle, struct table * relations_table, int query_size) {
    int middlesSize;
    int ch1 = '=';
    int ch2 = '<';
    int ch3 = '>';
    //int * type_of_predicate;
    int global_index;
    char * my_operator;
    char * test_operator;
    int n;
    char * temp;
    char * p_end;
    int index;
    //struct column c1, c2;
    //type_of_predicate = malloc(sizeof(int) * query_size);
    struct predicate *predicatesArray;
    predicatesArray = malloc(sizeof(struct predicate)*query_size);

    for (global_index = 0; global_index < query_size; global_index++) {
        middlesSize = currQuery.size2;
        size_t s = strlen(currQuery.filters[global_index]) + 1;
        char buf[s];
        p_end = currQuery.filters[global_index] + s;
        temp = currQuery.filters[global_index];
        index = 0;
        if ((my_operator = strchr(currQuery.filters[global_index], ch1)) != NULL) {
            int comparison_value;
            for (; currQuery.filters[global_index] < p_end && sscanf(currQuery.filters[global_index], "%[^.=]%n", & buf, & n); currQuery.filters[global_index] += (n + 1)) {
                int x;
                if (sscanf(buf, "%d", & x)) {
                    switch (index) {
                    case 0:
                        predicatesArray[global_index].c1.table = currQuery.table_indeces[x];
                        predicatesArray[global_index].c1.virtualRelation = x;
                    case 1:
                        predicatesArray[global_index].c1.column = x;
                    case 2:
                        comparison_value = x;
                    case 3:
                        predicatesArray[global_index].c2.column = x;
                    }
                }
                index++;
            }
            //printf("table 1| %d %d table 2| %d %d",c1.table,c1.column,c2.table,c2.column );
            if ((test_operator = strchr(my_operator, '.')) != NULL) {
                predicatesArray[global_index].c2.table = currQuery.table_indeces[comparison_value];
                predicatesArray[global_index].c2.virtualRelation = comparison_value;
                predicatesArray[global_index].predicateType = JOIN;
                //insert_to_middle(middle, relations_table, middlesSize, c1, c2);
            } else {
                //type_of_predicate[global_index] = EQUAL;
                predicatesArray[global_index].predicateType = EQUAL;
                predicatesArray[global_index].comparingValue = comparison_value;
                //insert_to_middle_predicate(middle, relations_table, middlesSize, c1, comparison_value, EQUAL);
            }
        } else if ((my_operator = strchr(currQuery.filters[global_index], ch2)) != NULL) {
            //printf("%s",my_operator);
            int comparison_value;
            for (; currQuery.filters[global_index] < p_end && sscanf(currQuery.filters[global_index], "%[^.<]%n", & buf, & n); currQuery.filters[global_index] += (n + 1)) {
                int x;
                if (sscanf(buf, "%d", & x)) {
                    switch (index) {
                    case 0:
                        predicatesArray[global_index].c1.table = currQuery.table_indeces[x];
                        predicatesArray[global_index].c1.virtualRelation = x;
                    case 1:
                        predicatesArray[global_index].c1.column = x;
                    case 2:
                        predicatesArray[global_index].comparingValue = x;
                    }
                }
                index++;
            }
            predicatesArray[global_index].predicateType = LESS;
            //insert_to_middle_predicate(middle, relations_table, middlesSize, c1, comparison_value, LESS);
            //printf("table 1| %d %d ",c1.table,c1.column);
        } else if ((my_operator = strchr(currQuery.filters[global_index], ch3)) != NULL) {
            int comparison_value;
            int flag = 0;
            //printf("%s",my_operator);
            for (; currQuery.filters[global_index] < p_end && sscanf(currQuery.filters[global_index], "%[^.>]%n", & buf, & n); currQuery.filters[global_index] += (n + 1)) {
                int x;

                if (sscanf(buf, "%d", & x)) {
                    switch (index) {
                    case 0:
                        predicatesArray[global_index].c1.table = currQuery.table_indeces[x];
                        predicatesArray[global_index].c1.virtualRelation = x;
                    case 1:
                        predicatesArray[global_index].c1.column = x;
                    case 2:
                        predicatesArray[global_index].comparingValue = x;
                    }
                }
                index++;
            }
            predicatesArray[global_index].predicateType = BIGGER;
            //insert_to_middle_predicate(middle, relations_table, middlesSize, c1, comparison_value, BIGGER);
            //printf("table 1| %d %d %d",c1.table,c1.column, temp);
        } else
            printf("fail");

    }

    return predicatesArray;
}
