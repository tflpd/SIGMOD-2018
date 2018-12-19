#include "storage.h"

struct relation * store_data(char *my_file)
{
  FILE *fp;
  u_int64_t relations,n_tuples;
  u_int64_t pload;
  //int32_t j;
  struct tuple *t;
  struct relation *r;
  fp = fopen(my_file,"r");

  fread(&n_tuples, sizeof(u_int64_t) , 1,fp);
  //printf("\n\t%ld\n",n_tuples);
  fread(&relations,sizeof(u_int64_t),1,fp);
  //printf("\n\t%ld\n",relations);
  r = malloc(sizeof(struct relation)*relations);
  //printf("\n\t%d\n",n_tuples);
  for(int i=0; i<relations; i++)
  {
    r[i].tuples = malloc(sizeof(struct tuple)*n_tuples);
    r[i].num_tuples = n_tuples;
    for (int j=0; j<n_tuples; j++)
    {
      fread(&pload, sizeof(u_int64_t), 1, fp);
      //printf("\n\t%d\n",pload);
      r[i].tuples[j].key = j;
      r[i].tuples[j].payload = pload;
    }
  }
  fclose(fp);
  return r;

}

int parse_workloads(char * name, struct table *new_table)
{
  char temp[10];
  FILE *fp;
  FILE *fp_temp;
  int i =0;
  u_int64_t tuples,column;
  fp = fopen(name,"r");
  while(fscanf(fp,"%10s", temp) == 1)
  {
    //printf("%d\n",i);
    puts(temp);
    fp_temp = fopen(temp,"r");
    fread(&new_table[i].tuples, sizeof(u_int64_t), 1, fp_temp);
    fread(&new_table[i].columns, sizeof(u_int64_t), 1, fp_temp);
    fclose(fp_temp);
    new_table[i].my_relation = store_data(temp);
    i++;
  }
  fclose(fp);
  return 1;
}

int count_lines(char *name)
{
  FILE *fp;
  fp = fopen(name,"r");
  int ch=0;
  int lines=0;
  //0lines++;
  while ((ch = fgetc(fp)) != EOF)
    {
      if (ch == '\n')
    lines++;
    }
  fclose(fp);
  return lines;
}

struct table * create_table_new(char * name)
{
  char table[10];
  int lines;
  FILE *fp;
  fp = fopen(name, "r");
  struct table *new_table;
  lines = count_lines(name);
  //fseek(fp, 0, SEEK_SET);
  new_table = malloc(sizeof(struct table)*lines);
  int x = parse_workloads(name, new_table);
  fclose(fp);
  return new_table;
}

void free_table_new(struct table *my_table, int lines)
{
  //printf("greinn\n");
  //printf("my_t %d\n",my_table[1].columns);
  for(int i=0; i<lines; i++)
  {
      //printf("i = %d",i);
      for(int j=0; j<my_table[i].columns; j++)
      {
        free(my_table[i].my_relation[j].tuples);
      }
      free(my_table[i].my_relation);
  }
  free(my_table);
}

void string_parser(struct query currQuery, struct middle_table *middle, struct table *relations_table, int filterIndex)
{
  int middlesSize = currQuery.size2;
  char *query;
  query = malloc(sizeof(char)*(strlen(currQuery.filters[filterIndex]) + 1));
  strcpy(query, currQuery.filters[filterIndex]);
  char *temp = query;
  int ch1 = '=';
  int ch2 = '<';
  int ch3 = '>';
  char *my_operator;
  size_t s = strlen(query)+1;
  char buf[s];
  const char * p_end = query+s;
  int n;
  int index =0;
  struct column c1,c2;

  if((my_operator = strchr(query,ch1)) != NULL)
    {
      int comparison_value;
      for(; query<p_end && sscanf(query, "%[^.=]%n", &buf, &n); query += (n+1))
      {
        int x;
        if(sscanf(buf, "%d", &x))
        {
          switch(index)
          {
            case 0:
              c1.table = currQuery.table_indeces[x];
            case 1:
              c1.column = x;
            case 2:
              comparison_value = x;
            case 3:
              c2.column = x;
          }
        }
        index ++;
      }
      //printf("table 1| %d %d table 2| %d %d",c1.table,c1.column,c2.table,c2.column );
      if(comparison_value > 50)
        insert_to_middle_predicate(middle, relations_table, middlesSize, c1.table, c1.column, comparison_value, EQUAL);
      else
      {
        c2.table = currQuery.table_indeces[comparison_value];
        insert_to_middle(middle, relations_table, middlesSize, c1.table, c2.table, c1.column, c2.column);
      }
    }

  else if ((my_operator = strchr(query,ch2)) != NULL)
  {
    //printf("%s",my_operator);
    int comparison_value;
    for(; query<p_end && sscanf(query, "%[^.<]%n", &buf, &n); query += (n+1))
    {
      int x;
      if(sscanf(buf, "%d", &x))
      {
        switch(index)
        {
          case 0:
            c1.table = currQuery.table_indeces[x];;
          case 1:
            c1.column = x;
          case 2:
            comparison_value = 0;
        }
      }
      index ++;
    }
    insert_to_middle_predicate(middle, relations_table, middlesSize, c1.table, c1.column, comparison_value, LESS);
    //printf("table 1| %d %d ",c1.table,c1.column);
  }

  else if((my_operator = strchr(query,ch3)) != NULL)
  {
    int comparison_value;
    int flag =0;
    //printf("%s",my_operator);
    for(; query<p_end && sscanf(query, "%[^.>]%n", &buf, &n); query += (n+1))
    {
      int x;

      if(sscanf(buf, "%d", &x))
      {
        switch(index)
        {
          case 0:
            c1.table = currQuery.table_indeces[x];;
          case 1:
            c1.column = x;
          case 2:
            comparison_value = x;
        }
      }
      index ++;
    }
    insert_to_middle_predicate(middle, relations_table, middlesSize, c1.table, c1.column, comparison_value, BIGGER);
    //printf("table 1| %d %d %d",c1.table,c1.column, temp);
  }

  else
      printf("fail");

  free(query);
}
