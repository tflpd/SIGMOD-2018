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
  printf("\n\t%ld\n",n_tuples);
  fread(&relations,sizeof(u_int64_t),1,fp);
  printf("\n\t%ld\n",relations);
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
    printf("%d\n",i);
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
  printf("greinn\n");
  printf("my_t %d\n",my_table[1].columns);
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

void string_parser(char *query)
{
  char *temp = query;
  int ch1 = '=';
  int ch2 = '<';
  int ch3 = '>';
  char *my_operator;

  if((my_operator = strchr(query,ch1)) != NULL)
    {
      //printf("%s",my_operator);
      int temp[4];
      char *p =query;
      int count,n,sum;
      struct column column1,column2;
      for (count = 0; query[count] != '\0'; count++)

      {

          if ((query[count] >= '0') && (query[count] <= '9'))

          {

              n += 1;

              sum += (query[count] - '0');

          }

      }
      printf("%d \n",sum);
    }
  else if ((my_operator = strchr(query,ch2)) != NULL)
      printf("%s",my_operator);
  else if((my_operator = strchr(query,ch3)) != NULL)
      printf("%s",my_operator);
  else
      printf("fail");
}
