#include "structs.h"
#include "storage.h"

int main(int argc, char **argv){

	/* Variables for the getline() function that will be used
	   to get input from the user. */
	char *input = NULL; // The input that the user provides
	size_t n = 0;
	struct table *relations_table;


	/* EDW PERNEIS DATA */
	relations_table = create_table_new("small.init");

	print_welcome_msg(1);

	/* Keep getting input from the user, until he/she decides to exit
	   the program */
	while(getline(&input,&n,stdin) != -1){

		// The user wants to retrieve certain data
		if(strncmp(input,"Q",strlen("Q")) == 0){

			printf("Enter the query/-ies file name: ");
			char *queriesFileName = NULL;
			size_t qsize = 0;
			FILE *fin = NULL;
			if(getline(&queriesFileName,&qsize,stdin) != -1){
				printf("File to be executed:%s\n", queriesFileName);
				fin = fopen(strtok(queriesFileName,"\n"),"r");
				if (fin == NULL)
				{
					perror("Queries file opening failed:");
					return -1;
				}
			}

			/* Variables for the following getline() function.
			   They are initialized with NULL and 0 respectively,
			   so that getline() can allocate the appropriate ammount
			   of memory. */
			char *query = NULL;	// The actual query the user gives
			size_t n1 = 0;

			/* We'll keep accepting queries from the user until he/she types
			   the letter 'F'.
			   Every query consists of three parts and has the following form:

			   0 2 4|0.1=1.2&1.0=2.1&0.1>3000|0.0 1.1 */

			int numQueries = 0;

			struct batch* my_batch;
			my_batch = malloc(sizeof(struct batch));
			my_batch->queries = NULL;
			my_batch->numQueries = 0;

			while(getline(&query,&n1,fin) != -1){

				// The user wants to stop providing us with queries
				if(strncmp(query,"F",strlen("F")) == 0){
					printf("We're done with accepting queries.\n");
					break;
				}else{

					/* We must split the given query into its three parts.
					   Since we'll be using the strtok() function, we must
					   make a copy of the provided query */
					char *query_copy;
					query_copy = (char *)malloc(strlen(query)*sizeof(char));
					if(query_copy == NULL){
						perror("Memory allocation failed: ");
						return -1;
					}
					memset((char *)query_copy,0,strlen(query));
					strncpy(query_copy,query,strlen(query)-1);

					/* We'll store each part individually.
					   e.g: parts -> parts[0] -> "0 2 4"
					                 parts[1] -> "0.1=1.2&1.0=2.1&0.1>3000"
					                 parts[2] -> "0.0 1.1" */
					char **parts;
					parts = (char **)malloc(3*sizeof(char *));
					if(parts == NULL){
						perror("Memory allocation failed: ");
						return -1;
					}

					/* Finding the three parts of the query, using strtok()
					   and the "|" character as the delimiter */
					for(int i = 0; i < 3; i++){

						if(i == 0){
							parts[i] = strtok(query_copy,"|");
						}
						else
							parts[i] = strtok(NULL,"|");
					}

					/*****************************************/
					/*** Storing the elements of each part ***/
					/*****************************************/

					/* (*) Each one of the following can (and should) be put
					       in an individual function */

					char *tmp; // Temporary pointer used with strtok()

					/* We'll store the IDs of the tables that participate
                       in the JOIN statement as integers.
                                             -----
                       e.g: table_indeces -> | 0 |
                       						 -----
                       						 | 2 |
                       						 -----
                       						 | 4 |
                       						 -----                            */
					int *table_indeces; // The array where we'll store the IDs
					int size = 0;  // The size of the table_indeces array

					// Getting the IDs of the tables
					while(1){

						// Getting the ID of the first table
						if(!size){
							tmp = strtok(parts[0]," ");

							if(tmp == NULL)
								break;

							// i.e size = 1
							size++;

							table_indeces = (int *)malloc(size*sizeof(int));
							if(table_indeces == NULL){
								perror("Memory allocation failed: ");
								return -1;
							}
							table_indeces[size-1] = atoi(tmp);
						}

						// Getting the ID of the remaining tables
						else{

							tmp = strtok(NULL," ");

							if(tmp == NULL)
								break;

							// i.e size = 1, 2, ...
							size++;

							table_indeces = (int *)realloc((int *)table_indeces,
								size*sizeof(int));

							if(table_indeces == NULL){
								perror("Memory reallocation failed: ");
								return -1;
							}

							table_indeces[size-1] = atoi(tmp);
						}
					}


					/* We'll store each one of the filters as an individual
					   string.

					   e.g filters -> filters[0] -> "0.1=1.2"
					                  filters[1] -> "1.0=2.1"
					                  filters[2] -> "0.1>3000"                */
					char **filters;
					int size_1 = 0;

					while(1){

						if(!size_1){

							tmp = strtok(parts[1],"&");

							if(tmp == NULL)
								break;

							// i.e size_1 = 1
							size_1++;

							filters = (char **)malloc(size_1*sizeof(char *));
							if(filters == NULL){
								perror("Memory allocation failed: ");
								return -1;
							}

							filters[size_1-1] = (char *)malloc((strlen(tmp)+1)*
								sizeof(char));
							if(filters[size_1-1] == NULL){
								perror("Memory allocation failed: ");
								return -1;
							}

							strcpy(filters[size_1-1],tmp);
						}

						else{

							tmp = strtok(NULL,"&");

							if(tmp == NULL)
								break;

							// i.e size_1 = 2, 3, ...
							size_1++;

							filters = (char **)realloc((char **)filters,
								size_1*sizeof(char *));
							if(filters == NULL){
								printf("Memory reallocation failed: \n");
								return -1;
							}

							filters[size_1-1] = (char *)malloc((strlen(tmp)+1)*
								sizeof(char));
							if(filters[size_1-1] == NULL){
								perror("Memory allocation failed: ");
								return -1;
							}

							strcpy(filters[size_1-1],tmp);
						}
					}

					/* In order to store the ID of the table along with one of
					   its columns as integers, we have to split the original
					   string first.

					   e.g: part3 -> part3[0] -> "0.0"
					                 part3[1] -> "1.1"                        */
					char **part3;
					int size_2 = 0;

					while(1){

						if(!size_2){

							tmp = strtok(parts[2]," ");
							if(tmp == NULL)
								break;

							// i.e: size_2 = 1
							size_2++;

							part3 = (char **)malloc(size_2*sizeof(char *));
							if(part3 == NULL){
								perror("Memory allocation failed: ");
								return -1;
							}

							part3[size_2-1] = (char *)malloc((strlen(tmp)+1)*
								sizeof(char));
							if(part3[size_2-1] == NULL){
								perror("Memory allocation failed: ");
								return -1;
							}
							memset((char *)part3[size_2-1],0,strlen(tmp)+1);
							strcpy(part3[size_2-1],tmp);
						}

						else{

							tmp = strtok(NULL," ");
							if(tmp == NULL)
								break;

							// i.e: size_2 = 2, 3, ...
							size_2++;

							part3 = (char **)realloc((char **)part3,size_2*
								sizeof(char *));
							if(part3 == NULL){
								perror("Memory reallocation failed: ");
								return -1;
							}

							part3[size_2-1] = (char *)malloc((strlen(tmp)+1)*
								sizeof(char));
							if(part3[size_2-1] == NULL){
								perror("Memory allocation failed: ");
								return -1;
							}
							memset((char *)part3[size_2-1],0,strlen(tmp)+1);
							strcpy(part3[size_2-1],tmp);
						}
					}


					/* Now we can store both the table and its column as
					   integers.
					   						   table  column
											 -----------------
					   e.g: sum -> sum[0] -> |   0   |   0   |
					   						 -----------------
					   			   sum[1] -> |   1   |   1   |
					   			   			 -----------------                */
					int **sum;
					sum = (int **)malloc(size_2*sizeof(int *));
					if(sum == NULL){
						perror("Memory allocation failed: ");
						return -1;
					}

					for(int i = 0; i < size_2; i++){

						sum[i] = (int *)malloc(2*sizeof(int));
						if(sum[i] == NULL){
							perror("Memory allocation failed: ");
							return -1;
						}

						tmp = strtok(part3[i],".");
						sum[i][0] = atoi(tmp);

						tmp = strtok(NULL,".");
						sum[i][1] = atoi(tmp);
					}

					/* Printing the results to ensure that everything
					   works fine */
					// printf("You've requested the following:\n");
					// for(int i = 0; i < size; i++)
					// 	printf("table[%d]: %d, ",i,table_indeces[i]);

					// printf("\n");
					// for(int i = 0; i < size_1; i++)
					// 	printf("filters[%d]: %s, ",i,filters[i]);

					// printf("\n");
					// for(int i = 0; i < size_2; i++){
					// 	printf("sum[%d][0]: %d, ",i,sum[i][0]);
					// 	printf("sum[%d][1]: %d\n",i,sum[i][1]);
					// }

					// Merge the information into query struct
					struct query* my_query;
					my_query = create_query(table_indeces, size, filters, size_1, sum, size_2);
					printQuery(*my_query);
					// Add that struct into the current batch
					my_batch->numQueries++;
					my_batch->queries = realloc(my_batch->queries, sizeof(struct query)*(my_batch->numQueries));
					if(my_batch->queries == NULL){
						perror("Memory reallocation failed: ");
						return -1;
					}
					my_batch->queries[my_batch->numQueries - 1] = *my_query;


					// Freeing every piece of memory that we allocated.
					for(int i = 0; i < size_1; i++)
						free(filters[i]);

					for(int i = 0; i < size_2; i++)	{
						free(part3[i]);
						free(sum[i]);
					}

					free(sum);
					free(part3);
					free(filters);
					free(table_indeces);
					free(parts);
					free(query_copy);
					numQueries++;
				}
			}
			/* EDW STELNW BATCH GIA EKTELESI*/
			executeBatch(my_batch, relations_table);

			// Free the queries information of that batch
			/*for (int i = 0; i < my_batch; ++i)
			{
				
			}*/

			printf("Executed %d queries.\n", numQueries);
			fclose(fin);
			free(query);
			free(queriesFileName);
			print_welcome_msg(0);
		}

		// The user wants to exit the program
		else if(strncmp(input,"Exit",strlen("Exit")) == 0){
			printf("Bye!\n");
			break;
		}

		else{
			printf("Wrong input. Try again.\n");
			print_welcome_msg(0);
		}
	}





	// Getting input from the user
	//if(get_user_input(&input,&n) < 0)
	//	return -1;



	// Releasing memory
	// free_memory(&hist,&psum,2,&testInputArray,&finalArray,&my_array,buckets,
	// 	&input);
	return 0;
}
