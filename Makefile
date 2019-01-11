OBJS = main_test.o functions.o myList.o storage.o statistics.o threadpool.o jobs.o
SOURCE = main_test.c functions.c myList.c storage.c statistics.c threadpool.c jobs.c
HEADER = structs.h myList.h	storage.h statistics.h threapool.h jobs.h
OUT = project
CC = gcc
FLAGS = -g -c

$(OUT): $(OBJS)
	$(CC) -g $(OBJS) -lm -pthread -o $@

main_test.o: main_test.c
	$(CC) $(FLAGS) main_test.c

functions.o: functions.c
	$(CC) $(FLAGS) functions.c -lm 

myList.o: myList.c
	$(CC) $(FLAGS) myList.c -lm 

storage.o: storage.c
	$(CC) $(FLAGS) storage.c -lm 

statistics.o: statistics.c
	$(CC) $(FLAGS) statistics.c -lm

threapool.o: threadpool.c
	$(CC) $(FLAGS) threadpool.c -lm -lpthread

jobs.o: jobs.c
	$(CC) $(FLAGS) jobs.c -lm

clean:
	rm -rf $(OBJS) $(OUT)
