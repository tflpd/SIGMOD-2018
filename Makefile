OBJS = main.o functions.o myList.o storage.o statistics.o jobscheduler.o jobs.o
SOURCE = main.c functions.c myList.c storage.c statistics.c jobscheduler.c jobs.c
HEADER = structs.h myList.h	storage.h statistics.h jobscheduler.h jobs.h
OUT = project
CC = gcc
FLAGS = -g -c

$(OUT): $(OBJS)
	$(CC) -g $(OBJS) -lm -pthread -o $@

main.o: main.c
	$(CC) $(FLAGS) main.c

functions.o: functions.c
	$(CC) $(FLAGS) functions.c -lm

myList.o: myList.c
	$(CC) $(FLAGS) myList.c -lm

storage.o: storage.c
	$(CC) $(FLAGS) storage.c -lm

statistics.o: statistics.c
	$(CC) $(FLAGS) statistics.c -lm

threapool.o: jobscheduler.c
	$(CC) $(FLAGS) jobscheduler.c -lm -lpthread

jobs.o: jobs.c
	$(CC) $(FLAGS) jobs.c -lm

clean:
	rm -rf $(OBJS) $(OUT)
