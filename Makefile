OBJS = main.o functions.o
SOURCE = main.c functions.c
HEADER = structs.h
OUT = project
CC = gcc
FLAGS = -g -c

$(OUT): $(OBJS)
	$(CC) -g $(OBJS) -lm -o $@

main.o: main.c
	$(CC) $(FLAGS) main.c

functions.o: functions.c
	$(CC) $(FLAGS) functions.c -lm

clean:
	rm -rf $(OBJS) $(OUT)
