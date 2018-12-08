OBJS = main.o functions.o myList.o
SOURCE = main.c functions.c myList.c
HEADER = structs.h myList.h
OUT = project
CC = gcc
FLAGS = -g -c

$(OUT): $(OBJS)
	$(CC) -g $(OBJS) -lm -o $@

main.o: main.c
	$(CC) $(FLAGS) main.c

functions.o: functions.c
	$(CC) $(FLAGS) functions.c -lm

myList.o: myList.c
	$(CC) $(FLAGS) myList.c -lm

clean:
	rm -rf $(OBJS) $(OUT)
