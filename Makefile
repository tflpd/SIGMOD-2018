OBJS = main.o functions.o myList.o storage.o
SOURCE = main.c functions.c myList.c storage.c
HEADER = structs.h myList.h	storage.h
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

storage.o: storage.c
	$(CC) $(FLAGS) storage.c -lm

clean:
	rm -rf $(OBJS) $(OUT)
