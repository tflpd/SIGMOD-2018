all: test_project

test_project: test.o functions.o
	gcc test.o functions.o -lm -o test_project

test.o: test.c
	gcc -c test.c

functions.o: functions.c
	gcc -c functions.c -lm

clean:
	rm -rf *.o test_project

