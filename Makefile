MAIN = main.o
OUTMAIN = main
HEADER = 
CC = gcc
FLAGS  = -g -c

all: $(OUTMAIN)

$(OUTMAIN): $(MAIN)
	$(CC) -g $(MAIN) -o $@

main.o: main.c
	$(CC) $(FLAGS) main.c

clean: 
	rm -f $(OUTMAIN) $(MAIN)