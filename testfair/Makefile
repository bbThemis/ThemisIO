default: all

EXECS = rw_speed simple test_fair_queue
OPT = -g
# OPT = -O3 -march=native
CC = gcc -Wall $(OPT)
CXX = g++ -Wall $(OPT)
MPICXX = mpicxx -Wall $(OPT)

all: $(EXECS)

simple: simple.c
	$(CC) $< -o $@

rw_speed: rw_speed.cpp
	$(MPICXX) $< -o $@

test_fair_queue: test_fair_queue.cpp ../include/fair_queue.h ../src/fair_queue.cpp
	$(CXX) -I../include test_fair_queue.cpp -o $@


clean:
	rm -f *.o *.exe $(EXECS)
