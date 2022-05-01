default: all

EXECS = rw_speed simple mmap_test
# OPT = -g
OPT = -O3 -march=native
CC = gcc -Wall $(OPT)
CXX = gcc -Wall $(OPT)
MPICXX = mpicxx -Wall $(OPT)

all: $(EXECS)

simple: simple.c
	$(CC) $< -o $@

rw_speed: rw_speed.cpp
	$(MPICXX) $< -o $@

mmap_test: mmap_test.cpp
	$(MPICXX) $< -o $@

clean:
	rm -f *.o $(EXECS)
