default: all

EXECS = rw_speed rw_speed_write rw_speed_read simple 
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

rw_speed_write: rw_speed_write.cpp
	$(MPICXX) $< -o $@


rw_speed_read: rw_speed_read.cpp
	$(MPICXX) $< -o $@

clean:
	rm -f *.o $(EXECS)
