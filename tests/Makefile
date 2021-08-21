default: all

EXECS=fill_disk iops_stat_mpi
CFLAGS=-Wall -g -O0
CXX=g++ $(CFLAGS)
MPICC=mpicc $(CFLAGS)
MPICXX=mpicxx $(CFLAGS)

all: $(EXECS)

fill_disk: fill_disk.cpp
	$(CXX) $^ -o $@

iops_stat_mpi: iops_stat_mpi.c
	$(MPICC) $^ -o $@

clean:
	rm -f $(EXECS) *.o *.so *~

