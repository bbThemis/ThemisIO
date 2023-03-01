default: all

EXECS=fill_disk iops_stat_mpi fs_variability
CFLAGS=-Wall -g -O0
CC=gcc $(CFLAGS)
CXX=g++ $(CFLAGS)
MPICC=mpicc $(CFLAGS)
MPICXX=mpicxx $(CFLAGS)

# have Lustre?
LFS = $(shell which lfs 2>/dev/null)
ifeq ($(LFS),)
  CFLAGS+=-DNO_LUSTRE
else
  CFLAGS+=-DHAVE_LUSTRE
  LUSTRE_LIB=-llustreapi
endif

all: $(EXECS)

fill_disk: fill_disk.cpp
	$(CXX) $^ -o $@

iops_stat_mpi: iops_stat_mpi.c
	$(MPICC) $^ -o $@

lustre_wrapper.o: lustre_wrapper.c lustre_wrapper.h
	$(CC) -c $<

common.o: common.cpp common.h
	$(CXX) -c $<

fs_variability: fs_variability.cpp common.o lustre_wrapper.o
	$(CXX) $^ -o $@

clean:
	rm -f $(EXECS) *.o *.so *~

