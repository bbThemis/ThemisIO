default: test_page_cache

EXECS = test_page_cache
all: $(EXECS) page_cache.o

OPT = -g
# OPT = -O3 -DNDEBUG
CFLAGS = $(OPT) -Wall -Wno-sign-compare -D_GNU_SOURCE
CC = gcc $(CFLAGS)
CXX = g++ $(CFLAGS)

page_cache.o: ../page_cache.cc ../page_cache.h
	$(CXX) -c $<

test_page_cache: test_page_cache.cc page_cache.o
	$(CXX) $^ -o $@


clean:
	rm -f $(EXECS) *.exe *.o *~ *.stackdump test_page_cache.out
