default: all

EXECS = test_page_cache test_last_modified test_inotify
all: $(EXECS)

OPT = -g
# OPT = -O3 -DNDEBUG
CFLAGS = $(OPT) -Wall -Wno-sign-compare -D_GNU_SOURCE
CC = gcc $(CFLAGS)
CXX = g++ $(CFLAGS)
MPICXX = mpicxx $(CFLAGS)

page_cache.o: ../page_cache.cc ../page_cache.h
	$(CXX) -c $<

test_page_cache: test_page_cache.cc page_cache.o
	$(CXX) $^ -o $@

test_last_modified: test_last_modified.cc simple_stats.hh
	$(MPICXX) $< -o $@

test_inotify: test_inotify.cc
	$(CXX) $< -o $@

clean:
	rm -f $(EXECS) *.exe *.o *~ *.stackdump test_page_cache.out
