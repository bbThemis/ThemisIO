CXX=g++
CC=gcc

CLP_INCLUDE_DIR ?= /home1/08571/yhkan/dist/include/coin-or/
CLP_LIB_DIR ?= /home1/08571/yhkan/dist/lib/

MDS_SRCS=util.cpp lnet.cpp mds.cpp lsocket.cpp epoll.cpp lnet_mds.cpp schemes.cpp mds_main.cpp
MDS_OBJS=util.o lnet.o mds.o lsocket.o epoll.o lnet_mds.o schemes.o mds_main.o

CFLAGS=-Wall -Werror -I. -I${CLP_INCLUDE_DIR} -g3 -O0 -c -D_FILE_OFFSET_BITS=64
LDFLAGS=-lfuse -lpthread

CLP_LDFLAGS=-L${CLP_LIB_DIR} -lClp -lCoinUtils -lOsi -lOsiClp -lz

default: mds

test: mds

mds: $(MDS_OBJS)
	$(CXX) -o $@ $^ $(LDFLAGS) ${CLP_LDFLAGS}

%.o: %.c
	$(CC) $(CFLAGS) $< -o $@

%.o: %.cpp
	$(CXX) $(CFLAGS) $< -o $@

cscope:
	mkprojdb $(CURDIR)

clean:
	-@rm -f *.o mds 2>/dev/null || true
	-@rm -f *.log 2>/dev/null || true

tidy:
	-@rm -f *.log 2>/dev/null || true
	-@rm -rf rootdir mountdir

.PHONY: default clean cscope test tidy
