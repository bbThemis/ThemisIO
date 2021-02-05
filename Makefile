CXX=g++
CXXFLAGS=-march=skylake-avx512 -g -O0 -I/opt/intel/compilers_and_libraries_2020.4.304/linux/mpi/intel64/include
OBJS=put_get_server.o dict.o xxhash.o put_get_client.o io_queue.o
HEADERS=dict.h qp_common.h qp.h io_queue.h utility.h xxhash.h client/qp_client.h
RM=rm -rf

# in cmd of windows
ifeq ($(SHELL),sh.exe)
    RM := del /f/q
endif

all: server fsclient

server: $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ put_get_server.o io_queue.o -libverbs -lpthread -lrt -Wunused-variable -L/opt/intel/compilers_and_libraries_2020.4.304/linux/mpi/intel64/lib/release -L/opt/intel/compilers_and_libraries_2020.4.304/linux/mpi/intel64/lib -lmpicxx -lmpifort -lmpi -ldl
#	$(CXX) $(CXXFLAGS) -o $@ put_get_server.o io_queue.o dict.o xxhash.o -libverbs -lpthread -lrt -Wunused-variable -L/opt/intel/compilers_and_libraries_2020.4.304/linux/mpi/intel64/lib/release -L/opt/intel/compilers_and_libraries_2020.4.304/linux/mpi/intel64/lib -lmpicxx -lmpifort -lmpi -ldl

fsclient: $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ put_get_client.o dict.o xxhash.o -libverbs -lpthread -lrt

put_get_server.o: put_get_server.cpp $(HEADERS)
	$(CXX) $(CXXFLAGS) -c $<

dict.o: dict.cpp $(HEADERS)
	$(CXX) $(CXXFLAGS) -c $<

xxhash.o: xxhash.cpp $(HEADERS)
	$(CXX) $(CXXFLAGS) -c $<

io_queue.o: io_queue.cpp $(HEADERS)
	$(CXX) $(CXXFLAGS) -c $<

put_get_client.o: client/put_get_client.cpp $(HEADERS)
	$(CXX) $(CXXFLAGS) -c $<

#run: myfs
#	./myfs
#	dot bdgraph.dot -Tpng > bd.png

clean:
	$(RM) *.o server fsclient
