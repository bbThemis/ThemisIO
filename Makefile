CXX=g++
CXXFLAGS=-march=skylake-avx512 -g -I/opt/intel/compilers_and_libraries_2018.6.288/linux/mpi/intel64/include -DNCX_PTR_SIZE=8 -pipe -DLOG_LEVEL=4  -DPAGE_MERGE
OBJS=put_get_server.o qp.o dict.o xxhash.o io_queue.o myfs.o io_ops.o buddy.o ncx_slab.o corebinding.o unique_thread.o queue_free_mem.o fair_queue.o
#HEADERS=dict.h qp_common.h qp.h io_queue.h utility.h xxhash.h list.h buddy.h myfs_common.h myfs.h io_ops_common.h io_ops.h ncx_slab.h ncx_core.h ncx_log.h client/qp_client.h
HEADERS=dict.h qp_common.h qp.h io_queue.h utility.h xxhash.h list.h buddy.h  myfs.h io_ops_common.h io_ops.h corebinding.h unique_thread.h ncx_slab.h ncx_core.h ncx_log.h queue_free_mem.h fixed_mem_allocator.h fair_queue.h
RM=rm -rf

# in cmd of windows
ifeq ($(SHELL),sh.exe)
    RM := del /f/q
endif

#all: server fsclient
all: server

server: $(OBJS)
	$(CXX) -O2 $(CXXFLAGS) -g -o $@ put_get_server.o qp.o io_queue.o buddy.o myfs.o io_ops.o corebinding.o unique_thread.o dict.o xxhash.o ncx_slab.o queue_free_mem.o -libverbs -lpthread -lrt -Wunused-variable -L/opt/intel/compilers_and_libraries_2018.6.288/linux/mpi/intel64/lib/release_mt -L/opt/intel/compilers_and_libraries_2018.6.288/linux/mpi/intel64/lib -lmpicxx -lmpifort -lmpi -ldl
#	$(CXX) $(CXXFLAGS) -o $@ put_get_server.o io_queue.o dict.o xxhash.o -libverbs -lpthread -lrt -Wunused-variable -L/opt/intel/compilers_and_libraries_2020.4.304/linux/mpi/intel64/lib/release -L/opt/intel/compilers_and_libraries_2020.4.304/linux/mpi/intel64/lib -lmpicxx -lmpifort -lmpi -ldl

#fsclient: $(OBJS)
#	$(CXX) $(CXXFLAGS) -o $@ put_get_client.o dict.o xxhash.o -libverbs -lpthread -lrt

put_get_server.o: put_get_server.cpp $(HEADERS)
	$(CXX) -O0 $(CXXFLAGS) -c $<

qp.o: qp.cpp $(HEADERS)
	$(CXX) -O2 $(CXXFLAGS) -c $<

dict.o: dict.cpp $(HEADERS)
	$(CXX) -g -O2 $(CXXFLAGS) -c $<

xxhash.o: xxhash.cpp $(HEADERS)
	$(CXX) -O2 $(CXXFLAGS) -c $<

io_queue.o: io_queue.cpp $(HEADERS)
	$(CXX) -g -O0 -c $<

fair_queue.o: fair_queue.cpp $(HEADERS)
	$(CXX) -g -O0 -c $<

corebinding.o: corebinding.cpp $(HEADERS)
	$(CXX) -O2 $(CXXFLAGS) -c $<
unique_thread.o: unique_thread.cpp $(HEADERS)
	$(CXX) -O2 $(CXXFLAGS) -c $<

buddy.o: buddy.cpp $(HEADERS)
	$(CXX) -O2 $(CXXFLAGS) -c $<
myfs.o: myfs.cpp $(HEADERS)
	$(CXX) -g -O2 $(CXXFLAGS) -c $<

io_ops.o: io_ops.cpp $(HEADERS)
	$(CXX) -O0 $(CXXFLAGS) -c $<

ncx_slab.o: ncx_slab.cpp $(HEADERS)
	$(CXX) -O2 $(CXXFLAGS) -c $<
queue_free_mem.o: queue_free_mem.cpp $(HEADERS)
	$(CXX) -g -O2 $(CXXFLAGS) -c $<

#put_get_client.o: client/put_get_client.cpp $(HEADERS)
#	$(CXX) $(CXXFLAGS) -c $<

#run: myfs
#	./myfs
#	dot bdgraph.dot -Tpng > bd.png

clean:
	$(RM) *.o server
#	$(RM) *.o server fsclient
