CXX=g++
OPT=-O2 -g
# OPT=
CXXFLAGS=-Iinclude/ -march=skylake-avx512 -g -I/opt/intel/compilers_and_libraries_2019.5.281/linux/mpi/intel64/include -DNCX_PTR_SIZE=8 -pipe -DLOG_LEVEL=4  -DPAGE_MERGE
OBJS=obj/put_get_server.o obj/qp.o obj/dict.o obj/xxhash.o obj/io_queue.o obj/myfs.o obj/io_ops.o obj/buddy.o obj/ncx_slab.o obj/corebinding.o obj/unique_thread.o obj/queue_free_mem.o obj/fair_queue.o
#HEADERS=dict.h qp_common.h qp.h io_queue.h utility.h xxhash.h list.h buddy.h myfs_common.h myfs.h io_ops_common.h io_ops.h ncx_slab.h ncx_core.h ncx_log.h client/qp_client.h
HEADERS=include/dict.h include/qp_common.h include/qp.h include/io_queue.h include/utility.h include/xxhash.h include/list.h include/buddy.h include/myfs.h include/io_ops_common.h include/io_ops.h include/corebinding.h include/unique_thread.h include/ncx_slab.h include/ncx_core.h include/ncx_log.h include/queue_free_mem.h include/fixed_mem_allocator.h include/fair_queue.h
RM=rm -rf

# in cmd of windows
ifeq ($(SHELL),sh.exe)
    RM := del /f/q
endif

#all: server fsclient
all: server

#iops_stat_mpi:
#	mpicc -g -O0 -o tests/iops_stat_mpi tests/iops_stat_mpi.c

server: $(OBJS)
	$(CXX) -O2 $(CXXFLAGS) -g -o $@ obj/put_get_server.o obj/qp.o obj/io_queue.o obj/fair_queue.o obj/buddy.o obj/myfs.o obj/io_ops.o obj/corebinding.o obj/unique_thread.o obj/dict.o obj/xxhash.o obj/ncx_slab.o obj/queue_free_mem.o -libverbs -lpthread -lrt -Wunused-variable -L/opt/intel/compilers_and_libraries_2019.5.281/linux/mpi/intel64/lib/release_mt -L/opt/intel/compilers_and_libraries_2019.5.281/linux/mpi/intel64/lib -lmpicxx -lmpifort -lmpi -ldl
#	$(CXX) $(CXXFLAGS) -o $@ put_get_server.o io_queue.o dict.o xxhash.o -libverbs -lpthread -lrt -Wunused-variable -L/opt/intel/compilers_and_libraries_2020.4.304/linux/mpi/intel64/lib/release -L/opt/intel/compilers_and_libraries_2020.4.304/linux/mpi/intel64/lib -lmpicxx -lmpifort -lmpi -ldl

#fsclient: $(OBJS)
#	$(CXX) $(CXXFLAGS) -o $@ put_get_client.o dict.o xxhash.o -libverbs -lpthread -lrt

# disable optimization. Possible threading bugs.
obj/put_get_server.o: src/put_get_server.cpp $(HEADERS)
	$(CXX) -O0 $(CXXFLAGS) -c -o obj/put_get_server.o $<

obj/qp.o: src/qp.cpp $(HEADERS)
	$(CXX) $(OPT) $(CXXFLAGS) -c -o obj/qp.o $<

obj/dict.o: src/dict.cpp $(HEADERS)
	$(CXX) $(OPT) $(CXXFLAGS) -c -o obj/dict.o $<

obj/xxhash.o: src/xxhash.cpp $(HEADERS)
	$(CXX) $(OPT) $(CXXFLAGS) -c -o obj/xxhash.o $<

# disable optimization. Possible threading bugs.
obj/io_queue.o: src/io_queue.cpp $(HEADERS)
	$(CXX) -Iinclude/ -g -O0 -c -o obj/io_queue.o $<

obj/fair_queue.o: src/fair_queue.cpp $(HEADERS)
	$(CXX) $(OPT) $(CXXFLAGS) -c -o obj/fair_queue.o  $<

obj/corebinding.o: src/corebinding.cpp $(HEADERS)
	$(CXX) $(OPT) $(CXXFLAGS) -c -o obj/corebinding.o $<
obj/unique_thread.o: src/unique_thread.cpp $(HEADERS)
	$(CXX) $(OPT) $(CXXFLAGS) -c -o obj/unique_thread.o $<

obj/buddy.o: src/buddy.cpp $(HEADERS)
	$(CXX) $(OPT) $(CXXFLAGS) -c -o obj/buddy.o $<
obj/myfs.o: src/myfs.cpp $(HEADERS)
	$(CXX) $(OPT) $(CXXFLAGS) -c -o obj/myfs.o $<

# disable optimization for io_ops.cpp. Possible threading bugs.
obj/io_ops.o: src/io_ops.cpp $(HEADERS)
	$(CXX) -O0 $(CXXFLAGS) -c -o obj/io_ops.o $<

obj/ncx_slab.o: src/ncx_slab.cpp $(HEADERS)
	$(CXX) $(OPT) $(CXXFLAGS) -c -o obj/ncx_slab.o $<
obj/queue_free_mem.o: src/queue_free_mem.cpp $(HEADERS)
	$(CXX) $(OPT) $(CXXFLAGS) -c -o obj/queue_free_mem.o $<

#put_get_client.o: client/put_get_client.cpp $(HEADERS)
#	$(CXX) $(CXXFLAGS) -c $<

#run: myfs
#	./myfs
#	dot bdgraph.dot -Tpng > bd.png

clean:
	$(RM) obj/*.o src/client/*.o wrapper.so server
#	$(RM) *.o server fsclient
