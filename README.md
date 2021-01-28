# TBD

g++ -march=skylake-avx512 -g -O0 -o server put_get_server.cpp -libverbs -lpthread -lrt -Wunused-variable -I/opt/intel/compilers_and_libraries_2020.4.304/linux/mpi/intel64/include -L/opt/intel/compilers_and_libraries_2020.4.304/linux/mpi/intel64/lib/release -L/opt/intel/compilers_and_libraries_2020.4.304/linux/mpi/intel64/lib -lmpicxx -lmpifort -lmpi -ldl
gcc -g -o fsclient client/put_get_client.cpp dict.cpp xxhash.cpp -libverbs -lpthread -lrt

