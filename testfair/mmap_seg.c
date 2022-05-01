#include <sys/mman.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

int main(int argc, char *argv[]){
    char * ptr2 = (char *)malloc(1024);
    char * ptr = (char*) mmap(NULL, 4096, PROT_READ|PROT_WRITE, MAP_PRIVATE| MAP_ANONYMOUS, -1, 0);
    printf("%p\n", ptr);
    assert(ptr != (void *) -1);

    ptr2[0] = 'b';
    printf("HERE\n");
    ptr[0] = 'a';
    
    return 0;

}