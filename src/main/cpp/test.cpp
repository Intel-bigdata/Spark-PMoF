#include "PersistentMemoryPool.h"

int main() {
    PMPool pmpool("/dev/dax0.0", 100, 100, 10, 20);
    char data[2097152] = {};
    memset(data, 'a', 2097152);
    long addr = pmpool.setPartition(30, 0, 0, 0, 2097152, data); 
    printf("data in mem addr is: %p, %ld\n", (void*)addr, addr);
    
    /*for (int i = 0; i < 10; i++) {
        MemoryBlock mb;
        pmpool.getPartition(&mb, 0, 0, i); 
    }
    */

    /*pmpool.setPartition(30, 0, 0, 0, 2097152, data); 
    printf("Set Second Partition\n");
    pmpool.setPartition(30, 0, 0, 0, 2097152, data); 
    printf("Set Third Partition\n");
    pmpool.setPartition(30, 0, 0, 0, 2097152, data); 
    printf("Set Fourth Partition\n");
    pmpool.setPartition(30, 0, 0, 0, 2097152, data); 
    printf("Set Fifth Partition\n");
    */

    sleep(1);
    //MemoryBlock mb;
    //long size = pmpool.getPartition(&mb, 0, 0, 0);
    //printf("size: %ld\n", size);
    char tmp[201] = {};
    memcpy(tmp, (void*)addr, 200);
    printf("data: %s\n", tmp);

    return 0;
}

