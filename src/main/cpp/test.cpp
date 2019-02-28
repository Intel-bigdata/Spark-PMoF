#include "PersistentMemoryPool.h"

int main() {
    PMPool pmpool("/mnt/pmem0/shuffle_block_f73f6bd4-abf8-4eca-8766-bb00c26f6633", 100, 100, 214748364800);
    /*char data[2097152] = {};
    memset(data, 'a', 2097152);
    long addr = pmpool.setMapPartition(30, 0, 0, 0, 2097152, data, true); 
    //long addr = pmpool.setReducePartition(30, 0, 0, 2097152, data, true); 
    printf("data in mem addr is: %p, %ld\n", (void*)addr, addr);
    */
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

    //sleep(1);
    MemoryBlock mb;
    long size = pmpool.getMapPartition(&mb, 0, 9, 0);
    printf("size: %ld\n", size);
    //printf("%s\n", mb.buf);
    /*CHAR TMP[201] = {};
    memcpy(tmp, (void*)addr, 200);
    printf("data: %s\n", tmp);*/

    return 0;
}

