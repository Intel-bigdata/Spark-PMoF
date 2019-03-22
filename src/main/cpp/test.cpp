#include "PersistentMemoryPool.h"
#include "PmemBuffer.h"

int main() {
    PMPool pmpool("/dev/dax0.0", 50, 10, 0);
    char data[2097152] = {};
    memset(data, 'a', 2097152);
    //long addr = pmpool.setReducePartition(30, 0, 0, 2097152, data, true); 
    //printf("data in mem addr is: %p, %ld\n", (void*)addr, addr);
    
    long addr;
    addr = pmpool.setMapPartition(30, 0, 0, 0, 2097152, data, false); 
    printf("Set First Partition, addr: %ld, length: %d\n", addr, 2097152);
    memset(data, 'b', 2097152);
    addr = pmpool.setMapPartition(30, 0, 0, 0, 2097152, data, false); 
    printf("Set Second Partition, addr: %ld, length: %d\n", addr, 2097152);
    memset(data, 'c', 2097152);
    addr = pmpool.setMapPartition(30, 0, 0, 0, 2097152, data, false); 
    printf("Set Third Partition, addr: %ld, length: %d\n", addr, 2097152);
    memset(data, 'd', 2097152);
    addr = pmpool.setMapPartition(30, 0, 0, 0, 2097152, data, false); 
    printf("Set Fourth Partition, addr: %ld, length: %d\n", addr, 2097152);
    memset(data, 'e', 2097152);
    addr = pmpool.setMapPartition(30, 0, 0, 0, 2097152, data, false); 
    printf("Set Fifth Partition, addr: %ld, length: %d\n", addr, 2097152);
    

    //sleep(1);
    /*for (int i = 0; i < 10; i++) {
        MemoryBlock mb;
        pmpool.getPartition(&mb, 0, 0, i); 
    }
    */
    /*MemoryBlock mb;
    long size = pmpool.getMapPartition(&mb, 0, 0, 0);
    printf("size: %d\n", size);*/

    BlockInfo bi;
    int len = pmpool.getMapPartitionBlockInfo(&bi, 0, 0, 0);
    int i = 0;
    printf("numBlocks: %d\n", len);
    while (i < len) {
      printf("addr: %ld, ", bi.data[i++]);
      printf("len: %ld\n", bi.data[i++]);
    }
    //printf("%s\n", mb.buf);
    /*char tmp[201] = {};
    memcpy(tmp, (void*)(bi.data[0]+2096955), 200);
    printf("data: %s\n", tmp);*/

    char print_tmp[201] = {};
    char tmp[2097150] = {};
    PmemBuffer pmBuffer;
    pmBuffer.load((char*)bi.data[0], (int)bi.data[1]);
    pmBuffer.load((char*)bi.data[2], (int)bi.data[3]);
    int read_len = pmBuffer.read(tmp, 2097150);
    memcpy(print_tmp, tmp, 200);
    printf("read_len:%d, data: %s\n", read_len, print_tmp);

    read_len = pmBuffer.read(tmp, 2097150);
    memcpy(print_tmp, tmp, 200);
    printf("read_len:%d, data: %s\n", read_len, print_tmp);
    
    return 0;
}

