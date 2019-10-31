#include "Request.h"
#include "PersistentMemoryPool.h"
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <iostream>

/******  Request ******/
TOID(struct Block) Request::getBlock() {
    int keyForIndex = key >> 34;
    int blockBucketIndex = keyForIndex % BLOCK_BUCKETS_NUM;
    int blockIndex = keyForIndex / BLOCK_BUCKETS_NUM;

    if(TOID_IS_NULL(D_RO(pmpool_ptr->hashMapRoot)->blockBuckets)){
        fprintf(stderr, "BlockBuckets does not Exist.\n");
        return TOID_NULL(struct Block);
    }

    TOID(struct BlockBuckets) blockBuckets = D_RO(pmpool_ptr->hashMapRoot)->blockBuckets;

    TOID(struct BlockBucketItem) blockBucketItem = D_RO(blockBuckets)->items[blockBucketIndex];
    if(TOID_IS_NULL(blockBucketItem) || TOID_IS_NULL(D_RO(blockBucketItem)->blockBucket)) {
        fprintf(stderr, "BlockBucket does not Exist.\n");
        return TOID_NULL(struct Block);
    }

    TOID(struct Block) block = D_RO(D_RO(blockBucketItem)->blockBucket)->items[blockIndex];
    while (!TOID_IS_NULL(block) && D_RO(block)->key != key) {
        block = D_RO(block)->nextBlock;
    }
    if(TOID_IS_NULL(block)) {
        fprintf(stderr, "Block does not Exist.\n");
        return TOID_NULL(struct Block);
    }

    return block;
}

TOID(struct Block)* Request::getAndCreateBlock() {
    int keyForIndex = key >> 34;
    int blockBucketIndex = keyForIndex % BLOCK_BUCKETS_NUM;
    int blockIndex = keyForIndex / BLOCK_BUCKETS_NUM;

    if (TOID_IS_NULL(pmpool_ptr->hashMapRoot)){
        pmpool_ptr->hashMapRoot = TX_ZNEW(struct HashMapRoot);
    }
    TX_ADD(pmpool_ptr->hashMapRoot);

    if (TOID_IS_NULL(D_RO(pmpool_ptr->hashMapRoot)->blockBuckets)){
        D_RW(pmpool_ptr->hashMapRoot)->blockBuckets = 
            TX_ZALLOC(struct BlockBuckets, sizeof(struct BlockBuckets) + BLOCK_BUCKETS_NUM * sizeof(struct BlockBucketItem));
    }

    TX_ADD_FIELD(pmpool_ptr->hashMapRoot, blockBuckets);
    TOID(struct BlockBucketItem) *blockBucketItem = &(D_RW(D_RW(pmpool_ptr->hashMapRoot)->blockBuckets)->items[blockBucketIndex]);
    if (TOID_IS_NULL(*blockBucketItem)) {
        *blockBucketItem = TX_ZNEW(struct BlockBucketItem);
    }
    TX_ADD(*blockBucketItem);
    if (TOID_IS_NULL(D_RO(*blockBucketItem)->blockBucket)) {
        D_RW(*blockBucketItem)->blockBucket = TX_ZALLOC(struct BlockBucket, sizeof(struct BlockBucket) + BLOCKS_NUM * sizeof(struct Block));
    }
    TX_ADD_FIELD(*blockBucketItem, blockBucket);

    TOID(struct Block) *block = &(D_RW(D_RW(*blockBucketItem)->blockBucket)->items[blockIndex]);

    while (!TOID_IS_NULL(*block) && D_RO(*block)->key != key) {
        //cout << "collision happend!" <<  endl;
        block = &(D_RW(*block)->nextBlock);
    }
    // could find a exists block or a null block.
    return block;
}


void Request::freeBlock(TOID(struct Block) block) {
    if (TOID_IS_NULL(block)) return;
    TOID(struct BlockPartition) blockPartition = D_RO(block)->firstBlockPartition;
    TOID(struct BlockPartition) nextBlockPartition = blockPartition;

    while(!TOID_IS_NULL(blockPartition)) {
        nextBlockPartition = D_RO(blockPartition)->nextBlockPartition;
        pmemobj_tx_free(D_RO(blockPartition)->data);
        TX_FREE(blockPartition);
        blockPartition = nextBlockPartition;
    }
    TX_ADD(block);
    D_RW(block)->firstBlockPartition = TOID_NULL(struct BlockPartition);
    D_RW(block)->lastBlockPartition = D_RW(block)->firstBlockPartition;
    D_RW(block)->size = 0;
    D_RW(block)->numBlocks = 0;
}

/******  WriteRequest ******/
void WriteRequest::exec() {
    setBlock();
}

void WriteRequest::setBlock() {
    TX_BEGIN(pmpool_ptr->pmpool) {
        TOID(struct Block) *block = getAndCreateBlock();
        if (set_clean) {
            freeBlock(*block);
        }
        if(TOID_IS_NULL(*block)) {
            *block = TX_ZNEW(struct Block);
            TX_ADD(*block);
            D_RW(*block)->firstBlockPartition = TOID_NULL(struct BlockPartition);
            D_RW(*block)->lastBlockPartition = D_RW(*block)->firstBlockPartition;
            D_RW(*block)->key = key;
            D_RW(*block)->size = 0;
            D_RW(*block)->numBlocks = 0;
            D_RW(*block)->nextBlock = TOID_NULL(struct Block);
        } else {
            TX_ADD(*block);
        }
        TOID(struct BlockPartition) currentBlockPartition = TOID_NULL(struct BlockPartition);
        if (TOID_IS_NULL(D_RO(*block)->firstBlockPartition)) {
            D_RW(*block)->firstBlockPartition = TX_ZNEW(struct BlockPartition);
            D_RW(*block)->lastBlockPartition = D_RW(*block)->firstBlockPartition;
        } else {
            currentBlockPartition = D_RW(*block)->lastBlockPartition;
            D_RW(*block)->lastBlockPartition = TX_ZNEW(struct BlockPartition);
        }

        TX_ADD_FIELD(*block, lastBlockPartition);
        if (!TOID_IS_NULL(currentBlockPartition)) {
            TX_ADD(currentBlockPartition);
            D_RW(currentBlockPartition)->nextBlockPartition = D_RW(*block)->lastBlockPartition;
        }
        TOID(struct BlockPartition) *lastBlockPartition = &(D_RW(*block)->lastBlockPartition);
        D_RW(*lastBlockPartition)->data = pmemobj_tx_zalloc(size, 0);
        D_RW(*lastBlockPartition)->data_size = size;
        D_RW(*lastBlockPartition)->nextBlockPartition = TOID_NULL(struct BlockPartition);

        data_addr = (char*)pmemobj_direct(D_RW(*lastBlockPartition)->data);
        pmemobj_tx_add_range_direct((const void *)data_addr, size);
        memcpy(data_addr, data, size);

        D_RW(*block)->size += size;
        D_RW(*block)->numBlocks += 1;
    } TX_ONCOMMIT {
        committed = true;
    } TX_ONABORT {
        fprintf(stderr, "Set Block failed. Error: %s\n", pmemobj_errormsg());
        exit(-1);
    } TX_END
}

/******  ReadRequest ******/
void ReadRequest::exec() {
    readBlock();
}

void ReadRequest::readBlock() {
    TOID(struct Block) block = getBlock();
    if (TOID_IS_NULL(block)) {
        mb->buf = nullptr;
        mb->len = 0;
        return;
    }
    long data_length = D_RO(block)->size;
    mb->buf = new char[data_length]();
    mb->len = data_length;
    long off = 0;
    TOID(struct BlockPartition) blockPartition = D_RO(block)->firstBlockPartition;

    char* data_addr;
    while(!TOID_IS_NULL(blockPartition)) {
        data_addr = (char*)pmemobj_direct(D_RO(blockPartition)->data);

        memcpy(mb->buf + off, data_addr, D_RO(blockPartition)->data_size);
        off += D_RO(blockPartition)->data_size;
        blockPartition = D_RO(blockPartition)->nextBlockPartition;
    }
}

/******  MetaRequest ******/
void MetaRequest::exec() {
    getBlockIndex();
}

void MetaRequest::getBlockIndex() {
    TOID(struct Block) block = getBlock();
    if (TOID_IS_NULL(block)) {
        block_info->data = nullptr;
        block_info->len = 0;
        return;
    }
    TOID(struct BlockPartition) blockPartition = D_RO(block)->firstBlockPartition;
    if (TOID_IS_NULL(blockPartition)) return;

    int numBlocks = D_RO(block)->numBlocks;
    long array_length = numBlocks * 2;
    block_info->data = new long[array_length]();
    block_info->len = array_length;
    int i = 0;

    while(!TOID_IS_NULL(blockPartition)) {
        block_info->data[i++] = (long)pmemobj_direct(D_RO(blockPartition)->data);
        block_info->data[i++] = D_RO(blockPartition)->data_size;
        blockPartition = D_RO(blockPartition)->nextBlockPartition;
    }
}

/******  SizeRequest ******/
void SizeRequest::exec() {
    getBlockSize();
}

void SizeRequest::getBlockSize() {
    TOID(struct Block) block = getBlock();
    if (TOID_IS_NULL(block)) {
        *data_length = 0;
        return;
    }
    *data_length = D_RO(block)->size;
}


/******  DeleteRequest ******/
void DeleteRequest::exec() {
    deleteBlock();
}

void DeleteRequest::deleteBlock() {
    TX_BEGIN(pmpool_ptr->pmpool) {
        TOID(struct Block) block = getBlock();
        if (TOID_IS_NULL(block)) return;
        freeBlock(block);
    } TX_ONCOMMIT {
        committed = true;
    } TX_ONABORT {
        fprintf(stderr, "delete Block failed. Error: %s\n", pmemobj_errormsg());
        exit(-1);
    } TX_END
}
