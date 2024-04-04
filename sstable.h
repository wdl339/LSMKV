#ifndef SSTABLE_H
#define SSTABLE_H

#include <cstdint>
#include <string>
#include "bloomFilter.h"
#include <vector>
#include "skiplist.h"
#include "utils.h"

uint64_t TIMESTAMP = 1;

class Header
{
    uint64_t timeStamp;
    uint64_t kvNum;
    uint64_t minKey;
    uint64_t maxKey;

public:
    Header(uint64_t t = 0, uint64_t n = 0, uint64_t min = 0, uint64_t max = 0) 
        : timeStamp(t), kvNum(n), minKey(min), maxKey(max) {}
};

class DataBlock
{
    uint64_t key;
    std::string value;
    uint64_t offset;
public:
    DataBlock(uint64_t k, std::string v, uint64_t o) : key(k), value(v), offset(o) {}
};

class SSTable
{
    Header header;
    BloomFilter bloomFilter;
    std::vector<DataBlock> data;

public:
    SSTable(SkipList* list);

};

#endif // SSTABLE_H