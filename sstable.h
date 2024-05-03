#ifndef SSTABLE_H
#define SSTABLE_H

#include <cstdint>
#include <string>
#include "bloomFilter.h"
#include <vector>
#include "skiplist.h"
#include "utils.h"

class Header
{

public:
    uint64_t timeStamp;
    uint64_t kvNum;
    uint64_t minKey;
    uint64_t maxKey;

    Header(uint64_t t = 0, uint64_t n = 0, uint64_t min = 0, uint64_t max = 0) 
        : timeStamp(t), kvNum(n), minKey(min), maxKey(max) {}
    bool inArea(uint64_t key) {
        return key >= minKey && key <= maxKey;
    }

    bool areaNotCross(uint64_t min, uint64_t max) {
        return minKey >= max || maxKey <= min;
    }

    bool before(uint64_t time) {
        return time > timeStamp;
    }
};

class DataBlock
{

public:
    uint64_t key;
    uint64_t offset;
    uint32_t vlen;

    DataBlock() {}
    DataBlock(uint64_t k, uint64_t o, uint32_t v) : key(k), offset(o), vlen(v) {}
};

struct Info{
    Header header;
    BloomFilter bloomFilter;
    std::vector<uint64_t> indexes;
};

class SSTable
{
public:
    Info info;
    std::vector<DataBlock> data;
    std::string DFLAG = "~DELETED~";

    SSTable(SkipList* list, uint64_t timestamp, std::map<uint64_t, std::string> &all, std::map<uint64_t, uint64_t> &offsets);
    SSTable(const std::string &path);
    void write2file(const std::string &path);

};

#endif // SSTABLE_H