#include "sstable.h"
#include <fstream>

SSTable::SSTable(SkipList* list, uint64_t timestamp, std::map<uint64_t, std::string> &all, std::map<uint64_t, uint64_t> &offsets) {
    info.header = Header(timestamp, list->size(), list->getMinKey(), list->getMaxKey());
    info.bloomFilter = BloomFilter();
    for (auto it = all.begin(); it != all.end(); it++) {
        info.indexes.push_back(it->first);
        uint32_t vlen = it->second.size();
        data.push_back(DataBlock(it->first, offsets[it->first], vlen));
        info.bloomFilter.insert(it->first);
    }
}

void SSTable::write2file(const std::string &path){
    std::ofstream out(path, std::ios::binary);
    out.write((char*)&(info.header), sizeof(Header));
    out.write((char*)&(info.bloomFilter), sizeof(BloomFilter));
    for (auto it = data.begin(); it != data.end(); it++){
        out.write((char*)&(it->key), sizeof(uint64_t));
        out.write((char*)&(it->offset), sizeof(uint64_t));
        out.write((char*)&(it->vlen), sizeof(uint32_t));
    }
    out.close();
}