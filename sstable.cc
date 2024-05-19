#include "sstable.h"
#include <fstream>

SSTable::SSTable(SkipList* list, 
                uint64_t timestamp, 
                std::map<uint64_t, std::string> &all, 
                std::map<uint64_t, uint64_t> &offsets) {
    info.header = Header(timestamp, list->size(), list->getMinKey(), list->getMaxKey());
    info.bloomFilter = BloomFilter();
    for (auto it = all.begin(); it != all.end(); it++) {
        info.indexes.push_back(it->first);
        uint32_t vlen = it->second.size();

        if (it->second == DFLAG) {
            vlen = 0;
        }

        data.push_back(DataBlock(it->first, offsets[it->first], vlen));
        info.bloomFilter.insert(it->first);
    }
}

SSTable::SSTable(uint64_t timestamp, 
                uint64_t size,
                uint64_t minKey,
                uint64_t maxKey,
                std::vector<DataBlock> &all) {
    info.header = Header(timestamp, size, minKey, maxKey);
    info.bloomFilter = BloomFilter();
    for (auto it = all.begin(); it != all.end(); it++) {
        info.indexes.push_back(it->key);
        data.push_back(DataBlock(it->key, it->offset, it->vlen));
        info.bloomFilter.insert(it->key);
    }
}

SSTable::SSTable(const std::string &path) {
    std::ifstream in(path, std::ios::binary);
    in.read((char*)&(info.header), sizeof(Header));
    in.read((char*)&(info.bloomFilter), sizeof(BloomFilter));
    while (!in.eof()) {
        DataBlock tmp;
        in.read((char*)&(tmp.key), sizeof(uint64_t));
        in.read((char*)&(tmp.offset), sizeof(uint64_t));
        in.read((char*)&(tmp.vlen), sizeof(uint32_t));
        data.push_back(tmp);
        info.indexes.push_back(tmp.key);
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