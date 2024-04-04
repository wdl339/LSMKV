#include "sstable.h"

SSTable::SSTable(SkipList* list) {
    header = Header(TIMESTAMP, list->size(), list->getMinKey(), list->getMaxKey());
    bloomFilter = BloomFilter();
    
}