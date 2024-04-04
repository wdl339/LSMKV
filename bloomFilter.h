#include "MurmurHash3.h"

#define HASH_NUM 4
#define HASH_SIZE (1024 * 8)

class BloomFilter 
{

private:
    bool *hashMap;

public:
    BloomFilter() {
        hashMap = new bool[HASH_SIZE];
        for(int i = 0; i < HASH_SIZE; i++) {
            this->hashMap[i] = false;
        }
    }

    ~BloomFilter() {
        delete[] hashMap;
    }

    void insert(uint64_t key) {
        uint32_t hash[HASH_NUM] = {0};
        MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
        for(int i = 0; i < HASH_NUM; i++) {
            hashMap[hash[i] % HASH_SIZE] = true;
        }
    }

    bool query(uint64_t key) {
        uint32_t hash[HASH_NUM] = {0};
        MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
        for(int i = 0; i < HASH_NUM; i++) {
            if(!hashMap[hash[i] % HASH_SIZE]) {
                return false;
            }
        }
        return true;
    }
};