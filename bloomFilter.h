#include "MurmurHash3.h"

#define HASH_NUM 4
#define HASH_SIZE 8192

class BloomFilter 
{

private:
    bool hashMap[HASH_SIZE];

public:
    BloomFilter() {
        for(int i = 0; i < HASH_SIZE; i++) {
            this->hashMap[i] = false;
        }
    }

    void insert(uint64_t key) {
        uint32_t hash[HASH_NUM] = {0};
        MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
        for(int i = 0; i < HASH_NUM; i++) {
            hashMap[hash[i] % HASH_SIZE] = true;
        }
    }

    bool contains(uint64_t key) {
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