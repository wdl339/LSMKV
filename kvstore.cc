#include "kvstore.h"
#include <string>
#include <map>
#include "skiplist.h"

KVStore::KVStore(const std::string &dir, const std::string &vlog) : KVStoreAPI(dir, vlog)
{

	memtable = new SkipList();
	DIR_PATH = dir + "/";
	PREFIX = dir + "/level";
	VLOG_PATH = vlog;

}

KVStore::~KVStore()
{
	delete memtable;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
	if (memtable->put(key, s)){

	}
	
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
	std::string value = memtable->get(key);
	if(value == DFLAG){
		//search in sstables
	} else {
		return value;
	}
	return "";
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
	std::string value = memtable->get(key);
	if(value == "" || value == DFLAG){
		//search in sstables
	} else {
		memtable->del(key);
		return true;
	}
	return false;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{

}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list)
{
	std::map<uint64_t, std::string> res;
	if (memtable->getMinKey() <= key2 && memtable->getMaxKey() >= key1){
		memtable->scan(key1, key2, res);
	}
	//search in sstables
	for (auto it = res.begin(); it != res.end(); it++){
		if (it->second == DFLAG){
			continue;
		}
		list.push_back(std::make_pair(it->first, it->second));
	}
}

/**
 * This reclaims space from vLog by moving valid value and discarding invalid value.
 * chunk_size is the size in byte you should AT LEAST recycle.
 */
void KVStore::gc(uint64_t chunk_size)
{

}