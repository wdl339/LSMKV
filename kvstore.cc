#include "kvstore.h"
#include <string>
#include <map>
#include "skiplist.h"
#include "utils.h"
#include <fstream>

bool searchIndex(uint64_t key, std::vector<uint64_t> &indexes, uint64_t &pos);

KVStore::KVStore(const std::string &dir, const std::string &vlog) : KVStoreAPI(dir, vlog)
{

	memtable = new SkipList();
	vLog = new VLog(vlog);
	DIR_PATH = dir + "/";
	SS_PATH = dir + "/level-";
	VLOG_PATH = vlog;

	// 检查现有数据并恢复
	uint64_t maxTimeStamp = 0;
	for (uint64_t level = 0; utils::dirExists(dirName(level)); level++){
		std::vector<std::string> files;
		utils::scanDir(dirName(level), files);
		for (auto file : files){
			SSTable ss = SSTable(dirName(level) + "/" + file);
			uint64_t timeStamp = ss.info.header.timeStamp;
			ssInfo[level][timeStamp] = ss.info;
			if (timeStamp > maxTimeStamp){
				maxTimeStamp = timeStamp;
			}
		}
	}

	TIMESTAMP = maxTimeStamp + 1;

}

KVStore::~KVStore()
{
	pushMem2ss();
	delete memtable;
	delete vLog;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
	if (!memtable->put(key, s)){
		pushMem2ss();
	}
}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */

// std::map<uint64_t, std::map<uint64_t, Info>> ssInfo; // level, timestamp, Info
std::string KVStore::get(uint64_t key)
{
	std::string res = memtable->get(key);
	if(res == ""){
		std::string tmp = "";
		uint64_t tmp_timestamp = 0;
		for (auto it = ssInfo.begin(); it != ssInfo.end(); it++){
			for (auto it2 = it->second.begin(); it2 != it->second.end(); it2++){
				auto info = it2->second;
				if (!info.header.inArea(key) || info.header.before(tmp_timestamp)){
					continue;
				}
				if (info.bloomFilter.contains(key)){
					uint64_t pos;
					if (searchIndex(key, info.indexes, pos)){
						// return "bbbbbb";
						tmp = readData(it->first, it2->first, pos);
						if (tmp != ""){
							res = tmp;
							tmp_timestamp = it2->first;
						}
					}
					// return "ccccc";	
				}
			}
		}
	}
	if (res == DFLAG){
		return "";
	}
	return res;
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{

	std::string value = get(key);
	if(value == ""){
		return false;
	} else {
		put(key, DFLAG);
		return true;
	}
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
	for (auto it = ssInfo.begin(); it != ssInfo.end(); it++){
		for (auto it2 = it->second.begin(); it2 != it->second.end(); it2++){
			std::string file = filePath(it->first, it2->first);
			utils::rmfile(file);
		}
		if (utils::dirExists(dirName(it->first)))
			utils::rmdir(dirName(it->first));
	}

	utils::rmfile(VLOG_PATH);

	ssInfo.clear();
	TIMESTAMP = 1;
	delete memtable;
	memtable = new SkipList();
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */

// std::map<uint64_t, std::map<uint64_t, Info>> ssInfo; // level, timestamp, Info
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list)
{
	std::map<uint64_t, std::string> res;
	std::map<uint64_t, std::pair<uint64_t, uint64_t>> record; // key, timestamp, pos
	for (auto it = ssInfo.begin(); it != ssInfo.end(); it++){
		for (auto it2 = it->second.begin(); it2 != it->second.end(); it2++){
			auto info = it2->second;
			if (info.header.areaNotCross(key1, key2)){
				continue;
			}
			for (int i = 0; i < info.indexes.size(); i++){
				uint64_t key = info.indexes[i];
				if (key >= key1 && key <= key2){
					record[key] = std::make_pair(it2->first, i);
				}
			}
		}
	}
	for (auto it = record.begin(); it != record.end(); it++){
		uint64_t key = it->first;
		uint64_t timestamp = it->second.first;
		uint64_t pos = it->second.second;
		std::string tmp = readData(0, timestamp, pos);
		if (tmp != ""){
			res[key] = tmp;
		}
	}
	if (!memtable->areaNotCross(key1, key2)){
		memtable->scan(key1, key2, res);
	}
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
	std::map<uint64_t, std::pair<std::string, uint64_t>> kVOff;
	uint64_t holeOff = 0;
	uint64_t len = 0;
	vLog->gc(kVOff, chunk_size, holeOff, len);
	// 在ssTable中查找最新的记录，比较其offSet是否与vLog中的offSet相同，如果将这对kv重新插入到memtable中，否则就是过期数据，不做处理
	for (auto it = kVOff.begin(); it != kVOff.end(); it++){
		uint64_t key = it->first;
		std::string value = it->second.first;
		uint64_t offset = it->second.second;
		uint64_t offInSS = getOffInSS(key);
		if (offInSS == offset){
			put(key, value);
		}
	}
	pushMem2ss();
	utils::de_alloc_file(VLOG_PATH, holeOff, len);

}

uint64_t KVStore::getOffInSS(uint64_t key){
	uint64_t tmp = 0;
	uint64_t tmp_timestamp = 0;
	for (auto it = ssInfo.begin(); it != ssInfo.end(); it++){
			for (auto it2 = it->second.begin(); it2 != it->second.end(); it2++){
				auto info = it2->second;
				if (!info.header.inArea(key) || info.header.before(tmp_timestamp)){
					continue;
				}
				if (info.bloomFilter.contains(key)){
					uint64_t pos;
					if (searchIndex(key, info.indexes, pos)){
						tmp = readOffset(it->first, it2->first, pos);
						tmp_timestamp = it2->first;
					}
				}
			}
	}
	return tmp;
}

void KVStore::pushMem2ss(){
	if (memtable->size() == 0){
		return;
	}
	std::map<uint64_t, std::string> all;
	std::map<uint64_t, uint64_t> offsets;
	memtable->getAll(all);
	vLog->append(all, offsets);
	write2ss(all, offsets);
}

void KVStore::write2ss (std::map<uint64_t, std::string> &all, 
						std::map<uint64_t, uint64_t> &offsets){
	if (!utils::dirExists(dirName(0))){
		createLevel(0);
	}
	SSTable ss = SSTable(memtable, TIMESTAMP, all, offsets);
	std::string path = filePath(0, TIMESTAMP);
	ss.write2file(path);
	ssInfo[0][TIMESTAMP] = ss.info;
	TIMESTAMP++;
	delete memtable;
	memtable = new SkipList();
}

void KVStore::createLevel(uint64_t level){
	utils::_mkdir(dirName(level));
}

std::string KVStore::readData (uint64_t level,
                               uint64_t time_stamp,
                               uint64_t pos) {
    std::string file = filePath(level, time_stamp);
    std::fstream f;
    f.open(file, std::ios::in | std::ios::binary);

    uint64_t offsetPos = pos * sizeData + sizeHeader + sizeFliter + sizeof(uint64_t);
	f.seekg(offsetPos, std::ios::beg);

    uint64_t offset;
    f.read(reinterpret_cast<char*>(&offset), sizeof(offset));
    uint32_t vlen;
    f.read(reinterpret_cast<char*>(&vlen), sizeof(vlen));

	if (vlen == 0){
		return DFLAG;
	}

    return vLog->get(offset, vlen);
}

uint64_t KVStore::readOffset (uint64_t level,
                               uint64_t time_stamp,
                               uint64_t pos) {
    std::string file = filePath(level, time_stamp);
    std::fstream f;
    f.open(file, std::ios::in | std::ios::binary);

    uint64_t offsetPos = pos * sizeData + sizeHeader + sizeFliter + sizeof(uint64_t);
	f.seekg(offsetPos, std::ios::beg);

    uint64_t offset;
    f.read(reinterpret_cast<char*>(&offset), sizeof(offset));

    return offset;
}

bool searchIndex(uint64_t key, std::vector<uint64_t> &indexes, uint64_t &pos) {
	// 二分法查找
	int left = 0, right = indexes.size() - 1;
	while (left <= right) {
		int mid = (left + right) / 2;
		if (indexes[mid] == key) {
			pos = mid;
			return true;
		} else if (indexes[mid] < key) {
			left = mid + 1;
		} else {
			right = mid - 1;
		}
	}
	return false;
}

std::string KVStore::filePath(uint64_t level, uint64_t timestamp){
	return SS_PATH + std::to_string(level) + "/" + std::to_string(timestamp) + ".sst";
}

std::string KVStore::dirName(uint64_t level){
	return SS_PATH + std::to_string(level);
}
