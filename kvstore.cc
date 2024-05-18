#include "kvstore.h"
#include <string>
#include <map>
#include "skiplist.h"
#include "utils.h"
#include <fstream>
#include <algorithm>

bool searchIndex(uint64_t key, std::vector<uint64_t> &indexes, uint64_t &pos);

KVStore::KVStore(const std::string &dir, const std::string &vlog) : KVStoreAPI(dir, vlog)
{

	memtable = new SkipList();
	vLog = new VLog(vlog);
	cacheSS = nullptr;
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
			uint64_t timeStamp, tag;
			getInfoInFileName(file, timeStamp, tag);
			ssInfo[level][{timeStamp, tag}] = ss.info;
			if (timeStamp > maxTimeStamp){
				maxTimeStamp = timeStamp;
			}
			if (maxTag.find(timeStamp) == maxTag.end() || tag > maxTag[timeStamp]){
				maxTag[timeStamp] = tag;
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
	if (cacheSS != nullptr){
		delete cacheSS;
	}
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

// std::map<uint64_t, std::map<std::pair<uint64_t, uint64_t>, Info>> ssInfo; // level, {timestamp, tag}, Info
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
						tmp = readData(it->first, (it2->first).first, (it2->first).second, pos);
						if (tmp != ""){
							res = tmp;
							tmp_timestamp = (it2->first).first;
						}
					}	
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
			std::string file = filePath(it->first, (it2->first).first, (it2->first).second);
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
	delete vLog;
	vLog = new VLog(VLOG_PATH);
	if (cacheSS != nullptr){
		delete cacheSS;
	}
}

struct LTTP{
	uint64_t level;
	uint64_t timestamp;
	uint64_t tag;
	int pos;
};

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */

// std::map<uint64_t, std::map<std::pair<uint64_t, uint64_t>, Info>> ssInfo; // level, {timestamp, tag}, Info
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list)
{
	std::map<uint64_t, std::string> res;
	std::map<uint64_t, LTTP> record;
	for (auto it = ssInfo.begin(); it != ssInfo.end(); it++){
		for (auto it2 = it->second.begin(); it2 != it->second.end(); it2++){
			auto info = it2->second;
			if (info.header.areaNotCross(key1, key2)){
				continue;
			}
			for (int i = 0; i < info.indexes.size(); i++){
				uint64_t key = info.indexes[i];
				if (key >= key1 && key <= key2){
					if (record.find(key) == record.end() || (it2->first).first > record[key].timestamp){
						record[key] = {it->first, (it2->first).first, (it2->first).second, i};
					}
				}
			}
		}
	}
	for (auto it = record.begin(); it != record.end(); it++){
		uint64_t key = it->first;
		uint64_t level = it->second.level;
		uint64_t timestamp = it->second.timestamp;
		uint64_t tag = it->second.tag;
		int pos = it->second.pos;
		std::string tmp = readData(level, timestamp, tag, pos);
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
		std::string valueInMem = memtable->get(key);
		if (offInSS == offset && valueInMem == ""){
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
						tmp = readOffset(it->first, (it2->first).first, (it2->first).second, pos);
						tmp_timestamp = (it2->first).first;
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
	std::string path = filePath(0, TIMESTAMP, 1);
	ss.write2file(path);
	ssInfo[0][{TIMESTAMP, 1}] = ss.info;
	// if (ssInfo[0].size() > maxFileNumInLevel(0)){
	// 	compact();
	// }
	maxTag[TIMESTAMP] = 1;
	TIMESTAMP++;
	delete memtable;
	memtable = new SkipList();
}

std::string KVStore::readData (uint64_t level,
                               uint64_t time_stamp,
							   uint64_t tag,
                               uint64_t pos) {
	if (cacheSS != nullptr && cacheSS->level == level && cacheSS->time_stamp == time_stamp && cacheSS->tag == tag){
		return cacheSS->ssCache->data[pos].vlen == 0 ? DFLAG : vLog->get(cacheSS->ssCache->data[pos].offset, cacheSS->ssCache->data[pos].vlen);
	} else {
		if (cacheSS != nullptr){
			delete cacheSS;
		}
		std::string file = filePath(level, time_stamp, tag);
		SSTable* ss = new SSTable(file);
		cacheSS = new CacheSS(ss, level, time_stamp, tag);
	}
	
    std::string file = filePath(level, time_stamp, tag);
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
							   uint64_t tag,
                               uint64_t pos) {
    std::string file = filePath(level, time_stamp, tag);
    std::fstream f;
    f.open(file, std::ios::in | std::ios::binary);

    uint64_t offsetPos = pos * sizeData + sizeHeader + sizeFliter + sizeof(uint64_t);
	f.seekg(offsetPos, std::ios::beg);

    uint64_t offset;
    f.read(reinterpret_cast<char*>(&offset), sizeof(offset));

    return offset;
}

struct TTMM{
	std::pair<uint64_t, uint64_t> timeTag;
	uint64_t minKey;
	uint64_t maxKey;
};

void KVStore::compact(){
	int curLevel = 0;

	while(ssInfo[curLevel].size() > maxFileNumInLevel(curLevel)){
		std::vector<std::pair<uint64_t, uint64_t>> needToCompact;

		// 统计需要合并的文件的覆盖的key的范围
		uint64_t max = 0, min = UINT64_MAX;

		if (curLevel == 0){
			// 第0层需要合并所有文件
			for (auto it = ssInfo[0].begin(); it != ssInfo[0].end(); it++){
				auto info = it->second;
				if (info.header.minKey < min){
					min = info.header.minKey;
				}
				if (info.header.maxKey > max){
					max = info.header.maxKey;
				}
				needToCompact.push_back(it->first);
			}
		} else {
			// 在其他层，优先选择时间戳最小的若干文件（时间戳相等选择键最小的），使删除后的文件数满足maxFileNumInLevel(curLevel)
			int deleteNum = ssInfo[curLevel].size() - maxFileNumInLevel(curLevel);
			std::vector<TTMM> tmp;
			for (auto it = ssInfo[curLevel].begin(); it != ssInfo[curLevel].end(); it++){
				auto info = it->second;
				tmp.push_back({it->first, info.header.minKey, info.header.maxKey});
			}
			std::sort(tmp.begin(), tmp.end(), [](TTMM a, TTMM b){
				if (a.timeTag.first == b.timeTag.first){
					return a.minKey < b.minKey;
				}
				return a.timeTag.first < b.timeTag.first;
			});
			for (int i = 0; i < deleteNum; i++){
				needToCompact.push_back(tmp[i].timeTag);
				if (tmp[i].minKey < min){
					min = tmp[i].minKey;
				}
				if (tmp[i].maxKey > max){
					max = tmp[i].maxKey;
				}
			}
		}

		if (!utils::dirExists(dirName(curLevel + 1))){
			createLevel(curLevel + 1);
		}

		// 在下一层中找到与此区间有交集的ssTable文件
		std::vector<std::pair<uint64_t, uint64_t>> needToMerge;
		for (auto it = ssInfo[curLevel + 1].begin(); it != ssInfo[curLevel + 1].end(); it++){
			auto info = it->second;
			if (info.header.areaNotCross(min, max)){
				continue;
			}
			needToMerge.push_back(it->first);
		}

		printf("needToMerge.size() = %d\n", needToMerge.size());

		// 使用归并排序，将以上所有涉及到的ssTable文件进行合并，并将结果每16个kB(408个datablock)分成一个新的ssTable文件（最后一个可不足16kB），写入到下一层中
		// 合并时，新ssTable文件的时间戳为原ssTable文件的时间戳最大值,因此生成的多个ssTable时间戳可能相同，需要用tag区分
		std::vector<SSTable> sstables;
		for(auto it = needToMerge.begin(); it != needToMerge.end(); it++){
			SSTable ss = SSTable(dirName(curLevel + 1) + "/" + filePath(curLevel + 1, it->first, it->second));
			sstables.push_back(ss);
		}
		for (auto it = needToCompact.begin(); it != needToCompact.end(); it++){
			SSTable ss = SSTable(dirName(curLevel) + "/" + filePath(curLevel, it->first, it->second));
			sstables.push_back(ss);
		}

		printf("sstables.size() = %d\n", sstables.size());

		std::map<uint64_t, std::pair<uint64_t, DataBlock>> all; // key, {timestamp, DataBlock}
		for (auto it = sstables.begin(); it != sstables.end(); it++){
			uint64_t timestamp = it->info.header.timeStamp;
			for (auto it2 = it->data.begin(); it2 != it->data.end(); it2++){
				// 遇到相同key，选择时间戳最大的
				uint64_t key = it2->key;
				if (all.find(key) == all.end()){
					if (it2->vlen == 0){
						continue;
					}
					all[key] = {timestamp, *it2};
				} else if (timestamp > all[key].first){
					if (it2->vlen == 0){
						all.erase(key);
					} else {
						all[key] = {timestamp, *it2};
					}
				}
			}
		}

		printf("all.size() = %d\n", all.size());

		while (all.size() > 0){
			std::vector<DataBlock> data;
			uint64_t size = 0;
			auto it = all.begin();
			uint64_t minKey = it->first;
			uint64_t maxKey = it->first;
			uint64_t maxTimeStamp = it->second.first;
			for (; it != all.end(), size < DataBlockNum; it++, size++){
				data.push_back(it->second.second);
				maxKey = it->first;
				if (it->second.first > maxTimeStamp){
					maxTimeStamp = it->second.first;
				}
				all.erase(it);
			}
			SSTable ss = SSTable(maxTimeStamp, size, minKey, maxKey, data);
			uint64_t tag = maxTag[maxTimeStamp] + 1;
			std::string path = filePath(curLevel + 1, maxTimeStamp, tag);
			ss.write2file(path);
			ssInfo[curLevel + 1][{maxTimeStamp, tag}] = ss.info;
			maxTag[maxTimeStamp] = tag;
		}

		printf("all.size() = %d\n", all.size());

		// 完成合并后，删除原来的ssTable文件，更新ssInfo
		for (auto it = needToCompact.begin(); it != needToCompact.end(); it++){
			std::string file = filePath(curLevel, it->first, it->second);
			utils::rmfile(file);
			ssInfo[curLevel].erase(*it);
		}

		curLevel++;
	}
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

uint64_t KVStore::maxFileNumInLevel(uint64_t level){
	uint64_t res = 2;
	for (; level > 0; level--){
		res *= 2;
	}
	return res;
}

void KVStore::createLevel(uint64_t level){
	utils::_mkdir(dirName(level));
}

std::string KVStore::filePath(uint64_t level, uint64_t timestamp, uint64_t tag){
	return SS_PATH + std::to_string(level) + "/" + std::to_string(timestamp) + "_" + std::to_string(tag) +".sst";
}

std::string KVStore::dirName(uint64_t level){
	return SS_PATH + std::to_string(level);
}

void KVStore::getInfoInFileName(std::string fileName, uint64_t &timeStamp, uint64_t &tag){
	std::string tmp = fileName.substr(fileName.find_last_of("/") + 1);
	timeStamp = std::stoull(tmp.substr(0, tmp.find_first_of("_")));
	tag = std::stoull(tmp.substr(tmp.find_first_of("_") + 1, tmp.find_last_of(".")));
}