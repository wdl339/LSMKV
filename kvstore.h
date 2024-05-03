#pragma once

#include "kvstore_api.h"
#include "skiplist.h"
#include "sstable.h"
#include "vlog.h"

class KVStore : public KVStoreAPI
{

private:

	uint64_t TIMESTAMP = 1;
	std::string DFLAG = "~DELETED~";
	uint64_t sizeHeader = 32;
	uint64_t sizeData = sizeof(uint64_t) * 2 + sizeof(uint32_t); 
	uint64_t sizeFliter = 1024 * 8;

	std::map<uint64_t, std::map<uint64_t, Info>> ssInfo; // level, timestamp, Info
	SkipList* memtable;
	VLog* vLog;
	std::string DIR_PATH;
	std::string SS_PATH;
	std::string VLOG_PATH;

	std::string filePath(uint64_t level, uint64_t timestamp);
	std::string dirName(uint64_t level);

public:
	KVStore(const std::string &dir, const std::string &vlog);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) override;

	void gc(uint64_t chunk_size) override;

	void pushMem2ss();

	void write2ss(std::map<uint64_t, std::string> &all, std::map<uint64_t, uint64_t> &offsets);

	void createLevel(uint64_t level);

	std::string readData(uint64_t level, uint64_t time_stamp, uint64_t pos);

	uint64_t getOffInSS(uint64_t key);

	uint64_t readOffset(uint64_t level, uint64_t time_stamp, uint64_t pos);

};
