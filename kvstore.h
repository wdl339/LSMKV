#pragma once

#include "kvstore_api.h"
#include "skiplist.h"
#include "sstable.h"
#include "vlog.h"

class KVStore : public KVStoreAPI
{

private:
	struct LTT
	{
		uint64_t level;
		uint64_t timestamp;
		uint64_t tag;
		LTT(uint64_t l, uint64_t t, uint64_t g) : level(l), timestamp(t), tag(g) {}
	};

	uint64_t TIMESTAMP = 1;
	std::string DFLAG = "~DELETED~";
	uint64_t sizeHeader = 32;
	uint64_t sizeData = sizeof(uint64_t) * 2 + sizeof(uint32_t); 
	uint64_t sizeFliter = 8192;
	uint64_t DataBlockNum = 408;

	std::map<uint64_t, std::map<std::pair<uint64_t, uint64_t>, Info>> ssInfo; // level, {timestamp, tag}, Info
																			  // 只缓存ssTable的基本信息，offset和vlen仍需要去磁盘读文件
	std::map<uint64_t, uint64_t> maxTag; // 文件以timestamp_tag命名，维护timestamp的最大tag，避免重名

	SkipList* memtable; // 内存MemTable
	VLog* vLog; // vLog

	SSTable ssCache; // 缓存一个完整的ssTable，减少读文件
	LTT* ltt; // 缓存的这个ssTable的level, timestamp, tag

	uint64_t curLevel; // 当前合并所在的level，以防在合并时打断
	std::map<uint64_t, std::pair<uint64_t, DataBlock>> compactCache; // key, {timestamp, DataBlock}
																	 // 缓存所有需要合并的ssTable的DataBlock，以防在合并时打断

	std::string DIR_PATH; // 存放ssTable的总目录
	std::string SS_PATH; // 存放ssTable的level目录(level- 不带数字)
	std::string VLOG_PATH; // vlog的路径

	void pushMem2ss(); // 将MemTable中的数据写入ssTable
	void write2ss(std::map<uint64_t, std::string> &all, std::map<uint64_t, uint64_t> &offsets); // 将数据写入ssTable文件

	std::string readData(uint64_t level, uint64_t time_stamp, uint64_t tag, uint64_t pos); // 根据key在ssTable中的pos，读取value数据
	std::string readDataForNoCache(uint64_t level, uint64_t time_stamp, uint64_t tag, uint64_t key); // 不使用缓存的读取value数据（测试用）

	uint64_t getOffInSS(uint64_t key); // 从ssTable中读取key的offset（gc用，判断是否是最新数据）
	uint64_t readOffset(uint64_t level, uint64_t time_stamp, uint64_t tag, uint64_t pos); // 根据key在ssTable中的pos，读取offset

	void compact(); // 合并ssTable

	uint64_t maxFileNumInLevel(uint64_t level); // 返回level层最大的文件数
	void createLevel(uint64_t level); // 创建level层的目录
	std::string filePath(uint64_t level, uint64_t timestamp, uint64_t tag); // 返回ssTable文件的路径
	std::string dirName(uint64_t level); // 返回level层的目录名
	void getInfoInFileName(std::string fileName, uint64_t &timeStamp, uint64_t &tag); // 从文件名中解析出timestamp和tag
	uint64_t getMaxLevel(); // 返回当前最大的level（在最后一层合并时，所有vlen为0的记录都被丢弃）

public:
	KVStore(const std::string &dir, const std::string &vlog);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) override;

	void gc(uint64_t chunk_size) override;

};
