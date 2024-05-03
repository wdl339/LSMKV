#ifndef VLOG_H
#define VLOG_H

#include <cstdint>
#include <string>
#include "utils.h"
#include <map>

#define MAGIC 0xff

class Entry
{
    friend class VLog;

    uint8_t magic;
    uint16_t checkSum;
    uint64_t key;
    uint32_t vlen;
    std::string value;

public:
    Entry(uint64_t k, std::string val);

};

class VLog
{
    std::string dir;
    uint64_t tail;
    uint64_t head;
    std::string DFLAG = "~DELETED~";


public:
    VLog(const std::string &dir);
    void append(std::map<uint64_t, std::string> all, std::map<uint64_t, uint64_t> &offsets);
    std::string get(uint64_t offset, uint32_t vlen);
    
};

#endif // VLOG_H