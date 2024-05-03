#include "vlog.h"

Entry::Entry(uint64_t k, std::string val) {
        key = k;
        value = val;
        vlen = val.size();
        magic = MAGIC;

        // 得到由key,vlen,value拼接而成的二进制序列data
        std::vector<unsigned char> data = std::vector<unsigned char>(sizeof(key) + sizeof(vlen) + vlen);
        memcpy(data.data(), &key, sizeof(key));
        memcpy(data.data() + sizeof(key), &vlen, sizeof(vlen));
        memcpy(data.data() + sizeof(key) + sizeof(vlen), value.c_str(), vlen);
        checkSum = utils::crc16(data);
    }

VLog::VLog(const std::string &dir) {
        this->dir = dir;
        int fd = open(dir.c_str(), O_RDWR | O_CREAT, 0644);
        if (fd < 0)
        {
            perror("open");
            return;
        }

        tail = utils::seek_data_block(dir);
        
        uint8_t buffer;
        while (true)
        {
            ssize_t bytesRead = read(fd, &buffer, 1);
            if (bytesRead < 0) {
                perror("read");
                break;
            } else if (bytesRead == 0) {
                tail = lseek(fd, 0, SEEK_CUR);
                break;
            }
            
            if (buffer == MAGIC) {
                tail = lseek(fd, -1, SEEK_CUR);
                
            }
            
        }
        head = lseek(fd, 0, SEEK_END);
        close(fd);
    }

void VLog::append(std::map<uint64_t, std::string> all, std::map<uint64_t, uint64_t> &offsets){
        int fd = open(dir.c_str(), O_RDWR | O_CREAT, 0644);
        if (fd < 0)
        {
            perror("open");
            return;
        }

        lseek(fd, head, SEEK_SET);
        for (auto &kv : all)
        {
            if (kv.second == DFLAG)
                continue;
            Entry entry(kv.first, kv.second);
            uint64_t size = sizeof(uint8_t) + sizeof(uint16_t) + sizeof(uint64_t) + sizeof(uint32_t) + entry.vlen;
            write(fd, &entry.magic, sizeof(uint8_t));
            write(fd, &entry.checkSum, sizeof(uint16_t));
            write(fd, &entry.key, sizeof(uint64_t));
            write(fd, &entry.vlen, sizeof(uint32_t));
            write(fd, entry.value.c_str(), entry.vlen);
            offsets[kv.first] = head;
            head += size;
        }
        close(fd);
}

std::string VLog::checkCrc(uint64_t offset, uint32_t vlen, int fd, bool &flag){
        uint8_t magic;
        read(fd, &magic, sizeof(magic));
        if (magic != MAGIC) {
            flag = false;
            return "222222";
        }

        uint16_t checkSum;
        read(fd, &checkSum, sizeof(checkSum));

        uint64_t key;
        read(fd, &key, sizeof(key));
        uint32_t len;
        read(fd, &len, sizeof(len));

        std::string value = std::string(len, 0);
        read(fd, &value[0], len);
        
        std::vector<unsigned char> data = std::vector<unsigned char>(sizeof(key) + sizeof(len) + len);
        memcpy(data.data(), &key, sizeof(key));
        memcpy(data.data() + sizeof(key), &len, sizeof(len));
        memcpy(data.data() + sizeof(key) + sizeof(len), value.c_str(), len);
        uint16_t crc = utils::crc16(data);

        if (crc != checkSum) {
            flag = false;
            return "111111";
        }

        flag = true;
        return value;
}

std::string VLog::get(uint64_t offset, uint32_t vlen){
        if (vlen == 0)
            return "";

        int fd = open(dir.c_str(), O_RDWR, 0644);
        if (fd < 0)
        {
            perror("open");
            return "";
        }

        lseek(fd, offset, SEEK_SET);
        
        bool flag = false;
        std::string value = checkCrc(offset, vlen, fd, flag);

        close(fd);
        return value;
}