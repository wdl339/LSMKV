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
            
            if (buffer == 0xff) {
                // 进行crc校验，如果通过则magic位置为tail位置，否则从现在的magic开始寻找下一个magic，直到校验通过
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

                tail = lseek(fd, -sizeof(uint16_t) - sizeof(uint64_t) - sizeof(uint32_t) - len, SEEK_CUR);
                if (crc == checkSum) {
                    break;
                }
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

std::string VLog::get(uint64_t offset, uint32_t vlen){
        int fd = open(dir.c_str(), O_RDWR, 0644);
        if (fd < 0)
        {
            perror("open");
            return "";
        }

        lseek(fd, offset, SEEK_SET);
        
        uint8_t magic;
        read(fd, &magic, sizeof(magic));
        if (magic != MAGIC) {
            close(fd);
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
            close(fd);
            return std::to_string(magic) + " " + std::to_string(checkSum) + " " + std::to_string(key) + " " + std::to_string(len) + " " + value;
        }


        close(fd);
        return value;
}