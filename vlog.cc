#include "vlog.h"

uint16_t getCrc(uint64_t key, std::string val, uint32_t vlen){
        std::vector<unsigned char> data = std::vector<unsigned char>(sizeof(key) + sizeof(vlen) + vlen);
        memcpy(data.data(), &key, sizeof(key));
        memcpy(data.data() + sizeof(key), &vlen, sizeof(vlen));
        memcpy(data.data() + sizeof(key) + sizeof(vlen), val.c_str(), vlen);
        return utils::crc16(data);
}

void readCKL(int fd, uint16_t &checkSum, uint64_t &key, uint32_t &len){
    if (read(fd, &checkSum, sizeof(checkSum)) < 0){
        perror("read checkSum");
    }
    if (read(fd, &key, sizeof(key)) < 0){
        perror("read key");
    }
    if (read(fd, &len, sizeof(len)) < 0){
        perror("read len");
    }
}

void readV(int fd, std::string &value, uint32_t len){
    if (read(fd, &value[0], len) < 0){
        perror("read value");
    }
}

void readMagic(int fd, uint8_t &magic){
    if (read(fd, &magic, sizeof(magic)) < 0){
        perror("read magic");
    }
}

Entry::Entry(uint64_t k, std::string val) {
        key = k;
        value = val;
        vlen = val.size();
        magic = MAGIC;

        checkSum = getCrc(key, value, vlen);
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
                uint64_t key;
                uint32_t len;
                readCKL(fd, checkSum, key, len);
                std::string value = std::string(len, 0);
                readV(fd, value, len);
                uint16_t crc = getCrc(key, value, len);

                tail = lseek(fd, -sizeof(uint8_t)- sizeof(uint16_t) - sizeof(uint64_t) - sizeof(uint32_t) - len, SEEK_CUR);
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
            if (write(fd, &entry.magic, sizeof(uint8_t)) < 0){
                perror("write magic");
            }
            if (write(fd, &entry.checkSum, sizeof(uint16_t)) < 0){
                perror("write checkSum");
            }
            if (write(fd, &entry.key, sizeof(uint64_t)) < 0){
                perror("write key");
            }
            if (write(fd, &entry.vlen, sizeof(uint32_t)) < 0){
                perror("write vlen");
            }
            if (write(fd, entry.value.c_str(), entry.vlen) < 0){
                perror("write value");
            }
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
        readMagic(fd, magic);
        if (magic != MAGIC) {
            printf("magic: %d\n, offset: %ld\n, vlen: %d\n", magic, offset, vlen);
            close(fd);
            return "magic error";
        }

        uint16_t checkSum;
        uint64_t key;
        uint32_t len;
        readCKL(fd, checkSum, key, len);
        std::string value = std::string(len, 0);
        readV(fd, value, len);
        uint16_t crc = getCrc(key, value, len);

        if (crc != checkSum) {
            close(fd);
            return std::to_string(magic) + " " + std::to_string(checkSum) + " " + std::to_string(key) + " " + std::to_string(len) + " " + value;
        }

        close(fd);
        return value;
}

void VLog::gc(std::map<uint64_t, std::pair<std::string, uint64_t>> &kVOff, uint64_t chunk_size, uint64_t &holeOff, uint64_t &len){
        int fd = open(dir.c_str(), O_RDWR, 0644);
        if (fd < 0)
        {
            perror("open");
            return;
        }

        lseek(fd, tail, SEEK_SET);
        uint64_t offset = tail;
        uint64_t oldOffset = tail;
        uint64_t total_size = 0;
        while (total_size < chunk_size && offset < head)
        {
            oldOffset = offset;
            uint8_t magic;
            readMagic(fd, magic);
            if (magic != MAGIC) {
                break;
            }

            uint16_t checkSum;
            uint64_t key;
            uint32_t len;
            readCKL(fd, checkSum, key, len);
            std::string value = std::string(len, 0);
            readV(fd, value, len);
            uint16_t crc = getCrc(key, value, len);

            offset = lseek(fd, 0, SEEK_CUR);

            if (crc != checkSum) {
                continue;
            }

            total_size += (sizeof(uint8_t) + sizeof(uint16_t) + sizeof(uint64_t) + sizeof(uint32_t) + len);

            kVOff[key] = std::make_pair(value, oldOffset);
        }

        holeOff = tail;
        tail = offset;
        len = total_size;
        close(fd);
    }