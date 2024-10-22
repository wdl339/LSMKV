#ifndef SKIPLIST_H
#define SKIPLIST_H

#include <cstdint>
// #include <optional>
// #include <vector>
#include <string>
#include <map>

using key_type = uint64_t;
using value_type = std::string;

class SkipList;

class Node
{
	friend SkipList;

	key_type key;
	value_type value;
	Node** forward;

public:

	explicit Node(int lvl, key_type k, value_type v);

	~Node(){
		delete[] forward;
	}

};

class SkipList
{
	std::string DFLAG = "~DELETED~";
	int level;
	int maxLevel;
	Node* header;
	Node* tailer;
	double p;

	uint64_t number;
	uint64_t MAX_SIZE = 408;

	int random_level();

public:

	explicit SkipList(double pr = 0.5);
	bool put(key_type key, const value_type &val);
	std::string get(key_type key) const;
	void scan(key_type key1, key_type key2, std::map<uint64_t, std::string> &res);
	void getAll(std::map<uint64_t, std::string> &res);

	uint64_t size() {
		return number;
	}

	key_type getMinKey() {
		return header->forward[0]->key;
	}

	key_type getMaxKey(){
		Node* x = header;
		while (x->forward[0] != tailer){
			x = x->forward[0];
		}
		return x->key;
	}

	bool areaNotCross(uint64_t min, uint64_t max) {
        return getMinKey() >= max || getMaxKey() <= min;
    }

};

#endif // SKIPLIST_H
