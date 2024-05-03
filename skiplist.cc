#include "skiplist.h"
#include <optional>
#include <limits>
#include <random>
#include <ctime>
#include <map>

    Node::Node(int lvl, key_type k, value_type v) {
		key = k;
		value = v;
		forward = new Node*[lvl];
		for (int i = 0; i < lvl; i++){
			forward[i] = nullptr;
		}
	}

    SkipList::SkipList(double pr) {
        srand(time(NULL)); 
        p = pr;
        level = 1;
        maxLevel = 20;
        header = new Node (maxLevel, std::numeric_limits<key_type>::min(), "");
        tailer = new Node (maxLevel, std::numeric_limits<key_type>::max(), "");
        for (int i = 0; i < maxLevel; i++){
                header->forward[i] = tailer;
        }
        number = 0;
    }

    int SkipList::random_level(){
        int lvl = 1;
        while ((double)(static_cast<double>(rand()) / RAND_MAX) < p && lvl < maxLevel){
            lvl++;
        }
        return lvl;
    }

    bool SkipList::put(key_type key, const value_type &val) {
        Node* update[maxLevel];
        Node* x = header;
        for (int i = level - 1; i >= 0; i--){
            while (x->forward[i] && x->forward[i]->key < key){
                x = x->forward[i];
            }
            update[i] = x;
        }
        x = x->forward[0];
        if (x->key == key)
            x->value = val;
        else {
            int lvl = random_level();
            if (lvl > level){
                for (int i = level; i < lvl; i++)
                    update[i] = header;
                level = lvl;
            }
            x = new Node(lvl, key, val);
            for (int i = 0; i < lvl; i++){
                x->forward[i] = update[i]->forward[i];
                update[i]->forward[i] = x;
            }
            number++;
            if (number >= MAX_SIZE)
                return false;
        }
        return true;
    }

    std::string SkipList::get(key_type key) const {
        Node* x = header;
        for (int i = level - 1; i >= 0; i--){
            while (x->forward[i] && x->forward[i]->key < key){
                x = x->forward[i];
            }
        }
        x = x->forward[0];
        if (x->key == key)
            return x->value;
        else
            return "";
    }

    void SkipList::scan(key_type key1, key_type key2, std::map<uint64_t, std::string> &res){
        Node* x = header;
        for (int i = level - 1; i >= 0; i--){
            while (x->forward[i] && x->forward[i]->key < key1){
                x = x->forward[i];
            }
        }
        x = x->forward[0];
        while (x->key <= key2){
            res[x->key] = x->value;
            x = x->forward[0];
        }
    }

    void SkipList::getAll(std::map<uint64_t, std::string> &res){
        Node* x = header->forward[0];
        while (x != tailer){
            res[x->key] = x->value;
            x = x->forward[0];
        }
    }
