#include <iostream>
#include <cstdint>
#include <string>
#include <assert.h>
#include <chrono>
#include <algorithm>
#include <fstream>

#include "../test.h"

class CompactTest : public Test
{
private:
	void seq_test(uint64_t max)
	{
		uint64_t i;
        std::ofstream out("./mytest/CompactTest.txt", std::ios::app);

        for (i = 0; i < max; ++i){
            auto start_put = std::chrono::high_resolution_clock::now();

            store.put(i, "ssss");

            auto end_put = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double> diff_put = end_put - start_put;

            out << diff_put.count() * 1000000 << std::endl;
        }
	}


public:
	CompactTest(const std::string &dir, const std::string &vlog, bool v = true) : Test(dir, vlog, v){}

	void start_test(void *args = NULL) override
	{
		std::cout << "KVStore Compact Test" << std::endl;

		store.reset();

		seq_test(1024 * 16);
		// store.reset();

	}
};

int main(int argc, char *argv[])
{
	bool verbose = (argc == 2 && std::string(argv[1]) == "-v");

	std::cout << "Usage: " << argv[0] << std::endl;

	CompactTest test("./data", "./data/vlog", verbose);

	test.start_test();

	return 0;
}
