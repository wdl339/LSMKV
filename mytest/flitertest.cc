#include <iostream>
#include <cstdint>
#include <string>
#include <assert.h>
#include <chrono>
#include <algorithm>
#include <fstream>

#include "../test.h"

class FliterTest : public Test
{
private:
	void regular_test(uint64_t max)
	{
		uint64_t i;

		auto start_put = std::chrono::high_resolution_clock::now();

		for (i = 0; i < max; ++i)
			store.put(i, "ssss");
		
		auto end_put = std::chrono::high_resolution_clock::now();
		std::chrono::duration<double> diff_put = end_put - start_put;

		auto start_get = std::chrono::high_resolution_clock::now();

		for (i = 0; i < max; ++i)
			store.get(i);
		
		auto end_get = std::chrono::high_resolution_clock::now();
		std::chrono::duration<double> diff_get = end_get - start_get;

		std::vector<uint64_t> rand_seq(max);
		for (uint64_t i = 0; i < max; ++i)
			rand_seq[i] = i;
		std::random_shuffle(rand_seq.begin(), rand_seq.end());

		auto start_ran_get = std::chrono::high_resolution_clock::now();

		for (uint64_t i = 0; i < max; ++i)
			store.get(rand_seq[i]);

		auto end_ran_get = std::chrono::high_resolution_clock::now();
		std::chrono::duration<double> diff_ran_get = end_ran_get - start_ran_get;


		// 将结果输出到文件中
		std::ofstream out("./mytest/FliterTest.txt", std::ios::app);
		out << "Max: " << max << std::endl;
		out << "Put Latency: " << diff_put.count() / max * 1000000 << " us per op" << std::endl;
		out << "Seq Get Latency: " << diff_get.count() / max * 1000000 << " us per op" << std::endl;
		out << "Random Get Latency: " << diff_ran_get.count() / max * 1000000 << " us per op" << std::endl;
		out.close();
	}

public:
	FliterTest(const std::string &dir, const std::string &vlog, bool v = true) : Test(dir, vlog, v){}

	void start_test(void *args = NULL) override
	{
		std::cout << "KVStore Fliter Test" << std::endl;

		store.reset();

		regular_test(1024 * 8);
	}
};

int main(int argc, char *argv[])
{
	bool verbose = (argc == 2 && std::string(argv[1]) == "-v");

	std::cout << "Usage: " << argv[0] << std::endl;

	FliterTest test("./data", "./data/vlog", verbose);

	test.start_test();

	return 0;
}
