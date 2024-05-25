#include <iostream>
#include <cstdint>
#include <string>
#include <assert.h>
#include <chrono>
#include <algorithm>
#include <fstream>

#include "../test.h"

class CacheTest : public Test
{
private:
	void seq_test(uint64_t max)
	{
		uint64_t i;

		for (i = 0; i < max; ++i)
			store.put(i, std::string(i + 1, 's'));

		auto start_get = std::chrono::high_resolution_clock::now();

		for (i = 0; i < max; ++i)
			store.get(i);
		
		auto end_get = std::chrono::high_resolution_clock::now();
		std::chrono::duration<double> diff_get = end_get - start_get;


		// 将结果输出到文件中
		std::ofstream out("./mytest/CacheTest.txt", std::ios::app);
		out << "Max: " << max << std::endl;
		out << "Seq Get Latency: " << diff_get.count() / max * 1000000 << " us per op" << std::endl;
		out.close();
	}

	void rand_test(uint64_t max)
	{
		// 将0至max-1的数字打乱
		std::vector<uint64_t> rand_seq(max);
		for (uint64_t i = 0; i < max; ++i)
			rand_seq[i] = i;
		std::random_shuffle(rand_seq.begin(), rand_seq.end());

		for (uint64_t i = 0; i < max; ++i)
			store.put(i, std::string(i, 's'));

		auto start_get = std::chrono::high_resolution_clock::now();

		for (uint64_t i = 0; i < max; ++i)
			store.get(rand_seq[i]);

		auto end_get = std::chrono::high_resolution_clock::now();
		std::chrono::duration<double> diff_get = end_get - start_get;

		// 将结果输出到文件中
		std::ofstream out("./mytest/CacheTest.txt", std::ios::app);
		out << "Max: " << max << std::endl;
		out << "Random Get Latency: " << diff_get.count() / max * 1000000 << " us per op" << std::endl;
		out << "\n\n";
		out.close();

	}

public:
	CacheTest(const std::string &dir, const std::string &vlog, bool v = true) : Test(dir, vlog, v){}

	void start_test(void *args = NULL) override
	{
		std::cout << "KVStore Cache Test" << std::endl;

		store.reset();

		seq_test(1024 * 8);
		store.reset();

		rand_test(1024 * 8);
		store.reset();
	}
};

int main(int argc, char *argv[])
{
	bool verbose = (argc == 2 && std::string(argv[1]) == "-v");

	std::cout << "Usage: " << argv[0] << std::endl;

	CacheTest test("./data", "./data/vlog", verbose);

	test.start_test();

	return 0;
}
