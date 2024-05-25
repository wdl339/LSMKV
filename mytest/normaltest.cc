#include <iostream>
#include <cstdint>
#include <string>
#include <assert.h>
#include <chrono>
#include <algorithm>
#include <fstream>

#include "../test.h"

class NormalTest : public Test
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

		// 将0至max-1的数字打乱
		std::vector<uint64_t> rand_seq(max);
		for (uint64_t i = 0; i < max; ++i)
			rand_seq[i] = i;
		std::random_shuffle(rand_seq.begin(), rand_seq.end());

		auto start_ran_get = std::chrono::high_resolution_clock::now();

		for (uint64_t i = 0; i < max; ++i)
			store.get(rand_seq[i]);

		auto end_ran_get = std::chrono::high_resolution_clock::now();
		std::chrono::duration<double> diff_ran_get = end_ran_get - start_ran_get;

		auto start_scan = std::chrono::high_resolution_clock::now();

		std::list<std::pair<uint64_t, std::string>> list;
		store.scan(max / 2, max / 2 + 9, list);
		store.scan(max / 2 - 10, max / 2 - 1, list);

		auto end_scan = std::chrono::high_resolution_clock::now();
		std::chrono::duration<double> diff_scan = end_scan - start_scan;
		
		auto start_del = std::chrono::high_resolution_clock::now();

		for (i = 0; i < max; ++i)
			store.del(i);

		auto end_del = std::chrono::high_resolution_clock::now();
		std::chrono::duration<double> diff_del = end_del - start_del;

		// 将结果输出到文件中
		std::ofstream out("./mytest/normaltest.txt", std::ios::app);
		out << "Max: " << max << std::endl;
		out << "Put Throughput: " << max / diff_put.count() << " ops per second" << std::endl;
		out << "Seq Get Throughput: " << max / diff_get.count() << " ops per second" << std::endl;
		out << "Random Get Throughput: " << max / diff_ran_get.count() << " ops per second" << std::endl;
		out << "Del Throughput: " << max / diff_del.count() << " ops per second" << std::endl;
		out << "Scan Throughput: " << 2 / diff_scan.count() << " ops per second" << std::endl;
		out << "\n";
		out << "Put Latency: " << diff_put.count() / max * 1000000 << " us per op" << std::endl;
		out << "Seq Get Latency: " << diff_get.count() / max * 1000000 << " us per op" << std::endl;
		out << "Random Get Latency: " << diff_ran_get.count() / max * 1000000 << " us per op" << std::endl;
		out << "Del Latency: " << diff_del.count() / max * 1000000 << " us per op" << std::endl;
		out << "Scan Latency: " << diff_scan.count() / 2 * 1000000 << " us per op" << std::endl;
		out << "\n";
		out.close();
	}


public:
	NormalTest(const std::string &dir, const std::string &vlog, bool v = true) : Test(dir, vlog, v){}

	void start_test(void *args = NULL) override
	{
		std::cout << "KVStore Normal Test" << std::endl;

		store.reset();

		uint64_t test[4] = {1024 * 2, 1024 * 8, 1024 * 32};

		for (uint64_t i = 0; i < 3; ++i){
			uint64_t max = test[i];

			regular_test(max);
			store.reset();
		}

	}
};

int main(int argc, char *argv[])
{
	bool verbose = (argc == 2 && std::string(argv[1]) == "-v");

	std::cout << "Usage: " << argv[0] << std::endl;

	NormalTest test("./data", "./data/vlog", verbose);

	test.start_test();

	return 0;
}
