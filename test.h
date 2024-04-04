#pragma once

#include <iostream>
#include <cstdint>
#include <string>

#include "kvstore.h"
#include "utils.h"

#define MB (1024 * 1024)

class Test
{
protected:
	static const std::string not_found;

	const std::string vlog;

	uint64_t nr_tests;
	uint64_t nr_passed_tests;
	uint64_t nr_phases;
	uint64_t nr_passed_phases;

#define EXPECT(exp, got) expect<decltype(got)>((exp), (got), __FILE__, __LINE__)
	template <typename T>
	void expect(const T &exp, const T &got,
				const std::string &file, int line)
	{
		++nr_tests;
		if (exp == got)
		{
			++nr_passed_tests;
			return;
		}
		if (verbose)
		{
			std::cerr << "TEST Error @" << file << ":" << line;
			std::cerr << ", expected " << exp;
			std::cerr << ", got " << got << std::endl;
		}
	}

#define GC_EXPECT(cur, last, size) gc_expect<decltype(last)>((cur), (last), (size), __FILE__, __LINE__)
	template <typename T>
	void gc_expect(const T &cur, const T &last, const T &size,
				   const std::string &file, int line)
	{
		++nr_tests;
		if (cur >= last + size)
		{
			++nr_passed_tests;
			return;
		}
		if (verbose)
		{
			std::cerr << "TEST Error @" << file << ":" << line;
			std::cerr << ", current offset " << cur;
			std::cerr << ", last offset " << last << std::endl;
		}
	}

	void check_gc(uint64_t size)
	{
		uint64_t last_offset, cur_offset;
		last_offset = utils::seek_data_block(vlog.c_str());
		store.gc(size);
		cur_offset = utils::seek_data_block(vlog.c_str());
		GC_EXPECT(cur_offset, last_offset, size);
	}

	void phase(void)
	{
		// Report
		std::cout << "  Phase " << (nr_phases + 1) << ": ";
		std::cout << nr_passed_tests << "/" << nr_tests << " ";

		// Count
		++nr_phases;
		if (nr_tests == nr_passed_tests)
		{
			++nr_passed_phases;
			std::cout << "[PASS]" << std::endl;
		}
		else
			std::cout << "[FAIL]" << std::endl;

		std::cout.flush();

		// Reset
		nr_tests = 0;
		nr_passed_tests = 0;
	}

	void report(void)
	{
		std::cout << nr_passed_phases << "/" << nr_phases << " passed.";
		std::cout << std::endl;
		std::cout.flush();

		nr_phases = 0;
		nr_passed_phases = 0;
	}

	class KVStore store;
	bool verbose;

public:
	Test(const std::string &dir, const std::string &vlog, bool v = true) : vlog(vlog), store(dir, vlog), verbose(v)
	{
		nr_tests = 0;
		nr_passed_tests = 0;
		nr_phases = 0;
		nr_passed_phases = 0;
	}

	virtual void start_test(void *args = NULL)
	{
		std::cout << "No test is implemented." << std::endl;
	}
};
const std::string Test::not_found = "";
