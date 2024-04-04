#include <iostream>
#include <cstdint>
#include <string>
#include <cassert>
#include <semaphore.h>
#include <random>
#include <signal.h>

#include "test.h"

class PersistenceTest : public Test
{
private:
	const uint64_t TEST_MAX = 1024 * 32;
	const uint64_t GC_TRIGGER = 1024;

public:
	void prepare()
	{
		std::cout << "KVStore Persistence Test" << std::endl;
		std::cout << "<<Preparation Mode>>" << std::endl;
		uint64_t i;

		// Clean up
		store.reset();

		// Test multiple key-value pairs
		for (i = 0; i < TEST_MAX; ++i)
		{
			store.put(i, std::string(i + 1, 's'));
			EXPECT(std::string(i + 1, 's'), store.get(i));
		}
		phase();

		// Test after all insertions
		for (i = 0; i < TEST_MAX; ++i)
			EXPECT(std::string(i + 1, 's'), store.get(i));
		phase();

		// Test deletions
		for (i = 0; i < TEST_MAX; i += 2)
		{
			EXPECT(true, store.del(i));

			if ((i / 2) % GC_TRIGGER == 0) [[unlikely]]
			{
				check_gc(16 * MB);
			}
		}

		// Prepare data for Test Mode
		for (i = 0; i < TEST_MAX; ++i)
		{
			switch (i & 3)
			{
			case 0:
				EXPECT(not_found, store.get(i));
				store.put(i, std::string(i + 1, 't'));
				break;
			case 1:
				EXPECT(std::string(i + 1, 's'), store.get(i));
				store.put(i, std::string(i + 1, 't'));
				break;
			case 2:
				EXPECT(not_found, store.get(i));
				break;
			case 3:
				EXPECT(std::string(i + 1, 's'), store.get(i));
				break;
			default:
				assert(0);
			}

			if (i % GC_TRIGGER == 0) [[unlikely]]
			{
				check_gc(8 * MB);
			}
		}

		check_gc(32 * MB);

		phase();

		report();
	}

	void loop()
	{
		std::cout << "Data is ready, start looping and wait to be terminated!" << std::endl;
		std::cout.flush();
		while (true)
		{
			volatile int dummy;
			for (uint64_t i = 0; i <= 1024; ++i)
			{
				// The loop slows down the program
				for (i = 0; i <= 1000; ++i)
					dummy = i;

				store.del(TEST_MAX + i);

				for (i = 0; i <= 1000; ++i)
					dummy = i;

				store.put(TEST_MAX + i, std::string(1024, '.'));

				for (i = 0; i <= 1000; ++i)
					dummy = i;

				store.put(TEST_MAX + i, std::string(512, 'x'));
			}
		}
	}

	void test()
	{
		std::cout << "KVStore Persistence Test" << std::endl;
		std::cout << "<<Test Mode>>" << std::endl;
		uint64_t i;
		// Test data
		for (i = 0; i < TEST_MAX; ++i)
		{
			if (i % GC_TRIGGER == 0) [[unlikely]]
			{
				check_gc(16 * MB);
			}
			switch (i & 3)
			{
			case 0:
				EXPECT(std::string(i + 1, 't'), store.get(i));
				break;
			case 1:
				EXPECT(std::string(i + 1, 't'), store.get(i));
				break;
			case 2:
				EXPECT(not_found, store.get(i));
				break;
			case 3:
				EXPECT(std::string(i + 1, 's'), store.get(i));
				break;
			default:
				assert(0);
			}
		}

		phase();

		report();
	}

	PersistenceTest(const std::string &dir, const std::string &vlog, bool v = true) : Test(dir, vlog, v)
	{
	}
};

void sigusr1Handler(int sig)
{
}

int main(int argc, char *argv[])
{
	bool verbose = (argc == 2 && std::string(argv[1]) == "-v");

	std::cout << "Usage: " << argv[0] << " [-v]" << std::endl;
	std::cout << "  -v: print extra info for failed tests [currently ";
	std::cout << (verbose ? "ON" : "OFF") << "]" << std::endl;
	std::cout << std::endl;
	std::cout.flush();

	if (signal(SIGUSR1, sigusr1Handler) == SIG_ERR)
	{
		perror("signalhandler");
		return -1;
	}

	pid_t pid = fork();

	if (pid == 0)
	{
		PersistenceTest test("./data", "./data/vlog", verbose);

		// The child process first prepared data for testing
		test.prepare();

		// Wake up parent process to randomly terminate child process
		kill(getppid(), SIGUSR1);

		test.loop();
	}
	else if (pid > 0)
	{
		pause();

		// After being waken up, wait random time to kill child process
		std::random_device rd;
		std::mt19937 gen(rd());
		std::uniform_int_distribution<int> dis(1000, 5000);
		int wait_time = dis(gen);
		usleep(1000 * wait_time);
		kill(pid, SIGINT);
		printf("Killing loop after %d ms.\n", wait_time);
		std::cout << std::endl;

		PersistenceTest test("./data", "./data/vlog", verbose);

		// test for data integrity
		test.test();
	}
	else
	{
		perror("fork");
		return -1;
	}

	return 0;
}
