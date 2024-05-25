
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++2a -Wall -Ofast

all: correctness persistence

correctness: kvstore.o correctness.o skiplist.o sstable.o vlog.o

persistence: kvstore.o persistence.o skiplist.o sstable.o vlog.o

clean:
	-rm -f correctness persistence *.o
	-rm -f ./data/level-*/* ./data/vlog
	-rm -rf ./data/level-*

data_clean:
	-rm -f ./data/level-*/* ./data/vlog
	-rm -rf ./data/level-*



normaltest: kvstore.o ./mytest/normaltest.o skiplist.o sstable.o vlog.o
	g++ -std=c++2a -Wall -Ofast -o ./mytest/normaltest kvstore.o ./mytest/normaltest.o skiplist.o sstable.o vlog.o

cachetest: kvstore.o ./mytest/cachetest.o skiplist.o sstable.o vlog.o
	g++ -std=c++2a -Wall -Ofast -o ./mytest/cachetest kvstore.o ./mytest/cachetest.o skiplist.o sstable.o vlog.o

compacttest: kvstore.o ./mytest/compacttest.o skiplist.o sstable.o vlog.o
	g++ -std=c++2a -Wall -Ofast -o ./mytest/compacttest kvstore.o ./mytest/compacttest.o skiplist.o sstable.o vlog.o

flitertest: kvstore.o ./mytest/flitertest.o skiplist.o sstable.o vlog.o
	g++ -std=c++2a -Wall -Ofast -o ./mytest/flitertest kvstore.o ./mytest/flitertest.o skiplist.o sstable.o vlog.o

mytest_clean:
	-rm -f ./mytest/normaltest ./mytest/cachetest ./mytest/compacttest ./mytest/flitertest ./mytest/*.o *.o ./mytest/*.txt
	-rm -f ./data/level-*/* ./data/vlog
	-rm -rf ./data/level-*