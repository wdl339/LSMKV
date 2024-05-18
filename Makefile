
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++2a -Wall -g

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
