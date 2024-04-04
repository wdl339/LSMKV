
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++2a -Wall

all: correctness persistence

correctness: kvstore.o correctness.o skiplist.o

persistence: kvstore.o persistence.o skiplist.o

clean:
	-rm -f correctness persistence *.o
