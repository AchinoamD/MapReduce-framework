FLAGS = -std=c++11 -g -Wall -pthread

all: Search

Search: Search.o MapReduceFramework.a
	g++ ${FLAGS} -o Search Search.o MapReduceFramework.a

Search.o: Search.cpp
	g++ -c ${FLAGS} Search.cpp

MapReduceFramework.a: MapReduceFramework.o
	ar rcs MapReduceFramework.a MapReduceFramework.o

MapReduceFramework.o: MapReduceFramework.cpp
	g++ -c ${FLAGS} MapReduceFramework.cpp

tar: MapReduceFramework.cpp Search.cpp
	tar cfv ex3.tar MapReduceFramework.cpp Search.cpp Makefile README

clean:
	rm *.o *.a Search

.PHONY: clean
