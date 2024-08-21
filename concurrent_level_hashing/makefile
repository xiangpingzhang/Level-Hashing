clevel: ycsb.o level_hashing.o hash.o
	cc -g -o clevel ycsb.o level_hashing.o hash.o -lm -lpthread

ycsb.o: ycsb.c level_hashing.h spinlock.h
	cc -g -c ycsb.c -lm

level_hashing.o : level_hashing.c level_hashing.h spinlock.h
	cc -g -c level_hashing.c -lm

hash.o : hash.c hash.h
	cc -g -c hash.c -lm

clean:
	rm *.o clevel
