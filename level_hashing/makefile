level: test.o level_hashing.o hash.o
	cc -O3 -g -o level test.o level_hashing.o hash.o -lm

test.o: test.c level_hashing.h
	cc -O3 -g -c test.c -lm
level_hashing.o : level_hashing.c level_hashing.h
	cc -O3 -g -c level_hashing.c -lm
hash.o : hash.c hash.h
	cc -O3 -g -c -g hash.c -lm

clean:
	rm *.o level
