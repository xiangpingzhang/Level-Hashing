level: test.o level_hashing.o hash.o
	cc -g -o level test.o level_hashing.o hash.o -lm -lnuma

test.o: test.c level_hashing.h
	cc -g -c test.c -lm -lnuma
level_hashing.o : level_hashing.c level_hashing.h
	cc -g -c level_hashing.c -lm -luma
hash.o : hash.c hash.h
	cc -g -c hash.c -lm -luma

clean:
	rm *.o level
