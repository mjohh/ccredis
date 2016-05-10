test:test.c ccredis.c
	gcc -g -Wall -gstabs+ -o $@ $^ -I./lib/ -L./lib/ -lhiredis -lpthread
