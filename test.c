#include "hiredis.h"
#include "ccredis.h"
#include <stdio.h>
#include <assert.h>
#include <string.h>

#define IP "218.205.81.12"
#define PORT 6379
#define TIMEOUT 2


static void test(){
    struct redisClient* c = createRedisClnt(IP, PORT, TIMEOUT);
	assert(c);
	int ret;
    int rv = redisSet(c, "test1", "value1", NULL);
	assert(rv == CC_SUCCESS);

	char val[32];
	rv = redisGet(c,"test1",val,32,NULL);
	if(rv != CC_SUCCESS || 0!=strcmp("value1", val)){
        printf("\n set/get fail!");
	}
	deleteRedisClnt(c);
}

int main(int argc, char **argv){
    //struct redisClient* c = createRedisClnt(IP, PORT, TIMEOUT);
	test();
	printf("\n yes!!!!");
    return 0;
}
