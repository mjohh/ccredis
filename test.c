#include "hiredis.h"
#include "ccredis.h"
#include <stdio.h>
#include <assert.h>
#include <string.h>

#define IP "127.0.0.1"
#define PORT 6379
#define TIMEOUT 2

static int tests = 0, fails = 0;
#define test(_s) { printf("#%02d ", ++tests); printf(_s); }
#define test_cond(_c) if(_c) printf("\033[0;32mPASSED\033[0;0m\n"); else {printf("\033[0;31mFAILED\033[0;0m\n"); fails++;}


static void testmain(){
    struct redisClient* c = createRedisClnt(IP, PORT, TIMEOUT);
	assert(c);
	long ret;
	test("redisSet/redisGet ");
    int rv = redisSet(c, "k1", "v1", NULL);
	assert(rv == CC_SUCCESS);

	char val[32];
	rv = redisGet(c,"k1",val,32,NULL);
	test_cond(rv == CC_SUCCESS && 0==strcmp("v1", val));

	test("redisDel ");
	rv = redisDel(c, "k1", &ret, NULL);
    test_cond(rv == CC_SUCCESS && ret == 1);

	test("redisDel none exist key ");
	rv = redisDel(c, "k1", &ret, NULL);
    test_cond(rv == CC_SUCCESS && ret == 0);

	test("redisGet none exist key ");
	rv = redisGet(c,"k1",val,32,NULL);
    test_cond(rv == CC_SUCCESS && val[0] == 0);

    ////
	test("redisExpire none exist key ")
    rv = redisExpire(c, "k1", 10, &ret, NULL);
	test_cond(rv == CC_SUCCESS && ret == 0);

	test("redisGetSet none exit key ")
	char val2[32] = "v2";
	rv = redisGetset(c, "k1", val2, 32, NULL);
	test_cond(rv == CC_SUCCESS && val2[0] == 0);

	test("redisGetSet exit key ")
	char val3[32] = "v3";
	rv = redisGetset(c, "k1", val3, 32, NULL);
	test_cond(rv == CC_SUCCESS && 0==strcmp(val3, "v2"));

	test("redisExpire exist key ")
    rv = redisExpire(c, "k1", 100, &ret, NULL);
	test_cond(rv == CC_SUCCESS && ret == 1);

	test("redisIncr none integer type ")
    rv = redisIncr(c, "k1", &ret, NULL);
	test_cond(rv == CC_REPLY_ERR);

	test("redisIncr none exist key ")
    rv = redisIncr(c, "k2", &ret, NULL);
	test_cond(rv == CC_SUCCESS && ret == 1);
	// del, other this case will fail next time
	rv = redisDel(c, "k2", &ret, NULL);

    test("redisIncrby none exist key ")
	rv = redisIncrby(c, "k3", 20, &ret, NULL);
	test_cond(rv == CC_SUCCESS && ret==20);
    // del, other this case will fail next time
	rv = redisDel(c, "k3", &ret, NULL);

    ////
	test("redisDecr none exist key ")
    rv = redisDecr(c, "k2", &ret, NULL);
	test_cond(rv == CC_SUCCESS && ret == -1);
	// del, other this case will fail next time
	rv = redisDel(c, "k2", &ret, NULL);

    test("redisDecrby none exist key ")
	rv = redisDecrby(c, "k3", 20, &ret, NULL);
	test_cond(rv == CC_SUCCESS && ret==-20);
    // del, other this case will fail next time
	rv = redisDel(c, "k3", &ret, NULL);

	test("redisMget none exist key ");
	const char keys2[2][32] = {"k4","k5"};
	char vals2[2][32];
	rv = redisMget(c, (const char**)keys2, (char**)vals2, 32, 2, NULL);
	test_cond(rv == CC_SUCCESS && vals2[0][0]==0 && vals2[1][0]==0);

	test("redisMset ");
	const char keys3[2][32] = {"k4","k5"};
	const char vals3[2][32]= {"v4","v5"};
	rv = redisMset(c, (const char**)keys3, (const char**)vals3, 32, 2, NULL);
	test_cond(rv == CC_SUCCESS);

	test("redisMget exist key ");
	const char keys4[2][32] = {"k4","k5"};
	char vals4[2][32];
	rv = redisMget(c, (const char**)keys4, (char**)vals4, 32, 2, NULL);
	test_cond(rv == CC_SUCCESS && 0==strcmp(vals4[0],"v4") && 0==strcmp(vals4[1], "v5"));
	// del, other this case will fail next time
	redisDel(c, "k4", &ret, NULL);
	redisDel(c, "k5", &ret, NULL);

    test("redisHmget non exist key ")
    const char fields[3][32] = {"f1","f2","f3"};
	char vals5[3][32];
	rv = redisHmget(c, "k6", (const char**)fields, (char**)vals5, 32, 3, NULL);
	test_cond(rv == CC_SUCCESS && vals5[0][0]==0 && vals5[1][0]==0 && vals5[2][0]==0);
	//int redisHmset(struct redisClient* c, const char* key, const char** fields, const char** vals, long strsize, long arylen, long* ret, void* pipeline);

	test("redisHmset non exist key ")
    const char fields1[3][32] = {"f1","f2","f3"};
	const char vals6[3][32] = {"v1","v2","v3"};
	rv = redisHmset(c, "k6", (const char**)fields1, (const char**)vals6, 32, 3, NULL);
	test_cond(rv == CC_SUCCESS);

	test("redisHmget exist key ")
    const char fields2[3][32] = {"f1","f2","f3"};
	char vals7[3][32];
	rv = redisHmget(c, "k6", (const char**)fields2, (char**)vals7, 32, 3, NULL);
	test_cond(rv == CC_SUCCESS && 0==strcmp(vals7[0],"v1") && 0==strcmp(vals7[1],"v2") && 0==strcmp(vals7[2],"v3"));
	
	deleteRedisClnt(c);
}

static void testpipeline(){
    struct redisClient* c = createRedisClnt(IP, PORT, TIMEOUT);
	assert(c);
	void* pipeline = createPipeline(32);
	assert(pipeline);
	//long ret;
	test("redisSet/redisGet ");
    int rv = redisSet(c, "k1", "v1", pipeline);
	assert(rv == CC_SUCCESS);

	char val[32];
	rv = redisGet(c,"k1",val,32,pipeline);
	test_cond(rv == CC_SUCCESS);

	test("redisDel ");
	long ret1;
	rv = redisDel(c, "k1", &ret1, pipeline);
    test_cond(rv == CC_SUCCESS);

	test("redisDel none exist key ");
	long ret2;
	rv = redisDel(c, "k1", &ret2, pipeline);
    test_cond(rv == CC_SUCCESS);

	test("redisGet none exist key ");
	char val1[32];
	rv = redisGet(c,"k1",val1,32,pipeline);
    test_cond(rv == CC_SUCCESS);

	////
	test("redisExpire none exist key ")
	long ret3;
	rv = redisExpire(c, "k1", 10, &ret3, pipeline);
	test_cond(rv == CC_SUCCESS);

	test("redisGetSet none exit key ")
	char val2[32] = "v2";
	rv = redisGetset(c, "k1", val2, 32, pipeline);
	test_cond(rv == CC_SUCCESS);

	test("redisGetSet exit key ")
	char val3[32] = "v3";
	rv = redisGetset(c, "k1", val3, 32, pipeline);
	test_cond(rv == CC_SUCCESS);

	test("redisExpire exist key ")
	long ret4;
	rv = redisExpire(c, "k1", 100, &ret4, pipeline);
	test_cond(rv == CC_SUCCESS);

	test("redisIncr none integer type ")
	long ret5;
	rv = redisIncr(c, "k1", &ret5, pipeline);
	test_cond(rv == CC_SUCCESS);//have not been exe,so success

	test("redisIncr none exist key ")
	long ret6;
	rv = redisIncr(c, "k2", &ret6, pipeline);
	test_cond(rv == CC_SUCCESS);
	// del, other this case will fail next time
	rv = redisDel(c, "k2", &ret6, pipeline);

	test("redisIncrby none exist key ")
	long ret7;
	rv = redisIncrby(c, "k3", 20, &ret7, pipeline);
	test_cond(rv == CC_SUCCESS);
	// del, other this case will fail next time
	long ret8;
	rv = redisDel(c, "k3", &ret8, pipeline);
	////
	
	test("flushPipeline ");
	rv = flushPipeline(pipeline);
	test_cond(rv == CC_SUCCESS && 0==strcmp("v1", val) && ret1 == 1 && ret2 == 0 && val1[0] == 0 && ret3 == 0 
		&& val2[0] == 0 && 0==strcmp(val3, "v2")&& ret4 == 1 && ret6 == 1 && ret7==20);
	
    deletePipeline(pipeline);
	deleteRedisClnt(c);
}

int main(int argc, char **argv){
    //struct redisClient* c = createRedisClnt(IP, PORT, TIMEOUT);
	testmain();
	testpipeline();
    return 0;
}
