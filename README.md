# ccredis
ccredis focus on using cluster and pipeline of redis.
you could use cluster and pipeline at the same time, and in very simply and naturely way:

#define IP "127.0.0.1"
#define PORT 6379
#define TIMEOUT 2

#define CLUSTER_PORT 7001

static int tests = 0, fails = 0;
#define test(_s) { printf("#%02d ", ++tests); printf(_s); }
#define test_cond(_c) if(_c) printf("\033[0;32mPASSED\033[0;0m\n"); else {printf("\033[0;31mFAILED\033[0;0m\n"); fails++;}

static void testclusterpipeline(){
  struct redisClient* c = createRedisClnt(IP, CLUSTER_PORT, TIMEOUT);
	assert(c);
	void* pipeline = createPipeline(32);
	assert(pipeline);

	test("redisSet/redisGet ");
  int rv = redisSet(c, "{p}k1", "v1", pipeline);
	assert(rv == CC_SUCCESS);

	char val[32];
	// to for pipeline only accept key with same hashslot, so add {p} to assure it!
	rv = redisGet(c,"{p}k1",val,32,pipeline);
	test_cond(rv == CC_SUCCESS);

	test("redisDel ");
	long ret1;
	rv = redisDel(c, "{p}k1", &ret1, pipeline);
  test_cond(rv == CC_SUCCESS);

	test("redisDel none exist key ");
	long ret2;
	rv = redisDel(c, "{p}k1", &ret2, pipeline);
    test_cond(rv == CC_SUCCESS);

	test("redisGet none exist key ");
	char val1[32];
	rv = redisGet(c,"{p}k1",val1,32,pipeline);
  test_cond(rv == CC_SUCCESS);
  
  test("flushPipeline ");
	rv = flushPipeline(pipeline);
	
	test_cond(rv == CC_SUCCESS && 0==strcmp("v1", val) && ret1 == 1 && ret2 == 0 && val1[0] == 0);
	
	attention:
	for efficiency, all keys in one pipeline must has same hashslot, 
	so you should add same tag (like {p} in upper sample) for all keys.
    
  
