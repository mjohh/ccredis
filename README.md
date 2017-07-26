
# ccredis
__1)Supporting pipelines for cluster and single node__  
__2)Same creation interface for single node and cluster__  
__3)Auto Sharding for cluster__  
__4)Auto reconnection for single node and cluster__


	void cluster_pipelines_test(){
	printf("\n cluster_pipelines_test() start!!!\n");
	struct redisClient* redis = createRedisClnt("172.23.25.178", 6379, 6);
	ASSERT_PRNT(redis!=NULL, "\nconnect redis fail!!!");

	do {
		void* pipeline = createPipelines(1, redis);
		////
		char r0[1024];
		char r1[1024];
		char r2[1024];
		char r3[1024];
		char r4[1024];
		char r5[1024];
		char r6[1024];
		char r7[1024];
		char r8[1024];
		long l = 1024;
		int rv; 
		rv = redisSet(redis, "k2{12345678900}", "v0", pipeline);
		ASSERT_PRNT(rv==0, "\nset rv0=%d", rv);
		rv = redisSet(redis, "k2{09876543211}", "v1", pipeline);
		ASSERT_PRNT(rv==0, "\nset rv1=%d", rv);
		rv = redisSet(redis, "k2{12345098762}", "v2", pipeline);
		ASSERT_PRNT(rv==0, "\nset rv2=%d", rv);
		rv = redisSet(redis, "k2{67890543213}", "v3", pipeline);
		ASSERT_PRNT(rv==0, "\nset rv3=%d", rv);
		rv = redisSet(redis, "k2{qwertyuiop4}", "v4", pipeline);
		ASSERT_PRNT(rv==0, "\nset rv4=%d", rv);
		rv = redisSet(redis, "k2{mnbvcxzlke5}", "v5", pipeline);
		ASSERT_PRNT(rv==0, "\nset rv5=%d", rv);
		rv = redisSet(redis, "k2{djehjkflsi6}", "v6", pipeline);
		ASSERT_PRNT(rv==0, "\nset rv6=%d", rv);
		rv = redisSet(redis, "k2{ssfkliowrj7}", "v7", pipeline);
		ASSERT_PRNT(rv==0, "\nset rv7=%d", rv);
		rv = redisSet(redis, "k2{akl;jfkjaw8}", "v8", pipeline);
		ASSERT_PRNT(rv==0, "\nset rv8=%d", rv);

		rv = flushPipelines(pipeline);
		ASSERT_PRNT(rv==0, "\nflush rv=%d", rv);

		rv = redisGet(redis, "k2{12345678900}", r0, l, pipeline);
		ASSERT_PRNT(rv==0, "\nget rv0=%d", rv);
		rv = redisGet(redis, "k2{09876543211}", r1, l, pipeline);
		ASSERT_PRNT(rv==0, "\nget rv1=%d", rv);
		rv = redisGet(redis, "k2{12345098762}", r2, l, pipeline);
		ASSERT_PRNT(rv==0, "\nget rv2=%d", rv);
		rv = redisGet(redis, "k2{67890543213}", r3, l, pipeline);
		ASSERT_PRNT(rv==0, "\nget rv3=%d", rv);
		rv = redisGet(redis, "k2{qwertyuiop4}", r4, l, pipeline);
		ASSERT_PRNT(rv==0, "\nget rv4=%d", rv);
		rv = redisGet(redis, "k2{mnbvcxzlke5}", r5, l, pipeline);
		ASSERT_PRNT(rv==0, "\nget rv5=%d", rv);
		rv = redisGet(redis, "k2{djehjkflsi6}", r6, l, pipeline);
		ASSERT_PRNT(rv==0, "\nget rv6=%d", rv);
		rv = redisGet(redis, "k2{ssfkliowrj7}", r7, l, pipeline);
		ASSERT_PRNT(rv==0, "\nget rv7=%d", rv);
		rv = redisGet(redis, "k2{akl;jfkjaw8}", r8, l, pipeline);
		ASSERT_PRNT(rv==0, "\nget rv8=%d", rv);
		flushPipelines(pipeline);
		ASSERT_PRNT(strcmp(r0, "v0")==0, "\nredis Get0 fail!");
		ASSERT_PRNT(strcmp(r1, "v1")==0, "\nredis Get1 fail!");
		ASSERT_PRNT(strcmp(r2, "v2")==0, "\nredis Get2 fail!");
		ASSERT_PRNT(strcmp(r3, "v3")==0, "\nredis Get3 fail!");
		ASSERT_PRNT(strcmp(r4, "v4")==0, "\nredis Get4 fail!");
		ASSERT_PRNT(strcmp(r5, "v5")==0, "\nredis Get5 fail!");
		ASSERT_PRNT(strcmp(r6, "v6")==0, "\nredis Get6 fail!");
		ASSERT_PRNT(strcmp(r7, "v7")==0, "\nredis Get7 fail!");
		ASSERT_PRNT(strcmp(r8, "v8")==0, "\nredis Get8 fail!");
		deletePipelines(pipeline);	
	} while(0);
	////
	
	deleteRedisClnt(redis);
	printf("\n cluster_pipelines_test() end!!!\n");
 	}

