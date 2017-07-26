#ifndef __CCREDIS_H
#define __CCREDIS_H
#include "hiredis.h"
#define CC_RESULT_EOF       5
#define CC_NO_EFFECT        4
#define CC_OBJ_NOT_EXIST    3
#define CC_OBJ_EXIST        2
#define CC_PART_SUCCESS     1
#define CC_SUCCESS          0
#define CC_PARAM_ERR        -1
#define CC_REPLY_ERR        -2
#define CC_RQST_ERR         -3
#define CC_NO_RESOURCE      -4
#define CC_PIPELINE_ERR     -5
#define CC_NOT_SUPPORT      -6
#define CC_NOT_SAME_HASHSLOT -7
#define CC_CLUSTER_ERR      -8
#define CC_SLOT_CHANGED     -100



#define HOST_LEN 64
#define BOOL int
#define FALSE 0
#define TRUE 1
typedef struct redisClient redisClient;
struct clusterSlot{
	int timeout;
	//int usetime;
	char host[HOST_LEN];
	int port;
	redisContext *ctx;
	int startslot;
	int endslot;
};

struct redisClient{
	BOOL bcluster;
	BOOL bvalid;

	///info for non-cluster enviroment
	int timeout;
	//int usetime;
	char host[HOST_LEN];
	int port;
	redisContext *ctx;

	////info for cluster enviroment
	struct clusterSlot* slots;
	int nslot;
};

struct redisClient* createRedisClnt(const char* host, int port, int timeout);
void deleteRedisClnt(struct redisClient* c);
void* createPipeline(int initlen);
void deletePipeline(void* pipeline);
int flushPipeline(void* pipeline);

void* createPipelines(int len, struct redisClient* c);
void deletePipelines(void* pipeline);
int flushPipelines(void* pipeline);


// ret:  pointer to numberof keys that were removed
int redisDel(struct redisClient* c, const char* key, long* ret, void* pipeline);

// pointer to return value:
// *ret==0: if the key does not exist or the timeout could not be set
// *ret==1: if the timeout was set
int redisExpire(struct redisClient* c, const char* key, long sec, long* ret, void* pipeline);

//ret: pointer to value of key after the decrement
int redisDecr(struct redisClient* c, const char* key, long* ret, void* pipeline);
//ret: pointer to value of key after the decrement
int redisDecrby(struct redisClient* c, const char* key, long decr, long* ret, void* pipeline);
// ret: pointer to value of key, if key does not exist, ret[0]==0 and *len==0
int redisGet(struct redisClient* c, const char* key, char* ret, long len, void* pipeline);
int redisSet(struct redisClient* c, const char* key, const char* val, void* pipeline);

int redisGetset(struct redisClient* c, const char* key, char* val, long len, void* pipeline);

//ret: pointer to value of key after the increment
int redisIncr(struct redisClient* c, const char* key, long* ret, void* pipeline);

//ret: pinter to value of key after the increment
int redisIncrby(struct redisClient* c, const char* key, long incr, long* ret, void* pipeline);
int redisMget(struct redisClient* c, const char **keys, char **vals, int strsize, long* arylen, void* pipeline);
int redisMset(struct redisClient* c, const char **keys, const char **vals, int strsize, long arylen, void* pipeline);
int redisHmget(struct redisClient* c, const char* key, const char** fields, char** vals, long strsize, long* arylen, void* pipeline);
int redisHmset(struct redisClient* c, const char* key, const char** fields, const char** vals, long strsize, long arylen, void* pipeline);

//list
int redisLlpush(struct redisClient* c, const char* key, char** vals, long strsize, long arylen, void* pipeline);
int redisLrange(struct redisClient* c, const char* key, int start, int stop, char **vals, long strsize, long *arylen, void* pipeline);
int redisLlen(struct redisClient* c, const char* key, char* val, long len, void* pipeline);
int redisLrem(struct redisClient* c, const char* key, int count, const char* val, void* pipeline);
int redisLindex(struct redisClient* c, const char* key, int index, char* val, long len, void* pipeline);
int redisLtrim(struct redisClient* c, const char* key, int start, int stop, void* pipeline);
int redisLset(struct redisClient* c, const char* key, int index, const char* val, void* pipeline);

//set
int redisSadd(struct redisClient* c, const char* key, char** vals, long strsize, long arylen, void* pipeline);
int redisScard(struct redisClient* c, const char* key,  char* val, long len, void* pipeline);
int redisSunionstore(struct redisClient* c, const char* dstkey, char** keys, long strsize, long arylen, long *res, void* pipeline);
int redisSismember(struct redisClient* c, const char* key, char* val, long len, void* pipeline);
int redisSrem(struct redisClient* c, const char* key, char** vals, long strsize, long arylen, void* pipeline);
int redisSadds(struct redisClient* c,const char* key,const char* val,void*pipeline);

//sorted set
int redisZadd(struct redisClient* c, const char* key, char** scores, char** vals, long strsize, long arylen, void* pipeline);
int redisZadds(struct redisClient* c, const char* key, unsigned long score, char* val, void* pipeline);
int redisZcount(struct redisClient* c, const char* key, unsigned long score_min, unsigned long score_max, long *res, void* pipeline);
int redisZremrangebyscore(struct redisClient* c, const char* key, unsigned long score_min, unsigned long score_max, long *res, void* pipeline);

#endif //__CCREDIS_H
