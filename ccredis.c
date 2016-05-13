#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include "hiredis.h"
#include "ccredis.h"

static BOOL checkConnectResult(redisContext* c);
static BOOL loadCluster(struct redisClient* c);
static void cleanSlot(struct clusterSlot* slot);



//#define BOOL int
//#define FALSE 0
//#define TRUE 1

// crc16 for computing redis cluster slot
static const uint16_t crc16table[256] = {
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
    0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
    0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
    0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
    0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
    0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
    0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
    0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
    0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
    0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
    0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
    0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
    0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
    0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
    0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
    0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
    0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
    0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
    0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
    0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
    0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
    0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
    0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
    0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
    0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
    0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
    0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
    0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
    0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0
};

static uint16_t crc16(const char *data, int len) {
    int cnt;
    uint16_t crc = 0;
    for (cnt = 0; cnt < len; cnt++)
        crc = (crc << 8) ^ crc16table[((crc >> 8) ^ * data++) & 0x00FF];
    return crc;
}

static uint32_t hashSlot(const char* key) {
    size_t len = strlen(key);
    size_t start, end; /* start-end indexes of { and  } */

    /* Search the first occurrence of '{'. */
    for (start = 0; start < len; start++)
	    if (key[start] == '{')
		    break;

    /* No '{' ? Hash the whole key. This is the base case. */
    if (start == len)
	    return crc16(key, len) & 16383;

    /* '{' found? Check if we have the corresponding '}'. */
    for (end = start + 1; end < len; end++)
	    if (key[end] == '}')
		    break;

    /* No '}' or nothing between {} ? Hash the whole key. */
    if (end == len || end == start + 1)
	    return crc16(key, len) & 16383;

    /* If we are here there is both a { and a  } on its right. Hash
     * what is in the middle between { and  }. */
    return crc16(key + start + 1, end - start - 1) & 16383;
}

typedef int (*fetchFunc)(redisReply* reply, void* p);

static int fetchInteger(redisReply *reply, void *p) {
	long* val = (long*)p;      
	if (reply->type == REDIS_REPLY_INTEGER) {
		if (val)
			*val = reply->integer;
		return CC_SUCCESS;
	}
	else if (reply->type == REDIS_REPLY_NIL)
		return CC_OBJ_NOT_EXIST;
	else
		return CC_REPLY_ERR;
}


struct strArg {
	char* str;
	long len;
};


static int _fetchString(redisReply *reply, char* str, long len) {	
	//if not care
	if(str == NULL)
		return CC_SUCCESS;
	if (reply->type == REDIS_REPLY_STRING || reply->type == REDIS_REPLY_STATUS) {
		if(len < strlen(str)+1)
			return CC_PARAM_ERR;

		strncpy(str, reply->str, len);
		return CC_SUCCESS;
	}
	else if (reply->type == REDIS_REPLY_NIL) {
		str[0]=0;
		return CC_SUCCESS;
	}
	else
		return CC_REPLY_ERR;
}

static int fetchString(redisReply *reply, void* p) {	
	struct strArg* val = (struct strArg*)p;
	char *str = val->str;
	long len = val->len;
	return _fetchString(reply, str, len);
}

struct intaryArg {
	long* ary;
	long* len;
};
static int fetchIntegerArray(redisReply *reply, void* p) {
	//if (reply->type == REDIS_REPLY_INTEGER) //need check!
	struct intaryArg* val = (struct intaryArg*)p;
	long* ary = val->ary;
	long* len = val->len;
	if (reply->type == REDIS_REPLY_ARRAY) {
		int ret = CC_SUCCESS;
		size_t i;
		if(*len < reply->elements)
			return CC_PARAM_ERR;

		for (i = 0; i < reply->elements; ++i) {
			int r = fetchInteger(reply->element[i], &ary[i]);
			if (r != CC_SUCCESS)
				return r;
		}
		*len = reply->elements;
		return ret;
	}
	else
		return CC_REPLY_ERR;
}

struct straryArg {
	char** ary;
	long strlen;
	long* arylen;
};


static int _fetchStringArray(redisReply *reply, char** ary, long strlen, long* arylen) {
	if (reply->type == REDIS_REPLY_ARRAY) {
		int ret = CC_SUCCESS;
		size_t i;
		if (*arylen < reply->elements)
			return CC_PARAM_ERR;

		for (i = 0; i < reply->elements; ++i) {
			int r = _fetchString(reply->element[i], (char*)ary+i*strlen, strlen);
			if (r != CC_SUCCESS)
				return r;
		}
		*arylen = reply->elements;
		return ret;
	}
	else if (reply->type == REDIS_REPLY_NIL) {
		*arylen = 0;
		return CC_SUCCESS;
	}
	else
		return CC_REPLY_ERR;
}

static int fetchStringArray(redisReply *reply, void* p) {
	struct straryArg* val = (struct straryArg*)p;
	char** ary = val->ary;
	long strlen = val->strlen;
	long* arylen = val->arylen;
	return _fetchStringArray(reply, ary, strlen, arylen);
}

static int fetchTime(redisReply *reply, void* p) {
	struct timeval* val = (struct timeval*)p;
	if (reply->type == REDIS_REPLY_ARRAY) {
		if (reply->elements != 2 || reply->element[0]->type != REDIS_REPLY_STRING ||
				reply->element[1]->type != REDIS_REPLY_STRING)
			return CC_REPLY_ERR;

		if (val) {
			val->tv_sec = atol(reply->element[0]->str);
			val->tv_usec = atol(reply->element[1]->str);
		}
		return CC_SUCCESS;
	}
	else
		return CC_REPLY_ERR;
}

struct slotArg{
	struct clusterSlot* slots;
	long* len;
};
static int fetchSlot(redisReply *reply, void* p)
{
	struct slotArg* val = (struct slotArg*)p;
	struct clusterSlot* slots = val->slots;
	long* len = val->len;
	if (reply->type == REDIS_REPLY_ARRAY) {
		size_t i;
		if (*len < reply->elements)
			return CC_PARAM_ERR;

		for (i = 0; i < reply->elements; ++i) {
			redisReply *subreply = reply->element[i];
			if (subreply->type != REDIS_REPLY_ARRAY || subreply->elements < 3)
				return CC_REPLY_ERR;

			slots[i].startslot = subreply->element[0]->integer;
			slots[i].endslot = subreply->element[1]->integer;
			//slotReg.pRedisServ = nullptr;
			assert(strlen(subreply->element[2]->element[0]->str)<HOST_LEN);
			strncpy(slots[i].host, subreply->element[2]->element[0]->str, HOST_LEN);
			slots[i].port = subreply->element[2]->element[1]->integer;
		}
		*len = reply->elements;
		return CC_SUCCESS;
	}
	else
		return CC_REPLY_ERR;
}


struct redisClient* createRedisClnt(const char* host, int port, int timeout){
	struct redisClient* c = malloc(sizeof(struct redisClient));
	if(c == NULL)
		return NULL;
	memset(c, 0, sizeof(struct redisClient));
	struct timeval tv = {timeout, 0};
	if((c->ctx = redisConnectWithTimeout(host, port, tv))==NULL || c->ctx->err){
		goto out;
	}

	redisReply* r = redisCommand(c->ctx, "info");
	if(r == NULL)
		goto out;

	// is cluster?
	if (strstr(r->str, "cluster_enabled:1")){
		c->bcluster = 1;
	}
	if(c->bcluster){
		c->slots[0].ctx = c->ctx;
		c->ctx = NULL;
		strncpy(c->slots[0].host, host, HOST_LEN);
		c->slots[0].port = port;
		//c->slots[0].usetime = c->usetime;
		c->slots[0].timeout = c->timeout;
		c->nslot=1;
		c->bvalid = loadCluster(c);
	}else{
		c->port = port;
		strncpy(c->host, host, HOST_LEN);
		c->timeout = timeout;
		c->bvalid = TRUE;
	}
	freeReplyObject(r);
	return c;
out:
	if(c->ctx)
		redisFree(c->ctx);
	free(c);
	return NULL;
}


void deleteRedisClnt(struct redisClient* c){
	if(c == NULL)
		return;
	if(c->ctx)
		redisFree(c->ctx);
	if(c->bcluster){
		int i;
		for(i=0; i<c->nslot; i++){
			cleanSlot(&c->slots[i]);
		}
	}
	free(c);
}

static BOOL tryLoadCluster(redisContext* ctx, struct clusterSlot slots[], int* nslot, long timeout);

// when cmd fail in cluster enviroment, we will reload cluster slots info:
// try slot in sequence until one success  
static BOOL loadCluster(struct redisClient* c){
	int i;

	for(i=0;i<c->nslot;i++){
		struct clusterSlot* slot = &c->slots[i];
		if(slot->ctx)
			if(tryLoadCluster(slot->ctx, c->slots, &c->nslot, c->timeout))
				return TRUE;
	}
	return FALSE;
}


// use a redis context to fetch cluster slots info, then create contexts for all slots
static BOOL tryLoadCluster(redisContext* ctx, struct clusterSlot* slots, int* nslot, long timeout)
{
	int ret;
	long slotnum=SERVER_NUM;
	redisReply* r = redisCommand(ctx, "cluster slots");
	if(r == NULL)
		return FALSE;
	struct slotArg slotarg = {slots, &slotnum};
	ret = fetchSlot(r, &slotarg);
	if(ret != CC_SUCCESS){
		freeReplyObject(r);
		return FALSE;
	}
	int i;
	for(i=0;i<slotnum;i++){
		struct timeval tm = {timeout, 0};
		struct clusterSlot* slot = &slots[i];
		// free old one if exist
		if(slot->ctx)
			redisFree(slot->ctx);
		slot->ctx = redisConnectWithTimeout(slot->host, slot->port, tm);
		if(!slot->ctx || slot->ctx->err){
			if(slot->ctx){
				redisFree(slot->ctx);
				slot->ctx = NULL;
			}
			goto out;
		}
		redisSetTimeout(slot->ctx, tm);
		//slot->usetime = time(NULL);
		slot->timeout = timeout;
	}
	//clean invalid slots if exist
	for(i=slotnum; i<SERVER_NUM; i++){
		cleanSlot(&slots[i]);
	}
	freeReplyObject(r);
	*nslot = slotnum;
	return TRUE;
out:
	freeReplyObject(r);
	for(i=0; i<slotnum; i++){
		cleanSlot(&slots[i]);
	}
	return FALSE;
}

static void cleanSlot(struct clusterSlot* slot){
	if(slot->ctx){
		redisFree(slot->ctx);
		slot->ctx = NULL;
		slot->host[0]=0;
		slot->port=-1;
		slot->startslot=0;
		slot->endslot=0;
	}
}

static struct clusterSlot* findSlot(struct redisClient* c, int hashslot){
	int i;
	for(i=0;i<c->nslot;i++){
		struct clusterSlot* slot = &(c->slots[i]);
		if(hashslot>=slot->startslot && hashslot<=slot->endslot)
			return slot;
	}
	return NULL;
}

static redisContext* findContext(struct redisClient* c, int hashslot){
	redisContext* ctx;
	assert(c->bcluster);
	assert(hashslot != -1);
	struct clusterSlot* slot = findSlot(c, hashslot);
	if (slot==NULL)
		return NULL;
	ctx = slot->ctx;
	return ctx;
}


static BOOL isSameHashslot(const char** keys, size_t keysz, size_t keynum){
	int i;
	for(i = 0; i < keynum-1; i++){
		if(hashSlot((char*)keys+keysz*i) != hashSlot((char*)keys+keysz*(i+1)))
			return FALSE;
	}
	return TRUE;
}


static BOOL isServerClosed(redisContext* c){
	return (c->err == REDIS_ERR_EOF || strstr(c->errstr, "Server closed"));
}

static BOOL reconnectServer(struct redisClient* c){
	struct timeval tv = {c->timeout, 0};
	if(c->ctx){
		redisFree(c->ctx);
	}
	c->ctx = redisConnectWithTimeout(c->host, c->port, tv);
	if(c->ctx==NULL || c->ctx->err){
		if(c->ctx){
			redisFree(c->ctx);
			c->ctx = NULL;
		}
		return FALSE;
	}
	return TRUE;
}

static BOOL isMovedErr(redisContext*c){
	return strstr(c->errstr, "MOVED")?TRUE:FALSE;
}


static BOOL reLoadServers(struct redisClient* c, redisContext* currentCtx){
	if(c->bcluster){
		if(currentCtx==NULL || isMovedErr(currentCtx) || isServerClosed(currentCtx)){
			c->bvalid = loadCluster(c);
			if(!c->bvalid){
				return FALSE;
			}
		}else{
			return FALSE;
		}
	}else{//single node
		if(currentCtx==NULL || isServerClosed(currentCtx)){
			if(!reconnectServer(c)){
				return FALSE;
			}
		}else{
			return FALSE;
		}
	}
	return TRUE;
}

int executeImpl(struct redisClient* c, int hashslot, sds cmdstr, void* result, fetchFunc fetch){
	if(c == NULL)
		return CC_PARAM_ERR;
	redisContext* ctx = c->bcluster?findContext(c, hashslot):c->ctx;

	int ret = CC_SUCCESS;
	redisReply* r = NULL;
	if(ctx)
		r = redisCommand(ctx, cmdstr);
	// seem ctx==NULL as an request fail, and try again
	if(ctx==NULL || r==NULL || ctx->err!=REDIS_OK){

		if(!reLoadServers(c, ctx)){
			ret = CC_RQST_ERR;
			goto err;
		}
		ctx = c->bcluster?findContext(c, hashslot):c->ctx;
		if(ctx == NULL){
			ret = CC_RQST_ERR;
			goto err;
		}
		//// try again
		r = redisCommand(ctx, cmdstr);
		if(r==NULL || ctx->err!=REDIS_OK){
			ret = CC_RQST_ERR;
			goto err;
		}
	}
	ret = fetch(r, result);
err:
	if(r)
		freeReplyObject(r);
	sdsfree(cmdstr);
	return ret;
}



struct cmdObj{
	char buf[32];
	void* result;
	fetchFunc fetchfunc;
	sds cmdstr;
};
struct pipeLine{
	int hashslot; //all keys in one pipeline must have same hashslot, which will get highest efficiency
	struct redisClient* c;
	struct cmdObj* cmds;
	size_t len;
	int used;
};

int pushPipeline(void* pipeline, sds cmdstr, void* result, size_t resultsz, fetchFunc fetchfunc);

int executeImplPipeline(struct redisClient* c, int hashslot, sds cmdstr, 
		void* result, int resultsz, fetchFunc fetch, void* pipeline){
	struct pipeLine* p = (struct pipeLine*)pipeline;
	if (p->used == 0){
		assert(p->c==NULL);
		p->c = c;
		if(p->c->bcluster){
			p->hashslot = hashslot;
		}
	}else{
		if(p->c != c)
			return CC_PIPELINE_ERR;
		if(p->c->bcluster && p->hashslot != hashslot)
			return CC_NOT_SAME_HASHSLOT;
	}
	pushPipeline(p, cmdstr, result, resultsz, fetch);
	return CC_SUCCESS;
}

void* createPipeline(int initlen){
	if (initlen < 1)
		return NULL;
	struct pipeLine* p = malloc(sizeof(struct pipeLine));
	if (p==NULL)
		return NULL;
	memset(p, 0, sizeof(*p));
	p->len = initlen;
	p->used = 0;
	p->cmds = malloc(sizeof(struct cmdObj)*p->len);
	if(p->cmds==NULL){
		free(p);
		return NULL;
	}
	memset(p->cmds, 0, sizeof(struct cmdObj)*p->len);
	return p;
}

void deletePipeline(void* pipeline){
	if(pipeline==NULL)
		return;
	struct pipeLine* p = (struct pipeLine*)pipeline;
	int i;
	for(i=0; i<p->len; i++){
		if(p->cmds[i].cmdstr){
			sdsfree(p->cmds[i].cmdstr);
			p->cmds[i].cmdstr = NULL;
		}
	}
	p->len = 0;
	free(p->cmds);
	free(p);
}

int pushPipeline(void* pipeline, sds cmdstr, void* result, size_t resultsz, fetchFunc fetchfunc){
	if(pipeline==NULL)
		return CC_PARAM_ERR;
	struct pipeLine* p = (struct pipeLine*)pipeline;
	if(p->len == p->used){
		// extend autoly
		void* cmds = malloc(sizeof(struct cmdObj)*(p->len+64));
		if(cmds==NULL)
			return CC_NO_RESOURCE;
		memset(cmds, 0, sizeof(struct cmdObj)*(p->len+64));
		memcpy(cmds, p->cmds, sizeof(struct cmdObj)*p->len);
		p->len = p->len + 64;
		free(p->cmds);
		p->cmds = cmds;
	}
	struct cmdObj* cmd = &(p->cmds[p->used]);
	if(resultsz == -1){
		cmd->result = result;
	}else{
		assert(resultsz < 32);
		memcpy(cmd->buf, result, resultsz);
		cmd->result = cmd->buf;
	}
	cmd->fetchfunc = fetchfunc;
	cmd->cmdstr = cmdstr; 
	//p->cmds[p->used].freeresult = freeresult;
	p->used++;
	return CC_SUCCESS;
}

static int appendAllCmds(void* pipeline, redisContext* ctx){
	struct pipeLine* p = (struct pipeLine*)pipeline;
	int i;
	for(i=0; i<p->used; i++){
		if(REDIS_OK != redisAppendCommand(ctx, p->cmds[i].cmdstr))
			return CC_RQST_ERR;
	}
	return CC_SUCCESS;
}

static void freeAllCmdStrs(void* pipeline){
	struct pipeLine* p = (struct pipeLine*)pipeline;
	int i;
	for(i=0; i<p->used; i++){
		if(p->cmds[i].cmdstr){
			sdsfree(p->cmds[i].cmdstr);
			p->cmds[i].cmdstr = NULL;
		}
	}
}

static int fetchAllReplys(void* pipeline, redisContext* ctx){
	struct pipeLine* p = (struct pipeLine*)pipeline;
	int ret = CC_SUCCESS;
	int i;
	// continue even failed, to avoid left reply in 
	// recv buf, which will affect next flush pipeline op
	for(i=0; i<p->used; i++){
		redisReply* reply = NULL;
		ret = redisGetReply(ctx, (void**)&reply);
		if(ret != REDIS_OK || reply==NULL || ctx->err!=REDIS_OK){
			ret = CC_RQST_ERR;
			if(reply)
				freeReplyObject(reply);
			continue;
		}
		struct cmdObj* cmd = &p->cmds[i];
		ret = cmd->fetchfunc(reply, cmd->result);
		if(ret != CC_SUCCESS){
			freeReplyObject(reply);
			ret = CC_RQST_ERR;
			continue;
		}
		if(reply)
			freeReplyObject(reply);
	}
	return ret;
}

int flushPipeline(void* pipeline){

	if(pipeline == NULL)
		return CC_PARAM_ERR;
	struct pipeLine* p = (struct pipeLine*)pipeline;

	if(p->used==0)
		return CC_SUCCESS;
	redisContext* ctx = p->c->bcluster?findContext(p->c, p->hashslot):p->c->ctx;

	int ret = CC_SUCCESS;
	if(ctx){
		ret = appendAllCmds(p, ctx);
		if(ret != CC_SUCCESS){
			goto out;
		}
		ret = fetchAllReplys(p, ctx);
	}

	//// double try
	// seem ctx==NULL as an request fail, and try again
	if (ctx==NULL || ret != CC_SUCCESS){
		if(!reLoadServers(p->c, ctx)){
			ret = CC_RQST_ERR;
			goto out;
		}
		ctx = p->c->bcluster?findContext(p->c, p->hashslot):p->c->ctx;
		if(ctx == NULL){
			ret = CC_RQST_ERR;
			goto out;
		}
		//// try again
		ret = appendAllCmds(p, ctx);
		if(ret != CC_SUCCESS){
			goto out;
		}
		ret = fetchAllReplys(p, ctx);
		if(ret != CC_SUCCESS){
			goto out;
		}
	}
out:
	freeAllCmdStrs(p);
	return ret;
}

sds cmdnew(const char* cmd){
	return sdsnew(cmd);
}

sds cmdcat(sds s, const char* str){
	return sdscat(sdscat(s, " "), str);
}

sds cmdcats(sds s, const char** strs, long sz, long num){
	int i;
	for(i=0; i<num; i++){
		s = sdscat(sdscat(s, " "), (char*)strs+sz*i);
	}
	return s;
}

sds cmdcatss(sds s, const char** keys, const char** vals, long sz, long num){
	int i;
	for(i=0; i<num; i++){
		s = sdscat(sdscat(s, " "), (char*)keys+sz*i);
		s = sdscat(sdscat(s, " "), (char*)vals+sz*i);
	}
	return s;
}


sds cmddup(const char* cmd){
	return sdsnew(cmd);
}


int executeCmd(struct redisClient* c, int hashslot, sds cmdstr, 
		void* result, int resultsz, fetchFunc fetch, void* pipeline){
	if(pipeline == NULL){
		return executeImpl(c,hashslot,cmdstr,result,fetch);
	}else{
		return executeImplPipeline(c,hashslot,cmdstr,result,resultsz,fetch,pipeline);
	}
}


// *ret: the numberof keys that were removed
int redisDel(struct redisClient* c, const char* key, long* ret, void* pipeline){
	sds cmd = cmdnew("del");
	cmd = cmdcat(cmd, key);
	return executeCmd(c, hashSlot(key), cmd, ret, -1, fetchInteger, pipeline);
}
// 
// *ret==0: if the key does not exist or the timeout could not be set
// *ret==1: if the timeout was set
int redisExpire(struct redisClient* c, const char* key, long sec, long* ret, void* pipeline){
	sds cmd = cmdnew("expire");
	cmd = sdscatfmt(cmd," %s %i", key, sec);
	return executeCmd(c, hashSlot(key), cmd, ret, -1, fetchInteger, pipeline);
}

//*ret: the value of key after the decrement
int redisDecr(struct redisClient* c, const char* key, long* ret, void* pipeline){
	sds cmd = cmdnew("decr");
	cmd = cmdcat(cmd, key);
	return executeCmd(c, hashSlot(key), cmd, ret, -1, fetchInteger, pipeline);
}

//*ret: the value of key after the decrement
int redisDecrby(struct redisClient* c, const char* key, long decr, long* ret, void* pipeline){
	sds cmd = cmdnew("decrby");
	cmd = sdscatfmt(cmd," %s %i", key, decr);
	return executeCmd(c, hashSlot(key), cmd, ret, -1, fetchInteger, pipeline);
}

// *ret: the value of key, if key does not exist, ret[0]==0 and *len==0
int redisGet(struct redisClient* c, const char* key, char* ret, long len, void* pipeline){
	sds cmd = cmdnew("get");
	cmd = cmdcat(cmd, key);
	struct strArg arg = {ret, len};
	return executeCmd(c, hashSlot(key), cmd, &arg, sizeof(arg), fetchString, pipeline);
}

int redisSet(struct redisClient* c, const char* key, const char* val, void* pipeline){
	sds cmd = cmdnew("set");
	cmd = sdscatfmt(cmd," %s %s", key, val);
	//do not care string returned
	struct strArg arg = {NULL, 0};
	return executeCmd(c, hashSlot(key), cmd, &arg, sizeof(arg), fetchString, pipeline);
}


int redisGetset(struct redisClient* c, const char* key, char* val, long len, void* pipeline){
	sds cmd = cmdnew("getset");
	cmd = sdscatfmt(cmd," %s %s", key, val);
	struct strArg arg = {val, len};
	return executeCmd(c, hashSlot(key), cmd, &arg, sizeof(arg), fetchString, pipeline);
}

//*ret: the value of key after the increment
int redisIncr(struct redisClient* c, const char* key, long* ret, void* pipeline){
	sds cmd = cmdnew("incr");
	cmd = cmdcat(cmd, key);
	return executeCmd(c, hashSlot(key), cmd, ret, -1, fetchInteger, pipeline);
}

//*ret: the value of key after the increment
int redisIncrby(struct redisClient* c, const char* key, long incr, long* ret, void* pipeline){
	sds cmd = cmdnew("incrby");
	cmd = sdscatfmt(cmd," %s %i", key, incr);
	return executeCmd(c, hashSlot(key), cmd, ret, -1, fetchInteger, pipeline);
}

int redisMget(struct redisClient* c, const char **keys, char **vals, int strsize, long* arylen, void* pipeline){
	if(c->bcluster || pipeline){
		if(!isSameHashslot(keys, strsize, *arylen)){
			return CC_NOT_SAME_HASHSLOT;
		}
	}
	sds cmd = cmdnew("mget");
	cmd = cmdcats(cmd,keys,strsize,*arylen);
	struct straryArg arg = {vals, strsize, arylen};
	return executeCmd(c, hashSlot((char*)keys), cmd, &arg, sizeof(arg), fetchStringArray, pipeline);
}

int redisMset(struct redisClient* c, const char **keys, const char **vals, int strsize, long arylen, void* pipeline){
	if(c->bcluster || pipeline){
		if(!isSameHashslot(keys, strsize, arylen)){
			return CC_NOT_SAME_HASHSLOT;
		}
	}
	sds cmd = cmdnew("mset");
	cmd = cmdcatss(cmd,keys,vals,strsize,arylen);
	// do not care return string
	struct strArg retstr = {NULL, 0};
	return executeCmd(c, hashSlot((char*)keys), cmd, &retstr, sizeof(retstr), fetchString, pipeline);
}


int redisHmget(struct redisClient* c, const char* key, const char** fields, char** vals, long strsize, long* arylen, void* pipeline){
	sds cmd = cmdnew("hmget");
	cmd = cmdcat(cmd, key);
	cmd = cmdcats(cmd, fields, strsize, *arylen);
	struct straryArg arg = {vals, strsize, arylen};
	return executeCmd(c, hashSlot(key), cmd, &arg, sizeof(arg), fetchStringArray, pipeline);
}

int redisHmset(struct redisClient* c, const char* key, const char** fields, const char** vals, long strsize, long arylen, void* pipeline){
	sds cmd = cmdnew("hmset");
	cmd = cmdcat(cmd, key);
	cmd = cmdcatss(cmd, fields, vals, strsize, arylen);
	// do not care return string
	struct straryArg retstr = {NULL, 0};
	return executeCmd(c, hashSlot(key), cmd, &retstr, sizeof(retstr), fetchString, pipeline);
}

