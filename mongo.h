#ifndef __COMMON_MONGO_H__
#define __COMMON_MONGO_H__

#include "../includes.h"

//#define COMMON_MONGO_DEBUG

typedef struct {
	const char *uri;
	const char *db;
	const char *collection;
	mongoc_client_pool_t *clientPool;
} mongo_config_t;

typedef struct {
	mongoc_client_t *client;
	mongoc_collection_t *collection;
	mongoc_cursor_t *cursor;
	mongo_config_t *config;
} mongo_cursor_t;

void mongo_initialize(mongo_config_t *config);
mongoc_collection_t *mongo_getCollection(mongo_config_t *config, mongoc_client_t *client);
mongo_cursor_t *mongo_query(mongo_config_t *config, apr_pool_t *pool, apr_table_t *map);
void mongo_destroyCursor(mongo_cursor_t *cursorObj);
char *mongo_commit(mongo_config_t *config, apr_pool_t *pool, apr_table_t *map);
void mongo_delete(mongo_config_t *config, apr_pool_t *pool, apr_table_t *map);
void mongo_update(mongo_config_t *config, apr_pool_t *pool, apr_table_t *oldMap, apr_table_t *newMap);
int64_t mongo_count(mongo_config_t *config, apr_pool_t *pool, apr_table_t *map);
void mongo_bson2map(apr_table_t *map, const bson_t *bson);
void mongo_destroy(mongo_config_t *config);
uint8_t mongo_isOidValid(char *oid);

#endif
