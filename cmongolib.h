#ifndef CMONGOLIB_CMONGOLIB_H
#define CMONGOLIB_CMONGOLIB_H

#include <bson.h>
#include <mongoc.h>

typedef struct {
	const char *uri;
	const char *db;
	const char *collection;
	mongoc_client_pool_t *clientPool;
} mongo_config_t;

void mongo_initialize(mongo_config_t *config);

#endif
