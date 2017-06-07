#include "mongo.h"
#include "jsonhandling.h"

#define COMMON_MONGO_DEBUG
#include "../common/logging.h"
#ifdef COMMON_MONGO_DEBUG


#define DEBUG_MSG(fmt, ...) LOG_SERVER_DEBUG_FORMAT(fmt, "mongo", __VA_ARGS__)
#define DEBUG_PUT(fmt) LOG_SERVER_DEBUG_FORMAT(fmt, "mongo")
#else
#define DEBUG_MSG(fmt, ...)
#define DEBUG_PUT(fmt)
#endif

mongoc_client_t *mongo_connect(mongo_config_t *config);
bson_t *mongo_map2bson(apr_pool_t *pool, apr_table_t *map);
void mongo_disconnect(mongo_config_t *config, mongoc_client_t *client, mongoc_collection_t *collection);

// init MongoDB incl. memory allocation and open the pool
void mongo_initialize(mongo_config_t *config) {
	DEBUG_PUT("%s_initialize([apr_pool_t *], [mongo_config_t *])...");

	if ( config->uri == NULL ) {
		DEBUG_PUT("%s_initialize([apr_pool_t *], [mongo_config_t *]): URI is not set");
	} else {
		DEBUG_MSG("%s_initialize([apr_pool_t *], [mongo_config_t *]): Connection to: %s", config->uri);
		mongoc_uri_t *uri = mongoc_uri_new(config->uri);
		config->clientPool = mongoc_client_pool_new(uri);
		mongoc_uri_destroy(uri);
	}
	DEBUG_PUT("%s_initialize([apr_pool_t *], [mongo_config_t *])... DONE");
}

// destroy the mongo connection and clean up
void mongo_destroy(mongo_config_t *config) {
	DEBUG_PUT("%s_destroy([mongo_config_t *])...");
	if ( config->clientPool != NULL ) {
		mongoc_client_pool_destroy(config->clientPool);
	}
	DEBUG_PUT("%s_destroy([mongo_config_t *])... DONE");
}


// open the connection to the mongo
mongoc_client_t *mongo_connect(mongo_config_t *config) {
	DEBUG_PUT("%s_connect([mongo_config_t *])...");

	while ( config->clientPool == NULL ) {
		mongo_initialize(config);
	}
	mongoc_client_t *client = mongoc_client_pool_pop(config->clientPool);
	DEBUG_PUT("%s_connect([mongo_config_t *])... DONE");
	return client;
	
}

// get a collection for the client
mongoc_collection_t *mongo_getCollection(mongo_config_t *config, mongoc_client_t *client) {
	DEBUG_PUT("%s_getCollection([mongo_config_t *], [mongoc_client_t *])...");
	mongoc_collection_t *collection = mongoc_client_get_collection(client, config->db, config->collection);
	DEBUG_PUT("%s_getCollection([mongo_config_t *], [mongoc_client_t *])... DONE");
	return collection;
}

// unconnect the client and put it back to the pool
void mongo_disconnect(mongo_config_t *config, mongoc_client_t *client, mongoc_collection_t *collection) {
	DEBUG_PUT("%s_disconnect([mongo_config_t *], [mongoc_client_t *], [mongo_collection_t *])...");
	mongoc_collection_destroy(collection);
	mongoc_client_pool_push(config->clientPool, client);
	DEBUG_PUT("%s_disconnect([mongo_config_t *], [mongoc_client_t *], [mongo_collection_t *])... DONE");
}


// update mongo document
void mongo_update(mongo_config_t *config, apr_pool_t *pool, apr_table_t *oldMap, apr_table_t *newMap) {
	DEBUG_PUT("%s_update([mongo_config_t *], [apr_pool_t *], [apr_table_t *], [apr_table_t *])...");

	bson_error_t error;
	mongoc_client_t *client = mongo_connect(config);
	mongoc_collection_t *collection = mongo_getCollection(config, client);

	bson_t *newDoc = mongo_map2bson(pool, newMap);
	DEBUG_MSG("%s_update([mongo_config_t *], [apr_pool_t *], [apr_table_t *], [apr_table_t *]): New Document= %s", bson_as_json(newDoc, NULL));
	
	bson_t *oldDoc = mongo_map2bson(pool, oldMap);
	DEBUG_MSG("%s_update([mongo_config_t *], [apr_pool_t *], [apr_table_t *], [apr_table_t *]): Old Document= %s", bson_as_json(oldDoc, NULL));


	if ( !mongoc_collection_update(collection, MONGOC_UPDATE_NONE, oldDoc, newDoc, NULL, &error) ) {
		DEBUG_PUT("%s_update([mongo_config_t *], [apr_pool_t *], [apr_table_t *], [apr_table_t *]): was not able to insert document");
	}

	DEBUG_PUT("%s_update([mongo_config_t *], [apr_pool_t *], [apr_table_t *], [apr_table_t *]): done updating");
	bson_destroy(oldDoc);
	bson_destroy(newDoc);

	mongo_disconnect(config, client, collection);
	DEBUG_PUT("%s_update([mongo_config_t *], [apr_pool_t *], [apr_table_t *], [apr_table_t *])... DONE");
}

// commit a document to the database
char *mongo_commit(mongo_config_t *config, apr_pool_t *pool, apr_table_t *map) {
	LOGGING_DEBUG_S("START");
	bson_error_t error;
	bson_oid_t *oid;
	bson_t *doc;
	char *documentId;

	documentId = apr_pcalloc(pool, sizeof(char) * 25);

	mongoc_client_t *client = mongo_connect(config);
	mongoc_collection_t *collection = mongo_getCollection(config, client);
	
	oid = (bson_oid_t *) apr_palloc(pool, sizeof(bson_oid_t));
	doc = mongo_map2bson(pool, map);
	bson_oid_init(oid, NULL);
	BSON_APPEND_OID (doc, "_id", oid);

	if ( !mongoc_collection_insert(collection, MONGOC_INSERT_NONE, doc, NULL, &error) ) {
		bson_destroy(doc);
		LOGGING_ERROR_S("was not able to insert document");
		LOGGING_ERROR_S("DONE");
		return NULL;
	}

	bson_destroy(doc);
	mongo_disconnect(config, client, collection);

	bson_oid_to_string(oid, documentId);

	LOGGING_ERROR_S("DONE");
	return documentId;
}

// delete a mongo document
void mongo_delete(mongo_config_t *config, apr_pool_t *pool, apr_table_t *map) {
	DEBUG_PUT("%s_delete([mongo_config_t *], [apr_pool_t *], [apr_table_t *])...");

	bson_error_t error;
	mongoc_client_t *client = mongo_connect(config);
	mongoc_collection_t *collection = mongo_getCollection(config, client);

	bson_t *doc = mongo_map2bson(pool, map);
	DEBUG_MSG("%s_delete([mongo_config_t *], [apr_pool_t *], [apr_table_t *]): Delete Document=%s", bson_as_json(doc, NULL));

	if ( !mongoc_collection_remove(collection, MONGOC_REMOVE_SINGLE_REMOVE, doc, NULL, &error) ) {
		DEBUG_PUT("%s_delete([mongo_config_t *], [apr_pool_t *], [apr_table_t *]): was not able to delete document");
	}

	DEBUG_PUT("%s_delete([mongo_config_t *], [apr_pool_t *], [apr_table_t *]): done deleting");
	bson_destroy(doc);

	mongo_disconnect(config, client, collection);
	DEBUG_PUT("%s_delete([mongo_config_t *], [apr_pool_t *], [apr_table_t *])... DONE");
}

// query a document
mongo_cursor_t *mongo_query(mongo_config_t *config, apr_pool_t *pool, apr_table_t *map) {
	DEBUG_PUT("%s_query([mongo_config_t *], [apr_pool_t *], [apr_table_t *])...");

	bson_t *query;

	mongoc_client_t *client = mongo_connect(config);
	mongoc_collection_t *collection = mongo_getCollection(config, client);
	
	query = mongo_map2bson(pool, map);
	DEBUG_MSG("%s_query([mongo_config_t *], [apr_pool_t *], [apr_table_t *]): Querying: %s", bson_as_json(query, NULL));
	mongoc_cursor_t *cursor = mongoc_collection_find(collection, MONGOC_QUERY_NONE, 0, 0, 0, query,
	                                                 NULL, NULL);
	bson_destroy(query);
	
	mongo_cursor_t *cursorObj = apr_pcalloc(pool, sizeof(mongo_cursor_t));
	cursorObj->config = config;
	cursorObj->client = client;
	cursorObj->collection = collection;
	cursorObj->cursor = cursor;

	DEBUG_PUT("%s_query([mongo_config_t *], [apr_pool_t *], [apr_table_t *])... DONE");
	return cursorObj;
}

// count documents
int64_t mongo_count(mongo_config_t *config, apr_pool_t *pool, apr_table_t *map) {
	DEBUG_PUT("%s_count([mongo_config_t *], [apr_pool_t *], [apr_table_t *])...");

	bson_t *query;

	mongoc_client_t *client = mongo_connect(config);
	mongoc_collection_t *collection = mongo_getCollection(config, client);

	query = mongo_map2bson(pool, map);
	DEBUG_MSG("%s_count([mongo_config_t *], [apr_pool_t *], [apr_table_t *]): Querying: %s", bson_as_json(query, NULL));
	int64_t count = mongoc_collection_count (collection, MONGOC_QUERY_NONE, query, 0, 0, NULL, NULL);
	DEBUG_MSG("%s_count([mongo_config_t *], [apr_pool_t *], [apr_table_t *]): Count: %ld", count);
	bson_destroy(query);
	
	DEBUG_PUT("%s_count([mongo_config_t *], [apr_pool_t *], [apr_table_t *])... DONE");
	return count;
}

// destroy the db cursor
void mongo_destroyCursor(mongo_cursor_t *cursorObj) {
	DEBUG_PUT("%s_destroy_cursor([mongoc_cursor_t *])...");

	if ( cursorObj != NULL ) {
		mongoc_cursor_destroy(cursorObj->cursor);
		mongo_disconnect(cursorObj->config, cursorObj->client, cursorObj->collection);
	}
	DEBUG_PUT("%s_destroy_cursor([mongoc_cursor_t *])... DONE");
}

// create bson object from map
bson_t *mongo_map2bson(apr_pool_t *pool, apr_table_t *map) {
	DEBUG_PUT("%s_map2bson([apr_pool_t *], [apr_table_t *])...");

	bson_t *bson;
	char *json = jsonhandling_aprmap2json(pool, map);

	DEBUG_MSG("%s_map2bson([apr_pool_t *], [apr_table_t *]): json=%s", json);
	bson = bson_new_from_json((const uint8_t *) json, strlen(json), NULL);

	DEBUG_PUT("%s_map2bson([apr_pool_t *], [apr_table_t *])... DONE");
	return bson;
}

// transform a bson to a json hashmap
void mongo_bson2map(apr_table_t *map, const bson_t *bson) {
	DEBUG_PUT("%s_bson2map([apr_table_t *], [bson_t *])...");

	char *str = bson_as_json(bson, NULL);
	DEBUG_MSG("%s_bson2map([apr_table_t *], [bson_t *]): str=%s", str);
	jsonhandling_json2aprmap(map, str);

	if ( str != NULL ) {
		bson_free(str);
	}

	DEBUG_PUT("%s_bson2map([apr_table_t *], [bson_t *])... DONE");
}


uint8_t mongo_isOidValid(char *oid) {
	if ( oid == NULL ) {
		return 0;
	}
	if ( strlen(oid) != 24 ) {
		return 0;
	}

	char *ptr = oid;
	while ( *ptr != '\0' ) {
		if ( !( ( *ptr >= '0' && *ptr <= '9' ) || ( *ptr >= 'a' && *ptr <= 'f' ) ) ) {
			return 0;
		}
		ptr++;
	}
	return 1;
}