#include "cmongolib.h"

#define LOGGING_LEVEL 5

#define LOGGING_ERROR(fmt, ...) { fprintf(stderr, "ERROR: %s:%d - %s: ", __FILE__, __LINE__, __FUNCTION__); fprintf(stderr, fmt, ##__VA_ARGS__); fprintf(stderr,"\n"); }
#define LOGGING_WARN(fmt, ...) if ( LOGGING_LEVEL > 0 ) { fprintf(stderr, "WARN: %s:%d - %s: ", __FILE__, __LINE__, __FUNCTION__); fprintf(stderr, fmt, ##__VA_ARGS__); fprintf(stderr,"\n"); }
#define LOGGING_INFO(fmt, ...) if ( LOGGING_LEVEL > 1 ) { fprintf(stderr, "INFO: %s:%d - %s: ", __FILE__, __LINE__, __FUNCTION__); fprintf(stderr, fmt, ##__VA_ARGS__); fprintf(stderr,"\n"); }
#define LOGGING_DEBUG(fmt, ...) if ( LOGGING_LEVEL > 2 ) { fprintf(stderr, "DEBUG: %s:%d - %s: ", __FILE__, __LINE__, __FUNCTION__); fprintf(stderr, fmt, ##__VA_ARGS__); fprintf(stderr, "\n"); }
#define LOGGING_TRACE(fmt, ...) if ( LOGGING_LEVEL > 3 ) { fprintf(stderr, "TRACE: %s:%d - %s: ", __FILE__, __LINE__, __FUNCTION__); fprintf(stderr, fmt, ##__VA_ARGS__); fprintf(stderr, "\n"); }


void mongo_initialize(mongo_config_t *config) {
	LOGGING_DEBUG("START");

	if ( config->uri == NULL ) {
		LOGGING_ERROR("URI is not set");
	} else {
		LOGGING_INFO("Connection to: %s", config->uri);
		mongoc_uri_t *uri = mongoc_uri_new(config->uri);
		config->clientPool = mongoc_client_pool_new(uri);
		mongoc_uri_destroy(uri);
	}
	LOGGING_DEBUG("DONE");
}

int main (int argc, char **argv) {
	printf("test\n");
	return 0;
}