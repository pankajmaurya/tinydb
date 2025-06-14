#ifndef KVSTORE_H
#define KVSTORE_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <dirent.h>
#include <unistd.h>
#include <pthread.h>

// Compaction status constants
#define COMPACTION_COMPLETED 0
#define COMPACTION_STARTED 1

// Default compaction threshold (64KB)
#define DEFAULT_COMPACTION_THRESHOLD (64 * 1024)

// File naming constants
#define HEAP_FILE_NAME "heap.dat"
#define INDEX_FILE_NAME "index.dat"
#define SSTABLE_PREFIX "sstable_"
#define SSTABLE_INDEX_PREFIX "sstable_index_"

// Data record structure for in-memory operations
typedef struct {
    int kLen;
    int vLen;  // -1 for tombstone
    char* key;
    char* value;  // NULL for tombstone
    int position; // Position in heap file (for index entries)
} DataRecord;

// Data Entry structure for in-memory operations
typedef struct {
    int kLen;
    int position; // Position in heap file 
    char* key;
} DataEntry;


// SSTable metadata
typedef struct SSTable {
    char* filename;
    char* index_filename;
    int record_count;
    struct SSTable* next;
} SSTable;

// Main KVStore structure
typedef struct {
    char* data_directory;
    FILE* heap_file;
    FILE* index_file;
    SSTable* sstables;
    long heap_size;
    int compaction_threshold;
    pthread_t compaction_thread;
    int compaction_status;
    pthread_mutex_t store_mutex;
} KVStore;

// Global KVStore instance
extern KVStore* kvstore;

// Public API functions
void init(char* data_directory);
void put(char* key, char* value);
char* get(char* key);
char* debug_get(char* key);
void delete(char* key);
void compact();
int getCompactionStatus();

// Cleanup function
void cleanup();

#endif // KVSTORE_H
