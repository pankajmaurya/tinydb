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
	int original_index;  // Useful during sorting, not to be persisted

} DataRecord;

// Data Entry structure for in-memory operations
typedef struct {
    int kLen;
    int position; // Position in heap file 
    char* key;
} DataEntry;

// Memtable index entry for heap file
typedef struct MemtableEntry {
    char* key;
    int position;           // Position in heap file
    struct MemtableEntry* next;
} MemtableEntry;

// Hash table for memtable index
#define MEMTABLE_HASH_SIZE 1024
typedef struct {
    MemtableEntry* buckets[MEMTABLE_HASH_SIZE];
    int count;
} MemtableIndex;

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
	MemtableIndex* memtable_index;  // In-memory index for heap file
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

// Memtable functions
MemtableIndex* memtable_init();
void memtable_cleanup(MemtableIndex* memtable);
void memtable_put(MemtableIndex* memtable, char* key, int position);
int memtable_get(MemtableIndex* memtable, char* key);
void memtable_delete(MemtableIndex* memtable, char* key);
unsigned int hash_key(char* key);
void rebuild_memtable_from_heap(KVStore* kvstore);

#endif // KVSTORE_H
