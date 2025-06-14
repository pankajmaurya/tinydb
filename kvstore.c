#include "kvstore.h"
#include <sys/stat.h>
#include <sys/types.h>
#include "utils.h"
#include "data_record.h"
#include "sstable.h"

// Global KVStore instance
KVStore* kvstore = NULL;

// Get current heap file size
long get_heap_size() {
    if (!kvstore->heap_file) return 0;
    
    long current_pos = ftell(kvstore->heap_file);
    fseek(kvstore->heap_file, 0, SEEK_END);
    long size = ftell(kvstore->heap_file);
    fseek(kvstore->heap_file, current_pos, SEEK_SET);
    return size;
}

// Comparison function that considers both key and original order
int compare_records_stable(const void* a, const void* b) {
    DataRecord* record_a = *(DataRecord**)a;
    DataRecord* record_b = *(DataRecord**)b;
    
    int key_cmp = strcmp(record_a->key, record_b->key);
    if (key_cmp != 0) {
        return key_cmp;  // Different keys, sort by key
    }
    
    // Same key: sort by original order (newer records should come later)
    // We'll use the original_index field to maintain chronological order
    return record_a->original_index - record_b->original_index;
}

// Compaction worker thread
void* compaction_worker(void* arg) {
    pthread_mutex_lock(&kvstore->store_mutex);
    
    // Close current files
    if (kvstore->heap_file) {
        fclose(kvstore->heap_file);
        kvstore->heap_file = NULL;
    }
    if (kvstore->index_file) {
        fclose(kvstore->index_file);
        kvstore->index_file = NULL;
    }
    
    // Create SSTable filename with timestamp
    static int sstable_counter = 0;
    char sstable_filename[256];
    char sstable_index_filename[256];
    
    sprintf(sstable_filename, "%s/%s%d.dat", kvstore->data_directory, SSTABLE_PREFIX, sstable_counter);
    sprintf(sstable_index_filename, "%s/%s%d.dat", kvstore->data_directory, SSTABLE_INDEX_PREFIX, sstable_counter);
    sstable_counter++;
    
    // Read all records from heap file
    char heap_path[256];
    sprintf(heap_path, "%s/%s", kvstore->data_directory, HEAP_FILE_NAME);
    
    FILE* old_heap = fopen(heap_path, "rb");
    DataRecord** records = NULL;
    int record_count = 0;
    int capacity = 100;
    
    if (old_heap) {
        records = malloc(capacity * sizeof(DataRecord*));
        
        int original_index = 0;  // Track original order
        while (!feof(old_heap)) {
            long pos = ftell(old_heap);
            DataRecord* record = read_record_from_file(old_heap, pos);
            if (!record) break;
            
            // Store original order to maintain chronological sequence
            record->original_index = original_index++;
            
            if (record_count >= capacity) {
                capacity *= 2;
                records = realloc(records, capacity * sizeof(DataRecord*));
            }
            
            records[record_count++] = record;
        }
        fclose(old_heap);
        
        // Sort records by key, maintaining chronological order for same keys
        qsort(records, record_count, sizeof(DataRecord*), compare_records_stable);
        
        // Write sorted SSTable and index
        FILE* sstable_file = fopen(sstable_filename, "wb");
        FILE* sstable_index_file = fopen(sstable_index_filename, "wb");
        
        // Declare unique_count in proper scope
        int unique_count = 0;
        
        if (sstable_file && sstable_index_file) {
            // Remove duplicates, keeping the latest entry (highest original_index)
            // Since records are sorted by key, we can process linearly
            DataRecord** unique_records = malloc(record_count * sizeof(DataRecord*));
            
            for (int i = 0; i < record_count; i++) {
                // Check if next record has different key (or if we're at the end)
                int is_last_of_key = (i == record_count - 1) || 
                                     (strcmp(records[i]->key, records[i + 1]->key) != 0);
                
                if (is_last_of_key) {
                    unique_records[unique_count++] = records[i];
                }
            }
            
            // Write unique records to SSTable
            for (int i = 0; i < unique_count; i++) {
                long pos = ftell(sstable_file);
                unique_records[i]->position = pos;
                write_record_to_file(sstable_file, unique_records[i]);
                write_index_entry_to_file(sstable_index_file, unique_records[i]);
            }
            
            free(unique_records);
        }
        
        if (sstable_file) fclose(sstable_file);
        if (sstable_index_file) fclose(sstable_index_file);
        
        // Add new SSTable to list
        SSTable* new_sstable = malloc(sizeof(SSTable));
        new_sstable->filename = strdup(sstable_filename);
        new_sstable->index_filename = strdup(sstable_index_filename);
        new_sstable->record_count = unique_count;  // Use unique_count for accurate count
        new_sstable->next = kvstore->sstables;
        kvstore->sstables = new_sstable;
        
        // Cleanup
        for (int i = 0; i < record_count; i++) {
            free_record(records[i]);
        }
        free(records);
    }
    
    // Create new heap and index files
    sprintf(heap_path, "%s/%s", kvstore->data_directory, HEAP_FILE_NAME);
    char index_path[256];
    sprintf(index_path, "%s/%s", kvstore->data_directory, INDEX_FILE_NAME);
    
    // Truncate the heap file and index file
    kvstore->heap_file = fopen(heap_path, "wb");
    kvstore->index_file = fopen(index_path, "wb");
    fclose(kvstore->heap_file);
    fclose(kvstore->index_file);
    kvstore->heap_size = 0;

    // Reopen in append mode for future writes
    kvstore->heap_file = fopen(heap_path, "a+b");
    kvstore->index_file = fopen(index_path, "a+b");
    
    kvstore->compaction_status = COMPACTION_COMPLETED;
    pthread_mutex_unlock(&kvstore->store_mutex);
    
    return NULL;
}

// Initialize the KVStore
void init(char* data_directory) {
    kvstore = malloc(sizeof(KVStore));
    kvstore->data_directory = strdup(data_directory);
    kvstore->heap_file = NULL;
    kvstore->index_file = NULL;
    kvstore->sstables = NULL;
    kvstore->heap_size = 0;
    kvstore->compaction_threshold = DEFAULT_COMPACTION_THRESHOLD;
    kvstore->compaction_status = COMPACTION_COMPLETED;
    
    pthread_mutex_init(&kvstore->store_mutex, NULL);
    
    // Create directory if it doesn't exist
    mkdir(data_directory, 0755);
    
    // Load existing SSTables
    load_sstables(kvstore);
    
    // Open or create heap and index files
    char heap_path[256];
    char index_path[256];
    sprintf(heap_path, "%s/%s", data_directory, HEAP_FILE_NAME);
    sprintf(index_path, "%s/%s", data_directory, INDEX_FILE_NAME);
    
    kvstore->heap_file = fopen(heap_path, "a+b");
    kvstore->index_file = fopen(index_path, "a+b");
    
    if (kvstore->heap_file) {
        kvstore->heap_size = get_heap_size();
    }
}

// Write a key-value pair
void put(char* key, char* value) {
    if (!kvstore || !kvstore->heap_file || !kvstore->index_file) return;
    
    pthread_mutex_lock(&kvstore->store_mutex);
    
    long position = ftell(kvstore->heap_file);
    DataRecord* record = create_record(key, value, position);
    
    write_record_to_file(kvstore->heap_file, record);
    write_index_entry_to_file(kvstore->index_file, record);
    
    kvstore->heap_size = get_heap_size();
    
    // Check if compaction is needed
    if (kvstore->heap_size > kvstore->compaction_threshold && 
        kvstore->compaction_status == COMPACTION_COMPLETED) {
        compact();
    }
    
    free_record(record);
    pthread_mutex_unlock(&kvstore->store_mutex);
}

// Get value for a key
char* get(char* key) {
    if (!kvstore) return NULL;
    
    pthread_mutex_lock(&kvstore->store_mutex);
    
    // First check heap file (most recent)
    if (kvstore->index_file) {
        int position = find_key_in_index(kvstore->index_file, key);
        if (position != -1) {
            DataRecord* record = read_record_from_file(kvstore->heap_file, position);
            if (record) {
                char* result = NULL;
                if (record->vLen >= 0) {
                    result = strdup(record->value ? record->value : "");
                }
                free_record(record);
                pthread_mutex_unlock(&kvstore->store_mutex);
                return result;
            }
        }
    }
    
    // Then check SSTables (older data)
    SSTable* current = kvstore->sstables;
    while (current) {
        char* result = search_sstable(current->filename, current->index_filename, key);
        if (result) {
            pthread_mutex_unlock(&kvstore->store_mutex);
            return result;
        }
        current = current->next;
    }
    
    pthread_mutex_unlock(&kvstore->store_mutex);
    return NULL;
}

// Get value for a key with comprehensive debugging
char* debug_get(char* key) {
    printf("[DEBUG] get() called with key: '%s'\n", key ? key : "(null)");
    
    if (!kvstore) {
        printf("[DEBUG] kvstore is NULL, returning NULL\n");
        return NULL;
    }
    
    printf("[DEBUG] kvstore found, acquiring mutex\n");
    pthread_mutex_lock(&kvstore->store_mutex);
    
    printf("[DEBUG] mutex acquired, checking heap file\n");
    
    // First check heap file (most recent)
    if (kvstore->index_file) {
        printf("[DEBUG] index_file exists, searching for key in index\n");
        int position = find_key_in_index(kvstore->index_file, key);
        printf("[DEBUG] find_key_in_index returned position: %d\n", position);
        
        if (position != -1) {
            printf("[DEBUG] key found in index at position %d, reading record from heap\n", position);
            DataRecord* record = read_record_from_file(kvstore->heap_file, position);
            
            if (record) {
                printf("[DEBUG] record read successfully:\n");
                printf("[DEBUG]   - kLen: %d\n", record->kLen);
                printf("[DEBUG]   - vLen: %d\n", record->vLen);
                printf("[DEBUG]   - key: '%s'\n", record->key ? record->key : "(null)");
                printf("[DEBUG]   - value: '%s'\n", record->value ? record->value : "(null)");
                
                char* result = NULL;
                if (record->vLen > 0) {
                    printf("[DEBUG] record is valid, duplicating value\n");
                    result = strdup(record->value);
                    printf("[DEBUG] strdup result: '%s'\n", result ? result : "(null)");
                } else {
                    if (record->vLen <= 0) {
                        printf("[DEBUG] record has vLen <= 0, treating as tombstone\n");
                    }
                }
                
                free_record(record);
                pthread_mutex_unlock(&kvstore->store_mutex);
                printf("[DEBUG] returning from heap search with result: '%s'\n", result ? result : "(null)");
                return result;
            } else {
                printf("[DEBUG] read_record_from_file returned NULL for position %d\n", position);
            }
        } else {
            printf("[DEBUG] key not found in index\n");
        }
    } else {
        printf("[DEBUG] index_file is NULL\n");
    }
    
    printf("[DEBUG] checking SSTables\n");
    
    // Then check SSTables (older data)
    SSTable* current = kvstore->sstables;
    int sstable_count = 0;
    
    while (current) {
        printf("[DEBUG] checking SSTable #%d: %s (index: %s)\n", 
               sstable_count, 
               current->filename ? current->filename : "(null)",
               current->index_filename ? current->index_filename : "(null)");
        
        char* result = search_sstable(current->filename, current->index_filename, key);
        printf("[DEBUG] search_sstable returned: '%s'\n", result ? result : "(null)");
        
        if (result) {
            printf("[DEBUG] found result in SSTable #%d, returning: '%s'\n", sstable_count, result);
            pthread_mutex_unlock(&kvstore->store_mutex);
            return result;
        }
        
        current = current->next;
        sstable_count++;
    }
    
    if (sstable_count == 0) {
        printf("[DEBUG] no SSTables found\n");
    } else {
        printf("[DEBUG] searched %d SSTables, key not found\n", sstable_count);
    }
    
    pthread_mutex_unlock(&kvstore->store_mutex);
    printf("[DEBUG] get() returning NULL - key not found anywhere\n");
    return NULL;
}

// Delete a key
void delete(char* key) {
    if (!kvstore || !kvstore->heap_file || !kvstore->index_file) return;
    
    pthread_mutex_lock(&kvstore->store_mutex);
    
    long position = ftell(kvstore->heap_file);
    DataRecord* tombstone = create_record(key, NULL, position);
	printf("[DEBUG] Creating tombstone record for key %s with heap offset at position %ld\n", key, position);
    
    write_record_to_file(kvstore->heap_file, tombstone);
	printf("[DEBUG] Written tombstone to heap file\n");
    write_index_entry_to_file(kvstore->index_file, tombstone);
	printf("[DEBUG] Written index entry for tombstone\n");
	debug_all_entries(key);
    
    kvstore->heap_size = get_heap_size();
    
    // Check if compaction is needed
    if (kvstore->heap_size > kvstore->compaction_threshold && 
        kvstore->compaction_status == COMPACTION_COMPLETED) {
		printf("[DEBUG] Triggering compactiong...(heap file too big)\n");
        compact();
    }
    
    free_record(tombstone);
    pthread_mutex_unlock(&kvstore->store_mutex);
	printf("[DEBUG] Deletion complete\n");
}

// Trigger compaction
void compact() {
    if (!kvstore || kvstore->compaction_status == COMPACTION_STARTED) return;
    
    kvstore->compaction_status = COMPACTION_STARTED;
    pthread_create(&kvstore->compaction_thread, NULL, compaction_worker, NULL);
    pthread_detach(kvstore->compaction_thread);
}

// Get compaction status
int getCompactionStatus() {
    return kvstore ? kvstore->compaction_status : COMPACTION_COMPLETED;
}

// Cleanup function
void cleanup() {
    if (!kvstore) return;
    
    pthread_mutex_lock(&kvstore->store_mutex);
    
    if (kvstore->heap_file) {
        fclose(kvstore->heap_file);
        kvstore->heap_file = NULL;
    }
    
    if (kvstore->index_file) {
        fclose(kvstore->index_file);
        kvstore->index_file = NULL;
    }
    
    // Free SSTable list
    SSTable* current = kvstore->sstables;
    while (current) {
        SSTable* next = current->next;
        free(current->filename);
        free(current->index_filename);
        free(current);
        current = next;
    }
    
    free(kvstore->data_directory);
    pthread_mutex_unlock(&kvstore->store_mutex);
    pthread_mutex_destroy(&kvstore->store_mutex);
    free(kvstore);
    kvstore = NULL;
}
