#include "kvstore.h"
#include <sys/stat.h>
#include <sys/types.h>

// Global KVStore instance
KVStore* kvstore = NULL;

// Helper function to create a data record
DataRecord* create_record(char* key, char* value, int position) {
    DataRecord* record = malloc(sizeof(DataRecord));
    record->kLen = strlen(key);
    record->vLen = value ? strlen(value) : -1;
    record->key = strdup(key);
    record->value = value ? strdup(value) : NULL;
    record->position = position;
    return record;
}

// Helper function to free a data record
void free_record(DataRecord* record) {
    if (record) {
        free(record->key);
        free(record->value);
        free(record);
    }
}

// Write a data record to file
void write_record_to_file(FILE* file, DataRecord* record) {
    fwrite(&record->kLen, sizeof(int), 1, file);
    fwrite(&record->vLen, sizeof(int), 1, file);
    fwrite(record->key, sizeof(char), record->kLen, file);
    if (record->vLen > 0) {
        fwrite(record->value, sizeof(char), record->vLen, file);
    }
    fflush(file);
}

// Write an index entry to file
void write_index_entry_to_file(FILE* file, DataRecord* record) {
    fwrite(&record->kLen, sizeof(int), 1, file);
    fwrite(&record->position, sizeof(int), 1, file);
    fwrite(record->key, sizeof(char), record->kLen, file);
    fflush(file);
}

// Read a data record from file
DataRecord* read_record_from_file(FILE* file, int position) {
    if (fseek(file, position, SEEK_SET) != 0) return NULL;
    
    int kLen, vLen;
    if (fread(&kLen, sizeof(int), 1, file) != 1) return NULL;
    if (fread(&vLen, sizeof(int), 1, file) != 1) return NULL;
    
    char* key = malloc(kLen + 1);
    if (fread(key, sizeof(char), (size_t) kLen, file) != (size_t) kLen) {
        free(key);
        return NULL;
    }
    key[kLen] = '\0';
    
    char* value = NULL;
    if (vLen > 0) {
        value = malloc(vLen + 1);
        if (fread(value, sizeof(char), (size_t) vLen, file) != (size_t) vLen) {
            free(key);
            free(value);
            return NULL;
        }
        value[vLen] = '\0';
    }
    
    DataRecord* record = create_record(key, value, position);
    free(key);
    free(value);
    return record;
}

// Read an index entry from file
DataRecord* read_index_entry_from_file(FILE* file) {
    int kLen, vPos;
    if (fread(&kLen, sizeof(int), 1, file) != 1) return NULL;
    if (fread(&vPos, sizeof(int), 1, file) != 1) return NULL;
    
    char* key = malloc(kLen + 1);
    if (fread(key, sizeof(char), (size_t) kLen, file) != (size_t) kLen) {
        free(key);
        return NULL;
    }
    key[kLen] = '\0';
    
    DataRecord* record = create_record(key, NULL, vPos);
    free(key);
    return record;
}

// Find key in index file
int find_key_in_index(FILE* index_file, char* key) {
    if (!index_file) return -1;
    
    fseek(index_file, 0, SEEK_SET);
    int last_position = -1;  // Track the LAST occurrence
    
    while (!feof(index_file)) {
        DataRecord* index_entry = read_index_entry_from_file(index_file);
        if (!index_entry) break;
        
        if (strcmp(index_entry->key, key) == 0) {
            last_position = index_entry->position;
            // DON'T break here - continue to find more recent entries
        }
        free_record(index_entry);
    }
    
    return last_position;
}

// Search for key in SSTable
char* search_sstable(char* sstable_file, char* index_file, char* key) {
    FILE* idx_file = fopen(index_file, "rb");
    if (!idx_file) return NULL;
    
    int position = find_key_in_index(idx_file, key);
    fclose(idx_file);
    
    if (position == -1) return NULL;
    
    FILE* data_file = fopen(sstable_file, "rb");
    if (!data_file) return NULL;
    
    DataRecord* record = read_record_from_file(data_file, position);
    fclose(data_file);
    
    if (!record) return NULL;
    
    char* result = NULL;
    if (record->vLen > 0) {
        result = strdup(record->value);
    }
    
    free_record(record);
    return result;
}

// Load existing SSTables
void load_sstables() {
    DIR* dir = opendir(kvstore->data_directory);
    if (!dir) return;
    
    struct dirent* entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strncmp(entry->d_name, SSTABLE_PREFIX, strlen(SSTABLE_PREFIX)) == 0 &&
            strstr(entry->d_name, ".dat") != NULL) {
            
            SSTable* sstable = malloc(sizeof(SSTable));
            sstable->filename = malloc(strlen(kvstore->data_directory) + strlen(entry->d_name) + 2);
            sprintf(sstable->filename, "%s/%s", kvstore->data_directory, entry->d_name);
            
            // Generate corresponding index filename
            char index_name[256];
            strcpy(index_name, entry->d_name);
            char* dot = strrchr(index_name, '.');
            if (dot) *dot = '\0';
            
            char* sstable_part = strstr(index_name, SSTABLE_PREFIX);
            if (sstable_part) {
                memmove(index_name + strlen(SSTABLE_INDEX_PREFIX), 
                       sstable_part + strlen(SSTABLE_PREFIX),
                       strlen(sstable_part + strlen(SSTABLE_PREFIX)) + 1);
                memcpy(index_name, SSTABLE_INDEX_PREFIX, strlen(SSTABLE_INDEX_PREFIX));
            }
            strcat(index_name, ".dat");
            
            sstable->index_filename = malloc(strlen(kvstore->data_directory) + strlen(index_name) + 2);
            sprintf(sstable->index_filename, "%s/%s", kvstore->data_directory, index_name);
            
            sstable->record_count = 0;
            sstable->next = kvstore->sstables;
            kvstore->sstables = sstable;
        }
    }
    closedir(dir);
}

// Get current heap file size
long get_heap_size() {
    if (!kvstore->heap_file) return 0;
    
    long current_pos = ftell(kvstore->heap_file);
    fseek(kvstore->heap_file, 0, SEEK_END);
    long size = ftell(kvstore->heap_file);
    fseek(kvstore->heap_file, current_pos, SEEK_SET);
    return size;
}

// Comparison function for sorting records
int compare_records(const void* a, const void* b) {
    DataRecord* rec_a = *(DataRecord**)a;
    DataRecord* rec_b = *(DataRecord**)b;
    return strcmp(rec_a->key, rec_b->key);
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
        
        while (!feof(old_heap)) {
            long pos = ftell(old_heap);
            DataRecord* record = read_record_from_file(old_heap, pos);
            if (!record) break;
            
            if (record_count >= capacity) {
                capacity *= 2;
                records = realloc(records, capacity * sizeof(DataRecord*));
            }
            
            records[record_count++] = record;
        }
        fclose(old_heap);
        
        // Sort records by key
        qsort(records, record_count, sizeof(DataRecord*), compare_records);
        
        // Write sorted SSTable and index
        FILE* sstable_file = fopen(sstable_filename, "wb");
        FILE* sstable_index_file = fopen(sstable_index_filename, "wb");
        
        if (sstable_file && sstable_index_file) {
            // Remove duplicates, keeping the latest entry
            DataRecord** unique_records = malloc(record_count * sizeof(DataRecord*));
            int unique_count = 0;
            
            for (int i = 0; i < record_count; i++) {
                // Skip if we've seen this key before (keep the last occurrence)
                int skip = 0;
                for (int j = i + 1; j < record_count; j++) {
                    if (strcmp(records[i]->key, records[j]->key) == 0) {
                        skip = 1;
                        break;
                    }
                }
                
                if (!skip) {
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
        new_sstable->record_count = record_count;
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
    
    kvstore->heap_file = fopen(heap_path, "wb");
    kvstore->index_file = fopen(index_path, "wb");
    kvstore->heap_size = 0;
    
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
    load_sstables();
    
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
                if (record->vLen > 0) {
                    result = strdup(record->value);
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
    
    write_record_to_file(kvstore->heap_file, tombstone);
    write_index_entry_to_file(kvstore->index_file, tombstone);
    
    kvstore->heap_size = get_heap_size();
    
    // Check if compaction is needed
    if (kvstore->heap_size > kvstore->compaction_threshold && 
        kvstore->compaction_status == COMPACTION_COMPLETED) {
        compact();
    }
    
    free_record(tombstone);
    pthread_mutex_unlock(&kvstore->store_mutex);
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
