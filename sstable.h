#include "kvstore.h"
#include <sys/stat.h>
#include <sys/types.h>
// Load existing SSTables

void load_sstables(KVStore* kvstore) {
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


