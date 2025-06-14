#include "kvstore.h"
#include <sys/stat.h>
#include <sys/types.h>

// Helper function to create a data record
DataEntry* create_entry(char* key, int position) {
    DataEntry* record = malloc(sizeof(DataEntry));
    record->kLen = strlen(key);
    record->key = strdup(key);
    record->position = position;
    return record;
}

// Write an index entry to file
void write_index_entry_to_file(FILE* file, DataRecord* record) {
    fwrite(&record->kLen, sizeof(int), 1, file);
    fwrite(&record->position, sizeof(int), 1, file);
    fwrite(record->key, sizeof(char), record->kLen, file);
    fflush(file);
}

// Read an index entry from file
DataEntry* read_index_entry_from_file(FILE* file) {
    int kLen, vPos;
    if (fread(&kLen, sizeof(int), 1, file) != 1) return NULL;
    if (fread(&vPos, sizeof(int), 1, file) != 1) return NULL;
    
    char* key = malloc(kLen + 1);
    if (fread(key, sizeof(char), (size_t) kLen, file) != (size_t) kLen) {
        free(key);
        return NULL;
    }
    key[kLen] = '\0';
    
    DataEntry* record = create_entry(key, vPos);
    free(key);
    return record;
}

// Helper function to free a data record
void free_entry(DataEntry* record) {
    if (record) {
        free(record->key);
        free(record);
    }
}

// Find key in index file
int find_key_in_index(FILE* index_file, char* key) {
    if (!index_file) return -1;
    
    fseek(index_file, 0, SEEK_SET);
    int last_position = -1;  // Track the LAST occurrence
    
    while (!feof(index_file)) {
        DataEntry* index_entry = read_index_entry_from_file(index_file);
        if (!index_entry) break;
        
        if (strcmp(index_entry->key, key) == 0) {
            last_position = index_entry->position;
			printf("DATA ENTRY MATCHES, saved position %d\n", last_position);
            // DON'T break here - continue to find more recent entries
        } else {
			// printf("DATA ENTRY MISMATCH\n");
		}
        free_entry(index_entry);
    }
    
    return last_position;
}



