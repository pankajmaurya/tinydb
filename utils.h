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

void debug_all_entries(char* key) {
    if (!kvstore->index_file) return;
    
    printf("[DEBUG] All entries for key '%s':\n", key);
    fseek(kvstore->index_file, 0, SEEK_SET);
    
    while (!feof(kvstore->index_file)) {
        DataEntry* entry = read_index_entry_from_file(kvstore->index_file);
        if (!entry) break;
        
        if (strcmp(entry->key, key) == 0) {
            printf("[DEBUG]   Found at position %d\n", entry->position);
        } else {
            printf("[DEBUG]   NOT Found at position %d\n", entry->position);
		}

        free_entry(entry);
    }
}

// Find key in index file
int find_key_in_index(FILE* index_file, char* key) {
	// debug_all_entries(key);
    if (!index_file) return -1;
    
    fseek(index_file, 0, SEEK_SET);
    int last_position = -1;  // Track the LAST occurrence
    
    while (!feof(index_file)) {
        DataEntry* index_entry = read_index_entry_from_file(index_file);
        if (!index_entry) break;
		// printf("[DEBUG] Fetched Data Entry - key: %s, pos: %d\n",
			// index_entry->key, index_entry->position);
        
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



