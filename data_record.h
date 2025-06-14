#include "kvstore.h"
#include <sys/stat.h>
#include <sys/types.h>

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

// Comparison function for sorting records
int compare_records(const void* a, const void* b) {
    DataRecord* rec_a = *(DataRecord**)a;
    DataRecord* rec_b = *(DataRecord**)b;
    return strcmp(rec_a->key, rec_b->key);
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

// Helper function to free a data record
void free_record(DataRecord* record) {
    if (record) {
        free(record->key);
        free(record->value);
        free(record);
    }
}


