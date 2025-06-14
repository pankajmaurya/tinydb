#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

// File naming constants (matching kvstore.h)
#define HEAP_FILE_NAME "heap.dat"
#define INDEX_FILE_NAME "index.dat"
#define SSTABLE_PREFIX "sstable_"

// ANSI color codes for better output formatting
#define COLOR_RESET   "\033[0m"
#define COLOR_BOLD    "\033[1m"
#define COLOR_RED     "\033[31m"
#define COLOR_GREEN   "\033[32m"
#define COLOR_YELLOW  "\033[33m"
#define COLOR_BLUE    "\033[34m"
#define COLOR_MAGENTA "\033[35m"
#define COLOR_CYAN    "\033[36m"

// Statistics structure
typedef struct {
    int total_records;
    int live_records;
    int tombstone_records;
    long total_key_bytes;
    long total_value_bytes;
    long file_size;
} FileStats;

// Print a separator line
void print_separator(char c, int length) {
    for (int i = 0; i < length; i++) {
        printf("%c", c);
    }
    printf("\n");
}

// Print file header
void print_file_header(const char* filename, const char* file_type) {
    printf("\n");
    print_separator('=', 80);
    printf("%s%s FILE: %s%s\n", COLOR_BOLD, file_type, filename, COLOR_RESET);
    print_separator('=', 80);
}

// Print record in formatted way
void print_record(int record_num, int kLen, int vLen, const char* key, const char* value, long position) {
    printf("%s[Record #%d]%s Position: %ld\n", COLOR_CYAN, record_num, COLOR_RESET, position);
    printf("  Key Length:   %d\n", kLen);
    printf("  Value Length: %d", vLen);
    
    if (vLen == -1) {
        printf(" %s(TOMBSTONE)%s\n", COLOR_RED, COLOR_RESET);
    } else {
        printf("\n");
    }
    
    printf("  Key:          \"%s%s%s\"\n", COLOR_YELLOW, key, COLOR_RESET);
    
    if (vLen > 0) {
        printf("  Value:        \"%s%s%s\"\n", COLOR_GREEN, value, COLOR_RESET);
    } else if (vLen == -1) {
        printf("  Value:        %s<DELETED>%s\n", COLOR_RED, COLOR_RESET);
    } else {
        printf("  Value:        %s<EMPTY>%s\n", COLOR_MAGENTA, COLOR_RESET);
    }
    printf("\n");
}

// Print index entry
void print_index_entry(int entry_num, int kLen, int position, const char* key) {
    printf("%s[Index Entry #%d]%s\n", COLOR_CYAN, entry_num, COLOR_RESET);
    printf("  Key Length:   %d\n", kLen);
    printf("  Data Position: %d\n", position);
    printf("  Key:          \"%s%s%s\"\n", COLOR_YELLOW, key, COLOR_RESET);
    printf("\n");
}

// Read and dump a data file (heap file or SSTable)
FileStats dump_data_file(const char* filepath, const char* file_type) {
    FileStats stats = {0, 0, 0, 0, 0, 0};
    
    FILE* file = fopen(filepath, "rb");
    if (!file) {
        printf("%sError: Cannot open file %s%s\n", COLOR_RED, filepath, COLOR_RESET);
        return stats;
    }
    
    // Get file size
    fseek(file, 0, SEEK_END);
    stats.file_size = ftell(file);
    fseek(file, 0, SEEK_SET);
    
    print_file_header(filepath, file_type);
    printf("File Size: %ld bytes\n\n", stats.file_size);
    
    int record_num = 1;
    while (!feof(file)) {
        long position = ftell(file);
        
        // Read key length
        int kLen;
        if (fread(&kLen, sizeof(int), 1, file) != 1) {
            if (feof(file)) break;
            printf("%sError reading key length at position %ld%s\n", COLOR_RED, position, COLOR_RESET);
            break;
        }
        
        // Validate key length
        if (kLen <= 0 || kLen > 10000) {
            printf("%sInvalid key length %d at position %ld - possibly corrupted data%s\n", 
                   COLOR_RED, kLen, position, COLOR_RESET);
            break;
        }
        
        // Read value length
        int vLen;
        if (fread(&vLen, sizeof(int), 1, file) != 1) {
            printf("%sError reading value length at position %ld%s\n", COLOR_RED, position, COLOR_RESET);
            break;
        }
        
        // Read key
        char* key = malloc(kLen + 1);
        if (fread(key, sizeof(char), (size_t) kLen, file) != (size_t) kLen) {
            printf("%sError reading key at position %ld%s\n", COLOR_RED, position, COLOR_RESET);
            free(key);
            break;
        }
        key[kLen] = '\0';
        
        // Read value (if not tombstone)
        char* value = NULL;
        if (vLen > 0) {
            value = malloc(vLen + 1);
            if (fread(value, sizeof(char), (size_t) vLen, file) != (size_t) vLen) {
                printf("%sError reading value at position %ld%s\n", COLOR_RED, position, COLOR_RESET);
                free(key);
                free(value);
                break;
            }
            value[vLen] = '\0';
            stats.live_records++;
            stats.total_value_bytes += vLen;
        } else if (vLen == -1) {
            stats.tombstone_records++;
        }
        
        // Print record
        print_record(record_num, kLen, vLen, key, value, position);
        
        // Update statistics
        stats.total_records++;
        stats.total_key_bytes += kLen;
        
        // Cleanup
        free(key);
        free(value);
        record_num++;
    }
    
    fclose(file);
    return stats;
}

// Read and dump an index file
void dump_index_file(const char* filepath) {
    FILE* file = fopen(filepath, "rb");
    if (!file) {
        printf("%sWarning: Cannot open index file %s%s\n", COLOR_YELLOW, filepath, COLOR_RESET);
        return;
    }
    
    // Get file size
    fseek(file, 0, SEEK_END);
    long file_size = ftell(file);
    fseek(file, 0, SEEK_SET);
    
    print_file_header(filepath, "INDEX");
    printf("File Size: %ld bytes\n\n", file_size);
    
    int entry_num = 1;
    while (!feof(file)) {
        // Read key length
        int kLen;
        if (fread(&kLen, sizeof(int), 1, file) != 1) {
            if (feof(file)) break;
            printf("%sError reading key length%s\n", COLOR_RED, COLOR_RESET);
            break;
        }
        
        // Validate key length
        if (kLen <= 0 || kLen > 10000) {
            printf("%sInvalid key length %d - possibly corrupted data%s\n", 
                   COLOR_RED, kLen, COLOR_RESET);
            break;
        }
        
        // Read position
        int position;
        if (fread(&position, sizeof(int), 1, file) != 1) {
            printf("%sError reading position%s\n", COLOR_RED, COLOR_RESET);
            break;
        }
        
        // Read key
        char* key = malloc(kLen + 1);
        if (fread(key, sizeof(char), (size_t) kLen, file) != (size_t) kLen) {
            printf("%sError reading key%s\n", COLOR_RED, COLOR_RESET);
            free(key);
            break;
        }
        key[kLen] = '\0';
        
        // Print index entry
        print_index_entry(entry_num, kLen, position, key);
        
        free(key);
        entry_num++;
    }
    
    fclose(file);
}

// Print statistics summary
void print_statistics(FileStats* stats_array, int num_files, char** filenames) {
    printf("\n");
    print_separator('=', 80);
    printf("%sSUMMARY STATISTICS%s\n", COLOR_BOLD, COLOR_RESET);
    print_separator('=', 80);
    
    FileStats total_stats = {0, 0, 0, 0, 0, 0};
    
    for (int i = 0; i < num_files; i++) {
        printf("\n%s%s:%s\n", COLOR_BLUE, filenames[i], COLOR_RESET);
        printf("  Total Records:     %d\n", stats_array[i].total_records);
        printf("  Live Records:      %d\n", stats_array[i].live_records);
        printf("  Tombstone Records: %d\n", stats_array[i].tombstone_records);
        printf("  Total Key Bytes:   %ld\n", stats_array[i].total_key_bytes);
        printf("  Total Value Bytes: %ld\n", stats_array[i].total_value_bytes);
        printf("  File Size:         %ld bytes\n", stats_array[i].file_size);
        
        // Add to totals
        total_stats.total_records += stats_array[i].total_records;
        total_stats.live_records += stats_array[i].live_records;
        total_stats.tombstone_records += stats_array[i].tombstone_records;
        total_stats.total_key_bytes += stats_array[i].total_key_bytes;
        total_stats.total_value_bytes += stats_array[i].total_value_bytes;
        total_stats.file_size += stats_array[i].file_size;
    }
    
    printf("\n%sGRAND TOTALS:%s\n", COLOR_BOLD, COLOR_RESET);
    printf("  Total Records:     %d\n", total_stats.total_records);
    printf("  Live Records:      %d\n", total_stats.live_records);
    printf("  Tombstone Records: %d\n", total_stats.tombstone_records);
    printf("  Total Key Bytes:   %ld\n", total_stats.total_key_bytes);
    printf("  Total Value Bytes: %ld\n", total_stats.total_value_bytes);
    printf("  Total File Size:   %ld bytes\n", total_stats.file_size);
    
    if (total_stats.total_records > 0) {
        double avg_key_size = (double)total_stats.total_key_bytes / total_stats.total_records;
        double avg_value_size = total_stats.live_records > 0 ? 
            (double)total_stats.total_value_bytes / total_stats.live_records : 0;
        double tombstone_ratio = (double)total_stats.tombstone_records / total_stats.total_records * 100;
        
        printf("  Average Key Size:  %.2f bytes\n", avg_key_size);
        printf("  Average Value Size: %.2f bytes\n", avg_value_size);
        printf("  Tombstone Ratio:   %.2f%%\n", tombstone_ratio);
    }
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        printf("Usage: %s <data_directory>\n", argv[0]);
        printf("  Dumps all heap files (current and SSTables) in the specified directory\n");
        return 1;
    }
    
    char* data_directory = argv[1];
    
    printf("%sKVStore Database Dump Utility%s\n", COLOR_BOLD, COLOR_RESET);
    printf("Data Directory: %s\n", data_directory);
    
    // Check if directory exists
    struct stat st;
    if (stat(data_directory, &st) != 0 || !S_ISDIR(st.st_mode)) {
        printf("%sError: Directory '%s' does not exist or is not a directory%s\n", 
               COLOR_RED, data_directory, COLOR_RESET);
        return 1;
    }
    
    // Arrays to store statistics and filenames
    FileStats stats_array[100];
    char* filenames[100];
    int file_count = 0;
    
    // First, dump the current heap file
    char heap_path[512];
    snprintf(heap_path, sizeof(heap_path), "%s/%s", data_directory, HEAP_FILE_NAME);
    
    if (access(heap_path, F_OK) == 0) {
        stats_array[file_count] = dump_data_file(heap_path, "CURRENT HEAP");
        filenames[file_count] = strdup("Current Heap File");
        file_count++;
        
        // Also dump the corresponding index file
        char index_path[512];
        snprintf(index_path, sizeof(index_path), "%s/%s", data_directory, INDEX_FILE_NAME);
        if (access(index_path, F_OK) == 0) {
            dump_index_file(index_path);
        }
    } else {
        printf("%sNo current heap file found%s\n", COLOR_YELLOW, COLOR_RESET);
    }
    
    // Then, find and dump all SSTable files
    DIR* dir = opendir(data_directory);
    if (!dir) {
        printf("%sError: Cannot open directory '%s'%s\n", COLOR_RED, data_directory, COLOR_RESET);
        return 1;
    }
    
    struct dirent* entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strncmp(entry->d_name, SSTABLE_PREFIX, strlen(SSTABLE_PREFIX)) == 0 &&
            strstr(entry->d_name, ".dat") != NULL) {
            
            char sstable_path[512];
            snprintf(sstable_path, sizeof(sstable_path), "%s/%s", data_directory, entry->d_name);
            
            stats_array[file_count] = dump_data_file(sstable_path, "SSTABLE");
            filenames[file_count] = strdup(entry->d_name);
            file_count++;
            
            // Find and dump corresponding index file
            char index_name[256];
            strcpy(index_name, entry->d_name);
            char* dot = strrchr(index_name, '.');
            if (dot) *dot = '\0';
            
            // Convert sstable_X to sstable_index_X
            char* sstable_part = strstr(index_name, SSTABLE_PREFIX);
            if (sstable_part) {
                char temp[256];
                strcpy(temp, sstable_part + strlen(SSTABLE_PREFIX));
                snprintf(index_name, sizeof(index_name), "sstable_index_%s.dat", temp);
                
                char index_path[512];
                snprintf(index_path, sizeof(index_path), "%s/%s", data_directory, index_name);
                
                if (access(index_path, F_OK) == 0) {
                    dump_index_file(index_path);
                }
            }
        }
    }
    closedir(dir);
    
    if (file_count == 0) {
        printf("%sNo data files found in directory '%s'%s\n", COLOR_YELLOW, data_directory, COLOR_RESET);
    } else {
        print_statistics(stats_array, file_count, filenames);
        
        // Free allocated filenames
        for (int i = 0; i < file_count; i++) {
            free(filenames[i]);
        }
    }
    
    return 0;
}
