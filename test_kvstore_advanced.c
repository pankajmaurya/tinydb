#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>
#include <time.h>
#include <pthread.h>
#include "kvstore.h"

// Test result tracking
int tests_run = 0;
int tests_passed = 0;

// Test framework macros
#define TEST_START(name) \
    printf("\n=== Running Advanced Test: %s ===\n", name); \
    tests_run++;

#define TEST_ASSERT(condition, message) \
    if (condition) { \
        printf("✓ PASS: %s\n", message); \
    } else { \
        printf("✗ FAIL: %s\n", message); \
        return 0; \
    }

#define TEST_END() \
    tests_passed++; \
    printf("✓ Advanced test completed successfully\n"); \
    return 1;

// Helper function to clean up test directory
void cleanup_test_dir(const char* dir_path) {
    DIR* dir = opendir(dir_path);
    if (!dir) return;
    
    struct dirent* entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        
        char filepath[512];
        snprintf(filepath, sizeof(filepath), "%s/%s", dir_path, entry->d_name);
        unlink(filepath);
    }
    closedir(dir);
    rmdir(dir_path);
}

// Generate large value for testing
char* generate_large_value(int size) {
    char* value = malloc(size + 1);
    for (int i = 0; i < size; i++) {
        value[i] = 'A' + (i % 26);
    }
    value[size] = '\0';
    return value;
}

// Test 1: Large value handling
int test_large_values() {
    TEST_START("Large Value Handling");
    
    const char* test_dir = "./test_data_adv";
    cleanup_test_dir(test_dir);
    
    init((char*)test_dir);
    
    // Test various large value sizes
    int sizes[] = {1024, 4096, 16384, 65536};
    int num_sizes = sizeof(sizes) / sizeof(sizes[0]);
    
    for (int i = 0; i < num_sizes; i++) {
        char key[32];
        snprintf(key, sizeof(key), "large_key_%d", sizes[i]);
        
        char* large_value = generate_large_value(sizes[i]);
        put(key, large_value);
        
        char* result = get(key);
        TEST_ASSERT(result != NULL, "Large value retrieved");
        TEST_ASSERT(strlen(result) == sizes[i], "Large value size correct");
        TEST_ASSERT(strcmp(result, large_value) == 0, "Large value content correct");
        
        free(result);
        free(large_value);
    }
    
    cleanup();
    cleanup_test_dir(test_dir);
    TEST_END();
}

// Test 2: Key-value operation sequences (write-heavy)
int test_write_heavy_workload() {
    TEST_START("Write Heavy Workload");
    
    const char* test_dir = "./test_data_adv";
    cleanup_test_dir(test_dir);
    
    init((char*)test_dir);
    
    const int num_operations = 1000;
    
    // Phase 1: Sequential writes
    for (int i = 0; i < num_operations; i++) {
        char key[32], value[64];
        snprintf(key, sizeof(key), "seq_key_%04d", i);
        snprintf(value, sizeof(value), "seq_value_%04d_data", i);
        put(key, value);
    }
    
    // Phase 2: Random overwrites
    srand(42); // Fixed seed for reproducible tests
    for (int i = 0; i < num_operations / 2; i++) {
        int idx = rand() % num_operations;
        char key[32], value[64];
        snprintf(key, sizeof(key), "seq_key_%04d", idx);
        snprintf(value, sizeof(value), "overwritten_value_%04d", idx);
        put(key, value);
    }
    
    // Phase 3: Verify some random entries
    for (int i = 0; i < 50; i++) {
        int idx = rand() % num_operations;
        char key[32];
        snprintf(key, sizeof(key), "seq_key_%04d", idx);
        
        char* result = get(key);
        TEST_ASSERT(result != NULL, "Random key exists after write-heavy workload");
        free(result);
    }
    
    cleanup();
    cleanup_test_dir(test_dir);
    TEST_END();
}

// Test 3: Interleaved operations (put/get/delete mix)
int test_interleaved_operations() {
    TEST_START("Interleaved Operations");
    
    const char* test_dir = "./test_data_adv";
    cleanup_test_dir(test_dir);
    
    init((char*)test_dir);
    
    const int num_keys = 1000;
    
    // Create initial dataset
    for (int i = 0; i < num_keys; i++) {
        char key[32], value[64];
        snprintf(key, sizeof(key), "mixed_key_%03d", i);
        snprintf(value, sizeof(value), "initial_value_%03d", i);
        put(key, value);
    }
    
    // Interleaved operations
    srand(42);
    for (int round = 0; round < 5; round++) {
        printf("  Round %d of interleaved operations\n", round + 1);
        
        for (int i = 0; i < num_keys; i++) {
            int operation = rand() % 10;
            int key_idx = rand() % num_keys;
            char key[32];
            snprintf(key, sizeof(key), "mixed_key_%03d", key_idx);
            
            if (operation < 5) {
                // 50% chance: Update
                char value[64];
                snprintf(value, sizeof(value), "updated_r%d_value_%03d", round, key_idx);
                put(key, value);
            } else if (operation < 8) {
                // 30% chance: Read
                char* result = get(key);
                if (result) {
                    free(result);
                }
            } else {
                // 20% chance: Delete
                delete(key);
            }
        }
    }
    
    // Verify some keys still exist
    int found_keys = 0;
    for (int i = 0; i < num_keys; i += 10) {
        char key[32];
        snprintf(key, sizeof(key), "mixed_key_%03d", i);
        char* result = get(key);
        if (result) {
            found_keys++;
            free(result);
        }
    }
    
    TEST_ASSERT(found_keys > 0, "Some keys survived interleaved operations");
    printf("  Found %d keys after interleaved operations\n", found_keys);
    
    cleanup();
    cleanup_test_dir(test_dir);
    TEST_END();
}

// Test 4: Compaction stress test
int test_compaction_stress() {
    TEST_START("Compaction Stress Test");
    
    const char* test_dir = "./test_data_adv";
    cleanup_test_dir(test_dir);
    
    init((char*)test_dir);
    
    // Force multiple compactions by writing lots of data
    const int batch_size = 100;
    const int num_batches = 10;
    
    for (int batch = 0; batch < num_batches; batch++) {
        printf("  Writing batch %d/%d\n", batch + 1, num_batches);
        
        // Write a batch of data
        for (int i = 0; i < batch_size; i++) {
            char key[32], value[128];
            snprintf(key, sizeof(key), "stress_key_%d_%d", batch, i);
            snprintf(value, sizeof(value), "stress_value_batch_%d_item_%d_with_extra_data_to_increase_size", batch, i);
            put(key, value);
        }
        
        // Also overwrite some existing keys to create more tombstones
        if (batch > 0) {
            for (int i = 0; i < batch_size / 2; i++) {
                char key[32], value[128];
                snprintf(key, sizeof(key), "stress_key_%d_%d", batch - 1, i);
                snprintf(value, sizeof(value), "overwritten_value_batch_%d", batch);
                put(key, value);
            }
        }
        
        // Wait for potential compaction
        sleep(1);
        
        // Check compaction status
        int status = getCompactionStatus();
        printf("  Compaction status: %s\n", 
               status == COMPACTION_COMPLETED ? "COMPLETED" : "STARTED");
    }
    
    // Wait for any ongoing compaction to complete
    while (getCompactionStatus() == COMPACTION_STARTED) {
        printf("  Waiting for compaction to complete...\n");
        sleep(1);
    }
    
    // Verify data integrity after compaction
    int verified_keys = 0;
    for (int batch = 0; batch < num_batches; batch++) {
        for (int i = 0; i < 10; i++) { // Check subset of keys
            char key[32];
            snprintf(key, sizeof(key), "stress_key_%d_%d", batch, i);
            char* result = get(key);
            if (result) {
                verified_keys++;
                free(result);
            }
        }
    }
    
    TEST_ASSERT(verified_keys > 0, "Keys accessible after compaction stress");
    printf("  Verified %d keys after compaction stress\n", verified_keys);
    
    cleanup();
    cleanup_test_dir(test_dir);
    TEST_END();
}

// Test 5: Delete and re-add pattern
int test_delete_readd_pattern() {
    TEST_START("Delete and Re-add Pattern");
    
    const char* test_dir = "./test_data_adv";
    cleanup_test_dir(test_dir);
    
    init((char*)test_dir);
    
    const int num_keys = 100;
    
    // Phase 1: Add initial data
    for (int i = 0; i < num_keys; i++) {
        char key[32], value[64];
        snprintf(key, sizeof(key), "cycle_key_%02d", i);
        snprintf(value, sizeof(value), "initial_value_%02d", i);
        put(key, value);
    }
    
    // Phase 2: Delete all keys
    for (int i = 0; i < num_keys; i++) {
        char key[32];
        snprintf(key, sizeof(key), "cycle_key_%02d", i);
        delete(key);
    }
    
    // Phase 3: Verify deletion
    int deleted_count = 0;
    for (int i = 0; i < num_keys; i++) {
        char key[32];
        snprintf(key, sizeof(key), "cycle_key_%02d", i);
        char* result = get(key);
        if (result == NULL) {
            deleted_count++;
        } else {
            free(result);
        }
    }
    TEST_ASSERT(deleted_count == num_keys, "All keys deleted successfully");
    
    // Phase 4: Re-add with different values
    for (int i = 0; i < num_keys; i++) {
        char key[32], value[64];
        snprintf(key, sizeof(key), "cycle_key_%02d", i);
        snprintf(value, sizeof(value), "readded_value_%02d", i);
        put(key, value);
    }
    
    // Phase 5: Verify re-addition
    int readded_count = 0;
    for (int i = 0; i < num_keys; i++) {
        char key[32];
        snprintf(key, sizeof(key), "cycle_key_%02d", i);
        char* result = get(key);
        if (result != NULL && strstr(result, "readded_value") != NULL) {
            readded_count++;
            free(result);
        } else if (result) {
            free(result);
        }
    }
    TEST_ASSERT(readded_count == num_keys, "All keys re-added successfully");
    
    cleanup();
    cleanup_test_dir(test_dir);
    TEST_END();
}

// Test 6: Persistence with compaction
int test_persistence_with_compaction() {
    TEST_START("Persistence with Compaction");
    
    const char* test_dir = "./test_data_adv";
    cleanup_test_dir(test_dir);
    
    // Session 1: Create data and force compaction
    init((char*)test_dir);
    
    const int num_keys = 200;
    for (int i = 0; i < num_keys; i++) {
        char key[32], value[128];
        snprintf(key, sizeof(key), "persist_key_%03d", i);
        snprintf(value, sizeof(value), "persist_value_%03d_with_lots_of_data_to_trigger_compaction_sooner", i);
        put(key, value);
    }
    
    // Overwrite some keys to create tombstones
    for (int i = 0; i < num_keys / 2; i++) {
        char key[32], value[128];
        snprintf(key, sizeof(key), "persist_key_%03d", i);
        snprintf(value, sizeof(value), "overwritten_persist_value_%03d", i);
        put(key, value);
    }
    
    // Wait for compaction to complete
    sleep(2);
    while (getCompactionStatus() == COMPACTION_STARTED) {
        printf("  Waiting for compaction...\n");
        sleep(1);
    }
    
    cleanup();
    
    // Session 2: Restart and verify data
    init((char*)test_dir);
    
    int verified_keys = 0;
    for (int i = 0; i < num_keys; i++) {
        char key[32];
        snprintf(key, sizeof(key), "persist_key_%03d", i);
        char* result = get(key);
        if (result) {
            verified_keys++;
            // Verify it's the correct value (overwritten or original)
            if (i < num_keys / 2) {
                TEST_ASSERT(strstr(result, "overwritten_persist_value") != NULL, 
                           "Overwritten value persisted correctly");
            } else {
                TEST_ASSERT(strstr(result, "persist_value") != NULL, 
                           "Original value persisted correctly");
            }
            free(result);
        }
    }
    
    TEST_ASSERT(verified_keys > 0, "Data persisted after compaction and restart");
    printf("  Verified %d keys after restart\n", verified_keys);
    
    cleanup();
    cleanup_test_dir(test_dir);
    TEST_END();
}

// Test 7: Edge cases and error conditions
int test_edge_cases() {
    TEST_START("Edge Cases");
    
    const char* test_dir = "./test_data_adv";
    cleanup_test_dir(test_dir);
    
    init((char*)test_dir);
    
    // Test very long keys
    char long_key[512];
    memset(long_key, 'k', 500);
    long_key[500] = '\0';
    put(long_key, "long_key_value");
    char* result = get(long_key);
    TEST_ASSERT(result != NULL && strcmp(result, "long_key_value") == 0, 
               "Very long key handled correctly");
    free(result);
    
    // Test empty string value (should work now)
    put("empty_value_key", "");
    result = get("empty_value_key");
    TEST_ASSERT(result != NULL && strlen(result) == 0, 
               "Empty string value handled correctly");
    free(result);
    
    // Test keys with special characters
    put("key with spaces", "value with spaces");
    result = get("key with spaces");
    TEST_ASSERT(result != NULL && strcmp(result, "value with spaces") == 0,
               "Keys with spaces handled correctly");
    free(result);
    
    // Test numeric keys
    put("12345", "numeric_key_value");
    result = get("12345");
    TEST_ASSERT(result != NULL && strcmp(result, "numeric_key_value") == 0,
               "Numeric keys handled correctly");
    free(result);
    
    // Test overwriting with different size values
    put("size_test", "small");
    put("size_test", "much_larger_value_than_before");
    result = get("size_test");
    TEST_ASSERT(result != NULL && strcmp(result, "much_larger_value_than_before") == 0,
               "Overwriting with larger value works");
    free(result);
    
    put("size_test", "tiny");
    result = get("size_test");
    TEST_ASSERT(result != NULL && strcmp(result, "tiny") == 0,
               "Overwriting with smaller value works");
    free(result);
    
    cleanup();
    cleanup_test_dir(test_dir);
    TEST_END();
}

// Test 8: High-frequency operations
int test_high_frequency_operations() {
    TEST_START("High Frequency Operations");
    
    const char* test_dir = "./test_data_adv";
    cleanup_test_dir(test_dir);
    
    init((char*)test_dir);
    
    const int num_operations = 200;
    clock_t start_time = clock();
    
    // Rapid-fire operations
    srand(42);
    for (int i = 0; i < num_operations; i++) {
        char key[32], value[64];
        int operation = rand() % 4;
        int key_id = rand() % 100; // Limited key space for conflicts
        
        snprintf(key, sizeof(key), "freq_key_%02d", key_id);
        snprintf(value, sizeof(value), "freq_value_%d", i);
        
        switch (operation) {
            case 0:
            case 1:
                // 50% put operations
                put(key, value);
                break;
            case 2:
                // 25% get operations
                {
                    char* result = get(key);
                    if (result) free(result);
                }
                break;
            case 3:
                // 25% delete operations
                delete(key);
                break;
        }
    }
    
    clock_t end_time = clock();
    double elapsed = ((double)(end_time - start_time)) / CLOCKS_PER_SEC;
    printf("  Completed %d operations in %.2f seconds (%.0f ops/sec)\n", 
           num_operations, elapsed, num_operations / elapsed);
    
    // Verify system is still functional
    put("final_test", "final_value");
    char* result = get("final_test");
    TEST_ASSERT(result != NULL && strcmp(result, "final_value") == 0,
               "System functional after high-frequency operations");
    free(result);
    
    cleanup();
    cleanup_test_dir(test_dir);
    TEST_END();
}

// Main test runner
int main() {
    printf("=== Advanced KVStore Test Suite ===\n");
    printf("Running comprehensive stress tests...\n");
    
    // Run all advanced tests
    test_large_values();
    test_write_heavy_workload();
    test_interleaved_operations();
    test_compaction_stress();
    test_delete_readd_pattern();
    test_persistence_with_compaction();
    test_edge_cases();
    test_high_frequency_operations();
    
    // Print summary
    printf("\n=== Advanced Test Results ===\n");
    printf("Tests run: %d\n", tests_run);
    printf("Tests passed: %d\n", tests_passed);
    printf("Tests failed: %d\n", tests_run - tests_passed);
    
    if (tests_passed == tests_run) {
        printf("🎉 All advanced tests passed! Your KVStore is robust.\n");
        return 0;
    } else {
        printf("❌ Some advanced tests failed. Check the output above.\n");
        return 1;
    }
}
