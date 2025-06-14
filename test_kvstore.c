#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>
#include "kvstore.h"

// Test result tracking
int tests_run = 0;
int tests_passed = 0;

// Simple test framework macros
#define TEST_START(name) \
    printf("\n=== Running Test: %s ===\n", name); \
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
    printf("✓ Test completed successfully\n"); \
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

// Test 1: Basic initialization and cleanup
int test_init_cleanup() {
    TEST_START("Init and Cleanup");
    
    const char* test_dir = "./test_data";
    cleanup_test_dir(test_dir);
    
    init((char*)test_dir);
    TEST_ASSERT(kvstore != NULL, "KVStore initialized");
    
    // Check if directory was created
    struct stat st;
    TEST_ASSERT(stat(test_dir, &st) == 0 && S_ISDIR(st.st_mode), "Data directory created");
    
    cleanup();
    TEST_ASSERT(kvstore == NULL, "KVStore cleaned up");
    
    cleanup_test_dir(test_dir);
    TEST_END();
}

// Test 2: Basic put and get operations
int test_basic_put_get() {
    TEST_START("Basic Put/Get");
    
    const char* test_dir = "./test_data";
    cleanup_test_dir(test_dir);
    
    init((char*)test_dir);
    
    // Test simple put/get
    put("key1", "value1");
    char* result = get("key1");
    TEST_ASSERT(result != NULL, "Retrieved value is not NULL");
    TEST_ASSERT(strcmp(result, "value1") == 0, "Retrieved correct value");
    free(result);
    
    // Test non-existent key
    result = get("nonexistent");
    TEST_ASSERT(result == NULL, "Non-existent key returns NULL");
    
    cleanup();
    cleanup_test_dir(test_dir);
    TEST_END();
}

// Test 3: Key overwrite functionality
int test_key_overwrite() {
    TEST_START("Key Overwrite");
    
    const char* test_dir = "./test_data";
    cleanup_test_dir(test_dir);
    
    init((char*)test_dir);
    
    // Put initial value
    put("key1", "value1");
    char* result = get("key1");
    TEST_ASSERT(strcmp(result, "value1") == 0, "Initial value correct");
    free(result);
    
    // Overwrite with new value
    put("key1", "value2");
    result = get("key1");
    TEST_ASSERT(strcmp(result, "value2") == 0, "Overwritten value correct");
    free(result);
    
    cleanup();
    cleanup_test_dir(test_dir);
    TEST_END();
}

// Test 4: Basic delete functionality
int test_delete() {
    TEST_START("Delete Operations");
    
    const char* test_dir = "./test_data";
    cleanup_test_dir(test_dir);
    
    init((char*)test_dir);
    
    // Put a value
    put("key1", "value1");
    char* result = get("key1");
    TEST_ASSERT(result != NULL && strcmp(result, "value1") == 0, "Value exists before delete");
    free(result);
    
    // Delete the key
    delete("key1");
    result = get("key1");
    TEST_ASSERT(result == NULL, "Key deleted successfully");
    
    cleanup();
    cleanup_test_dir(test_dir);
    TEST_END();
}

// Test 5: Multiple keys
int test_multiple_keys() {
    TEST_START("Multiple Keys");
    
    const char* test_dir = "./test_data";
    cleanup_test_dir(test_dir);
    
    init((char*)test_dir);
    
    // Put multiple keys
    put("key1", "value1");
    put("key2", "value2");
    put("key3", "value3");
    
    // Verify all keys
    char* result1 = get("key1");
    char* result2 = get("key2");
    char* result3 = get("key3");
    
    TEST_ASSERT(result1 != NULL && strcmp(result1, "value1") == 0, "Key1 correct");
    TEST_ASSERT(result2 != NULL && strcmp(result2, "value2") == 0, "Key2 correct");
    TEST_ASSERT(result3 != NULL && strcmp(result3, "value3") == 0, "Key3 correct");
    
    free(result1);
    free(result2);
    free(result3);
    
    cleanup();
    cleanup_test_dir(test_dir);
    TEST_END();
}

// Test 6: Persistence (restart simulation)
int test_persistence() {
    TEST_START("Persistence");
    
    const char* test_dir = "./test_data";
    cleanup_test_dir(test_dir);
    
    // First session - write data
    init((char*)test_dir);
    put("persistent_key", "persistent_value");
    cleanup();
    
    // Second session - read data
    init((char*)test_dir);
    char* result = get("persistent_key");
    TEST_ASSERT(result != NULL, "Value persisted after restart");
    TEST_ASSERT(strcmp(result, "persistent_value") == 0, "Persisted value correct");
    free(result);
    
    cleanup();
    cleanup_test_dir(test_dir);
    TEST_END();
}

// Test 7: Empty values
int test_empty_values() {
    TEST_START("Empty Values");
    
    const char* test_dir = "./test_data";
    cleanup_test_dir(test_dir);
    
    init((char*)test_dir);
    
    // Test empty string value
    put("empty_key", "");
    char* result = get("empty_key");
    TEST_ASSERT(result != NULL, "Empty value retrieved");
    TEST_ASSERT(strlen(result) == 0, "Empty value is empty string");
    free(result);
    
    cleanup();
    cleanup_test_dir(test_dir);
    TEST_END();
}

// Test 8: Compaction trigger (basic)
int test_compaction_trigger() {
    TEST_START("Compaction Trigger");
    
    const char* test_dir = "./test_data";
    cleanup_test_dir(test_dir);
    
    init((char*)test_dir);
    
    // Write enough data to potentially trigger compaction
    // (depending on your compaction threshold)
    for (int i = 0; i < 100; i++) {
        char key[32], value[64];
        snprintf(key, sizeof(key), "key_%d", i);
        snprintf(value, sizeof(value), "value_%d_with_some_extra_data_to_make_it_longer", i);
        put(key, value);
    }
    
    // Verify some data is still accessible
    char* result = get("key_50");
    TEST_ASSERT(result != NULL, "Data accessible after potential compaction");
    TEST_ASSERT(strstr(result, "value_50") != NULL, "Correct value after compaction");
    free(result);
    
    // Wait a bit for compaction to complete if it started
    sleep(1);
    
    cleanup();
    cleanup_test_dir(test_dir);
    TEST_END();
}

// Main test runner
int main() {
    printf("=== KVStore Test Suite ===\n");
    printf("Running minimal automated tests...\n");
    
    // Run all tests
    test_init_cleanup();
    test_basic_put_get();
    test_key_overwrite();
    test_delete();
    test_multiple_keys();
    test_persistence();
    test_empty_values();
    test_compaction_trigger();
    
    // Print summary
    printf("\n=== Test Results ===\n");
    printf("Tests run: %d\n", tests_run);
    printf("Tests passed: %d\n", tests_passed);
    printf("Tests failed: %d\n", tests_run - tests_passed);
    
    if (tests_passed == tests_run) {
        printf("🎉 All tests passed!\n");
        return 0;
    } else {
        printf("❌ Some tests failed.\n");
        return 1;
    }
}
