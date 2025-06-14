#include "kvstore.h"
#include <stdio.h>

int main(int argc, char** argv) {
	if (argc != 2) {
        printf("Usage: %s <data_directory>\n", argv[0]);
        printf("  Demo of the tiny db engine showing writes, reads, deletes, compaction\n");
        return 1;
    }
    char* data_directory = argv[1];
    init(data_directory);
    
    put("key1", "value1");
    put("key2", "value2");
    
    char* val = get("key2");  // Returns "value1"
	printf("Lookup of key2 gave: %s\n", val);    
    //delete("key1");           // Creates tombstone
    
    //compact();                // Trigger compaction
    
    cleanup();                // Clean up resources
    return 0;
}
