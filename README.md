We are implementing a toy database engine in C. It will be an LSM tree based architecture. We will have a heap file and an index file at the start of persisting records. Each data record in the heap file looks like {int: kLen, int: vLen, array: kLen <bytes for key>, array: vLen <bytes for value>}. Each data entry in the indexFile looks like {int: kLen, int: vPos, array: kLen <bytes for key>}.

We will represent a deletion via *tombstone *data record by making vLen as -1. e.g. tombstone for record with key "key" is {3, -1, “key”,}. The corresponding data entry looks like {3, -1, "key"}

At some configured threshold of bytes, the database engine will kick off compaction where the heap file will be saved as a Sorted Strings Table (SSTable) file containing sorted data records of the same format : {int: kLen, int: vLen, array: kLen <bytes for key>, array: vLen <bytes for value>}. The indexFile corresponding to the SSTable will be rewritten as well. For multiple entries for same key, the SSTable will contain the latest data record.

The interface to the db engine should look like this: kvstore.h 

// Initialize the KVStore. Scan data_directory to find existing heapfile, indexFile and all the compacted files (SSTables, indexFiles) and initialize the metadata. 

// If no file is found, initilize an empty heapFile. 

void init(char* data_directory);

// Write the data record as describe in the scheme. void put(char* key, char* value);

// Get the latest value stored for the given key. char* get(char* key);

// Delete the record for given key, if present. void delete(char* key);

// Trigger compaction to initialize new heapFile, indexFile for writes. produce SSTable from last heapFile, rewrite last indexFile. This is non-blocking. 

void compact();

// Returns status: STARTED, COMPLETED int getCompactionStatus();

Generate kvstore.h and kvstore.c
