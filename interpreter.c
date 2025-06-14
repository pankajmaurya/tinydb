#include "kvstore.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_LINE_LENGTH 1024
#define MAX_KEY_LENGTH 256
#define MAX_VALUE_LENGTH 512

void trim_whitespace(char* str) {
    char* end;
    
    // Trim leading space
    while (*str == ' ' || *str == '\t' || *str == '\n' || *str == '\r') str++;
    
    // All spaces?
    if (*str == 0) return;
    
    // Trim trailing space
    end = str + strlen(str) - 1;
    while (end > str && (*end == ' ' || *end == '\t' || *end == '\n' || *end == '\r')) end--;
    
    // Write new null terminator
    end[1] = '\0';
}

int parse_put_command(char* line, char* key, char* value) {
    char* token = strtok(line, " \t");
    if (!token || strcasecmp(token, "PUT") != 0) return 0;
    
    token = strtok(NULL, " \t");
    if (!token) {
        printf("Error: PUT command missing key\n");
        return 0;
    }
    strcpy(key, token);
    
    // Get the rest of the line as value (allowing spaces in values)
    char* value_start = strtok(NULL, "");
    if (!value_start) {
        printf("Error: PUT command missing value\n");
        return 0;
    }
    
    // Trim leading whitespace from value
    while (*value_start == ' ' || *value_start == '\t') value_start++;
    strcpy(value, value_start);
    trim_whitespace(value);
    
    return 1;
}

int parse_dget_command(char* line, char* key) {
    char* token = strtok(line, " \t");
    if (!token || strcasecmp(token, "DGET") != 0) return 0;
    
    token = strtok(NULL, " \t\n\r");
    if (!token) {
        printf("Error: DGET command missing key\n");
        return 0;
    }
    strcpy(key, token);
    
    return 1;
}

int parse_get_command(char* line, char* key) {
    char* token = strtok(line, " \t");
    if (!token || strcasecmp(token, "GET") != 0) return 0;
    
    token = strtok(NULL, " \t\n\r");
    if (!token) {
        printf("Error: GET command missing key\n");
        return 0;
    }
    strcpy(key, token);
    
    return 1;
}

int parse_del_command(char* line, char* key) {
    char* token = strtok(line, " \t");
    if (!token || strcasecmp(token, "DEL") != 0) return 0;
    
    token = strtok(NULL, " \t\n\r");
    if (!token) {
        printf("Error: DEL command missing key\n");
        return 0;
    }
    strcpy(key, token);
    
    return 1;
}

int parse_compact_command(char* line) {
    char* token = strtok(line, " \t\n\r");
    return (token && strcasecmp(token, "COMPACT") == 0);
}

void process_query(char* line) {
    char key[MAX_KEY_LENGTH];
    char value[MAX_VALUE_LENGTH];
    char* line_copy = malloc(strlen(line) + 1);
    strcpy(line_copy, line);
    
    trim_whitespace(line_copy);
    
    if (strlen(line_copy) == 0) {
        free(line_copy);
        return;
    }
    
    if (parse_put_command(line_copy, key, value)) {
        put(key, value);
        printf("PUT %s -> %s\n", key, value);
    }
    else {
        strcpy(line_copy, line);
        trim_whitespace(line_copy);
        
        if (parse_dget_command(line_copy, key)) {
            char* result = debug_get(key);
            if (result) {
                printf("DGET %s -> %s\n", key, result);
            } else {
                printf("DGET %s -> (not found)\n", key);
            }
        }
        else {
            strcpy(line_copy, line);
            trim_whitespace(line_copy);
            
            if (parse_get_command(line_copy, key)) {
                char* result = get(key);
                if (result) {
                    printf("GET %s -> %s\n", key, result);
                } else {
                    printf("GET %s -> (not found)\n", key);
                }
            }
            else {
                strcpy(line_copy, line);
                trim_whitespace(line_copy);
                
                if (parse_del_command(line_copy, key)) {
                    delete(key);
                    printf("DEL %s\n", key);
                }
                else {
                    strcpy(line_copy, line);
                    trim_whitespace(line_copy);
                    
                    if (parse_compact_command(line_copy)) {
                        compact();
                        printf("COMPACT executed\n");
                    }
                    else {
                        printf("Error: Unknown command. Supported commands:\n");
                        printf("  PUT <key> <value>\n");
                        printf("  GET <key>\n");
                        printf("  DGET <key>\n");
                        printf("  DEL <key>\n");
                        printf("  COMPACT\n");
                    }
                }
            }
        }
    }
    
    free(line_copy);
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        printf("Usage: %s <data_directory>\n", argv[0]);
        printf("  Interactive query interpreter for the tiny db engine\n");
        printf("  Supported commands:\n");
        printf("    PUT <key> <value> - Store a key-value pair\n");
        printf("    GET <key>         - Retrieve value for key\n");
        printf("    DGET <key>        - (With debug steps) Retrieve value for key\n");
        printf("    DEL <key>         - Delete key (creates tombstone)\n");
        printf("    COMPACT           - Trigger compaction\n");
        printf("    quit              - Exit the program\n");
        return 1;
    }
    
    char* data_directory = argv[1];
    init(data_directory);
    
    printf("Tiny DB Query Interpreter\n");
    printf("Data directory: %s\n", data_directory);
    printf("Type 'quit' to exit\n\n");
    
    char line[MAX_LINE_LENGTH];
    
    while (1) {
        printf("tinydb> ");
        fflush(stdout);
        
        if (!fgets(line, sizeof(line), stdin)) {
            printf("\n");
            break;
        }
        
        trim_whitespace(line);
        
        if (strcasecmp(line, "quit") == 0 || strcasecmp(line, "exit") == 0) {
            break;
        }
        
        if (strlen(line) > 0) {
            process_query(line);
        }
        
        printf("\n");
    }
    
    cleanup();
    printf("Goodbye!\n");
    return 0;
}
