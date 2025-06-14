# Makefile for KVStore LSM-tree database engine

# Compiler and flags
CC = gcc
CFLAGS = -Wall -Wextra -std=c99 -g -O2
LDFLAGS = -lpthread

# Source files
KVSTORE_SRC = kvstore.c
DEMO_SRC = demo.c
KVDUMP_SRC = kvdump.c
INTERPRETER_SRC = interpreter.c

# Object files
KVSTORE_OBJ = kvstore.o
DEMO_OBJ = demo.o
KVDUMP_OBJ = kvdump.o
INTERPRETER_OBJ = interpreter.o

# Target binaries
TARGET = demo
KVDUMP_TARGET = kvdump
INTERPRETER_TARGET = interpreter

# Header files
HEADERS = kvstore.h utils.h sstable.h

# Default target
all: $(TARGET) $(KVDUMP_TARGET) $(INTERPRETER_TARGET)

# Build the demo binary
$(TARGET): $(KVSTORE_OBJ) $(DEMO_OBJ) 
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

# Build the kvdump utility
$(KVDUMP_TARGET): $(KVSTORE_OBJ) $(KVDUMP_OBJ)
	$(CC) $(CFLAGS) -o $@ $^

# Build the interpreter utility
$(INTERPRETER_TARGET): $(KVSTORE_OBJ) $(INTERPRETER_OBJ)
	$(CC) $(CFLAGS) -o $@ $^

# Build kvstore object file
$(KVSTORE_OBJ): $(KVSTORE_SRC) $(HEADERS)
	$(CC) $(CFLAGS) -c $< -o $@

# Build interpreter object file
$(INTERPRETER_OBJ): $(INTERPRETER_SRC) $(HEADERS)
	$(CC) $(CFLAGS) -c $< -o $@

# Build demo object file
$(DEMO_OBJ): $(DEMO_SRC) $(HEADERS)
	$(CC) $(CFLAGS) -c $< -o $@

# Build kvdump object file
$(KVDUMP_OBJ): $(KVDUMP_SRC)
	$(CC) $(CFLAGS) -c $< -o $@

# Clean up build artifacts
clean:
	rm -f $(KVSTORE_OBJ) $(DEMO_OBJ) $(KVDUMP_OBJ) $(INTERPRETER_OBJ) $(TARGET) $(KVDUMP_TARGET) $(INTERPRETER_TARGET)
#	rm -rf /tmp/kvstore_data

# Clean and rebuild
rebuild: clean all

# Install dependencies (if needed)
deps:
	@echo "No external dependencies required. Using standard C libraries and pthreads."

# Create test data directory
test-setup:
	mkdir -p /tmp/kvstore_data

# Run the demo
run: $(TARGET) test-setup
	./$(TARGET)

# Run kvdump on test data
dump: $(KVDUMP_TARGET) test-setup
	./$(KVDUMP_TARGET) /tmp/kvstore_data

# Debug build
debug: CFLAGS += -DDEBUG -g3
debug: $(TARGET)

# Release build
release: CFLAGS += -O3 -DNDEBUG
release: clean $(TARGET)

# Check for memory leaks (requires valgrind)
memcheck: $(TARGET) test-setup
	valgrind --leak-check=full --show-leak-kinds=all --track-origins=yes ./$(TARGET)

# Static analysis (requires cppcheck)
static-analysis:
	cppcheck --enable=all --std=c99 $(KVSTORE_SRC) $(DEMO_SRC)

# Format code (requires clang-format)
format:
	clang-format -i $(KVSTORE_SRC) $(DEMO_SRC) $(HEADERS) $(INTERPRETER_SRC)

# Show help
help:
	@echo "Available targets:"
	@echo "  all          - Build demo and kvdump binaries (default)"
	@echo "  clean        - Remove build artifacts and test data"
	@echo "  rebuild      - Clean and rebuild"
	@echo "  run          - Build and run the demo"
	@echo "  dump         - Build and run kvdump on test data"
	@echo "  debug        - Build with debug symbols"
	@echo "  release      - Build optimized release version"
	@echo "  memcheck     - Run with valgrind memory checker"
	@echo "  static-analysis - Run cppcheck static analysis"
	@echo "  format       - Format code with clang-format"
	@echo "  test-setup   - Create test data directory"
	@echo "  deps         - Show dependency information"
	@echo "  help         - Show this help message"

# Phony targets
.PHONY: all clean rebuild deps test-setup run debug release memcheck static-analysis format help
