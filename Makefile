# Makefile for LLMFlowSimulator
#
# Available targets:
#   all        - Build the simulator executable (default)
#   so         - Build shared library (libpycallclass.so)
#   so-debug   - Build shared library with debug info
#   so-release - Build optimized shared library
#   debug      - Build executable with debug info
#   release    - Build optimized executable
#   clean      - Remove all build artifacts
#   run        - Build and run the simulator
#
# Examples:
#   make so         - Build shared library
#   make so-debug   - Build shared library with debug symbols
#   make so-release - Build optimized shared library

CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O2
INCLUDES = -I.

# Source files
SOURCES = main.cpp \
          simulator.cpp \
          flow.cpp \
          collective.cpp \
          ranktask.cpp \
          ranktask_methods.cpp \
          ranktask_events.cpp \
          ranktask_progress.cpp \
          grouptask.cpp \
          grouptask_progress.cpp \
          microbatchmanager.cpp \
          workload.cpp \
          topology.cpp

# Object files
OBJECTS = $(SOURCES:.cpp=.o)

# Target executable
TARGET = simulator

# Shared library target
SHARED_LIB = libpycallclass.so

# Default target
all: $(TARGET)

# Build the executable
$(TARGET): $(OBJECTS)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o $(TARGET) $(OBJECTS)

# Build the shared library
$(SHARED_LIB): $(SOURCES)
	$(CXX) -o $(SHARED_LIB) -shared -fPIC $(SOURCES) -I/usr/include/nlohmann

# Compile source files to object files
%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

# Clean build artifacts
clean:
	rm -f $(OBJECTS) $(TARGET) $(SHARED_LIB)

# Install dependencies (if needed)
install-deps:
	# Add any dependency installation commands here

# Run the simulator
run: $(TARGET)
	./$(TARGET)

# Build shared library
so: $(SHARED_LIB)

# Build shared library with debug info
so-debug: CXXFLAGS += -g -DDEBUG
so-debug: $(SHARED_LIB)

# Build shared library optimized
so-release: CXXFLAGS += -DNDEBUG -O3
so-release: $(SHARED_LIB)

# Show help information
help:
	@echo "LLMFlowSimulator Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  all        - Build the simulator executable (default)"
	@echo "  so         - Build shared library (libpycallclass.so)"
	@echo "  so-debug   - Build shared library with debug info"
	@echo "  so-release - Build optimized shared library"
	@echo "  debug      - Build executable with debug info"
	@echo "  release    - Build optimized executable"
	@echo "  clean      - Remove all build artifacts"
	@echo "  run        - Build and run the simulator"
	@echo "  help       - Show this help message"
	@echo ""
	@echo "Examples:"
	@echo "  make so         - Build shared library"
	@echo "  make so-debug   - Build shared library with debug symbols"
	@echo "  make so-release - Build optimized shared library"

# Debug build
debug: CXXFLAGS += -g -DDEBUG
debug: $(TARGET)

# Release build
release: CXXFLAGS += -DNDEBUG
release: $(TARGET)

.PHONY: all clean install-deps run so so-debug so-release debug release help
