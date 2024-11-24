# Variables
CXX = g++
CXXFLAGS = -O3 -Wno-pointer-arith
TARGET = pipeline
SRCS = test.cc

# Default target
all: $(TARGET)

# Build target
$(TARGET): $(SRCS)
	$(CXX) -o $@ $(CXXFLAGS) $^

# Clean target
clean:
	rm -f $(TARGET)
