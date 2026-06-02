# Makefile for pub-sub demo
CXX = g++
CXXFLAGS = -std=c++17 -O2 -pthread
SRC = $(wildcard *.cpp)
TARGET = pubsub
LOG = build.log

.PHONY: all build clean

all: build

build:
	@echo "Building $(TARGET)..." > $(LOG)
	$(CXX) $(CXXFLAGS) $(SRC) -o $(TARGET) >> $(LOG) 2>&1

clean:
	-@rm -f $(TARGET) $(LOG)
	-@echo "Cleaned."
