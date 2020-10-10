# *****************************************************
# Variables to control Makefile operation

CXX = g++
CXXFLAGS = -g -std=c++17 -I .
DEBUGFLAGS = -fsanitize=address -fsanitize=undefined -D_GLIBCXX_DEBUG

SRC := $(wildcard *.cpp)
OBJS = $(SRC:.cpp=.o)

EXEC_DIR = ./executors
EXEC_SRC := $(wildcard $(EXEC_DIR)/*.cpp)
EXEC_OBJS = $(EXEC_SRC:.cpp=.o)

ifdef DEBUG
CXXFLAGS += $(DEBUGFLAGS)
endif
# ****************************************************
# Macros

ifdef DEBUG
CXXFLAGS += $(DEBUGFLAGS)
endif
ifndef DEFER_WRITE
CXXFLAGS += -D DEFER_WRITE
else
ifneq ($(DEFER_WRITE), FALSE)
CXXFLAGS += -D DEFER_WRITE
endif
endif

# ****************************************************
# Targets needed to bring the executable up to date

all: server

server: $(OBJS) $(EXEC_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJS) $(EXEC_OBJS)

clean:
	rm -f *.o *~
	rm -f $(EXEC_DIR)/*.o $(EXEC_DIR)/*~
	rm -f server
	rm -f log

%.o: %.cpp global.h

$(EXEC_DIR)/%.o: $(EXEC_DIR)/.cpp global.h
