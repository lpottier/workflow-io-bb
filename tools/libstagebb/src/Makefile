# Author: Loic Pottier <lpottier@isi.edu>
# Created: 02/04/2020

CC            := gcc
CFLAGS        := -ansi -Wall -Wextra -O0
LDFLAG        := 
UNAME_S       := $(shell uname -s)

TARGET        := main

ifeq ($(UNAME_S),Darwin)
	TARGET_LIB    := libstagebb.dylib
endif

ifeq ($(UNAME_S),Linux)
	TARGET_LIB    := libstagebb.so
endif

RM            := rm -f

SRC           := main.c
LIB           := libstagebb.c
OBJ           := main.o

.PHONY: all clean distclean $(TARGET) $(TARGET_LIB)

all: clean $(TARGET) $(TARGET_LIB)

$(TARGET): $(OBJ)
	$(CC) -o $(TARGET) $(LDFLAGS) $(OBJ)

$(TARGET_LIB):
	$(CC) -shared -fPIC -ldl -o $(TARGET_LIB) $(LIB) 

$(OBJ): $(SRC)
	$(CC) -c $(CFLAGS) $(SRC)

clean:
	$(RM) $(OBJ)

distclean: clean
	$(RM) $(TARGET) $(TARGET_LIB)
