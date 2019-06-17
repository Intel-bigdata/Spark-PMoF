.PHONY:all
CC=g++

INCLUDES=-I${JAVA_HOME} -I${JAVA_HOME}/include/linux -I${JAVA_HOME}/include
CFLAGS=-std=c++11 -fPIC -shared -fno-strict-overflow -fno-delete-null-pointer-checks -fwrapv -o0
LIBS=-lpmemobj -lpthread
MAIN=libjnipmdk.so


all: $(MAIN) install

$(MAIN):
	$(CC) $(CFLAGS) $(INCLUDES) -o $(MAIN) lib_jni_pmdk.cpp PersistentMemoryPool.cpp Request.cpp

install:
	cp -rf $(MAIN) /usr/lib/
	cp -rf $(MAIN) /usr/lib64/
