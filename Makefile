JAVA_HOME := /home/disk1/baidu_hg/jdk1.8.0_301

OUTPUT := output

all: test jniset

test:
	g++ -std=c++11 -O2 src/test_hashset.cpp -o $(OUTPUT)/test_hashset -lpthread

jniset:
	g++ -std=c++11 -O2 -fPIC -shared -I $(JAVA_HOME)/include -I $(JAVA_HOME)/include/linux jni/c/JniFastSet.cpp -o $(OUTPUT)/libJniFastSet.so	

clean:
	rm -f $(OUTPUT)/*

install: test jniset
	rm -f /tmp/libJniFastSet.so
	cp $(OUTPUT)/libJniFastSet.so /tmp/
