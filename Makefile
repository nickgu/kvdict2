
VERSION = 1.1.0
CPPFLAGS =  -D__VERSION_ID__="\"$(VERSION)\"" -g -Wall -O2 -fPIC  -pipe -D_REENTRANT -DLINUX -Wall
PYTHON_HEADER_DIR = /home/users/gusimiu/.jumbo/include/

TARGET=c_kvdict2.so
INCLUDES = -Iinclude/ \
		   -I$(PYTHON_HEADER_DIR) 
		  
LIBS =  -lcrypto \
	   -lpthread

all: clean $(TARGET)
	@echo 'MAKE: ALL'
	mkdir output
	mv $(TARGET) output/
	cp -r include output/
	cp script/*.py output/

c_kvdict2.so: src/c_kvdict2.cc
	@echo 'MAKE: KVDICT'
	g++ -shared -fpic $^ -o $@ $(LIBS) $(CPPFLAGS) $(INCLUDES) 

test_kvindex: test_kvindex.cc
	@echo 'MAKE: test_kvindex'
	g++ $^ -o $@ $(CPPFLAGS)

clean:
	rm -rf *.o *~ script/*.pyc *.pyc $(TARGET)
	rm -rf output





