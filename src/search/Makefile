OUT_INCLUDE_DIR=../../include

CXX=g++
OPTFLAGS=-g
CFLAGS=-g -std=c++11
IFLAGS=-I../../include -I../../deps/boost_libs/include
LFLAGS=-L../../deps/boost_libs/lib -lboost_regex -L../../lib -ldyngraph

COMPILE_SRC=${CXX} ${OPTFLAGS} ${CFLAGS}

XMT=${MTANAME}
ifeq ($(XMT), mta)
	CXX=c++
	#CFLAGS=-par 
	IFLAGS=-I$(HOME)/dev/lib 
	OPTFLAGS=
endif

TARGET=run_test.x
SRCS=$(shell ls *.cc)
HEADERS=$(shell ls *.h *.hpp)
OBJ_DIR=.obj
OBJS=$(SRCS:%.cc=$(OBJ_DIR)/%.o)

all: ${TARGET}
cp: ${HEADERS}
	cp ${HEADERS} ${OUT_INCLUDE_DIR}

${TARGET}: ${OBJS} ${HEADERS} 
	${CXX} ${OPTFLAGS} ${CFLAGS} -o $@ ${OBJS} ${LFLAGS}
	cp ${HEADERS} ${OUT_INCLUDE_DIR}
$(OBJ_DIR)/%.o: %.cc ${HEADERS}
	@mkdir -p $(OBJ_DIR)
	${CXX} ${CFLAGS} -o $@ -c ${OPTFLAGS} $< $(IFLAGS) 
clean:
	rm -fr ${OBJ_DIR} 
	for file in ${HEADERS}; do \
	    rm ${OUT_INCLUDE_DIR}/$$file; \
	done
