OUT_INCLUDE_DIR=../../include
OUT_LIB_DIR=../../lib
OUT_LIB=$(OUT_LIB_DIR)/libdyngraph.a

CXX=g++
OPTFLAGS=-O3
CFLAGS=-std=c++0x
IFLAGS=-I../../deps/boost_libs/include -I../../include
LFLAGS=-L../../deps/boost_libs/lib -lboost_regex -L../../lib -lcommons

BUILD=ar -rcs
COMPILE_SRC=${CXX} ${OPTFLAGS} ${CFLAGS}

XMT=${MTANAME}
ifeq ($(XMT), mta)
	CXX=c++
	#CFLAGS=-par 
	CFLAGS=-par -pl ${OUT_LIB}
	IFLAGS=-I$(HOME)/dev/lib 
	OPTFLAGS=
	BUILD=${CXX} -pl 
	BUILD_FLAGS=-R
endif

TARGET=run
TEST=run_test.x
SRCS=$(shell ls *.cc)
HEADERS=$(shell ls *.h *.hpp)
OBJ_DIR=.obj
OBJS=$(SRCS:%.cc=$(OBJ_DIR)/%.o)

BUILD_LIB_CMD1=${BUILD} ${OUT_LIB} ${BUILD_FLAGS} ${OBJS}
BUILD_LIB_CMD2=ranlib ${OUT_LIB}

ifeq ($(XMT), mta)
	BUILD_LIB_CMD1=
	BUILD_LIB_CMD2=
endif


#all: ${TEST} ${TARGET}
all: ${TARGET}

${TEST}:${OBJS} 
	${CXX} ${OPTFLAGS} ${CFLAGS} -o $@ ${OBJS} ${LFLAGS}
${TARGET}: ${OBJS} ${HPPS} 
	cp *.h *.hpp ${OUT_INCLUDE_DIR}
	${BUILD_LIB_CMD1}
	${BUILD_LIB_CMD2}
# Added ${HEADERS} as a dependency so that it will automatically
# recompile every time the headers are modified, so we don't
# have to always call 'clean'
$(OBJ_DIR)/%.o: %.cc ${HEADERS}
	@mkdir -p $(OBJ_DIR)
	${CXX} ${CFLAGS} -o $@ -c ${OPTFLAGS} $< $(IFLAGS) 
clean:
	rm -fr ${OBJ_DIR} 
	rm -f ${OUT_LIB}
	for file in ${HEADERS}; do \
	    rm ${OUT_INCLUDE_DIR}/$$file; \
	done
