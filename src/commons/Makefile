OUT_INCLUDE_DIR=../../include
OUT_LIB_DIR=../../lib
OUT_LIB=$(OUT_LIB_DIR)/libcommons.a

CXX=g++
OPTFLAGS=-O3
CFLAGS=
IFLAGS=-I../../include -I../../deps/boost

BUILD=ar -rcs
COMPILE_SRC=${CXX} ${OPTFLAGS} ${CFLAGS}

TARGET=run
TEST=run_test.x
SRCS=$(shell ls *.cc)
HEADERS=$(shell ls *.h *.hpp)
OBJ_DIR=.obj
OBJS=$(SRCS:%.cc=$(OBJ_DIR)/%.o)

BUILD_LIB_CMD1=${BUILD} ${OUT_LIB} ${BUILD_FLAGS} ${OBJS}
BUILD_LIB_CMD2=ranlib ${OUT_LIB}

all: ${TEST} ${TARGET}
#all: ${TARGET}

${TEST}:
	${CXX} ${CFLAGS} test_commons.cpp $(IFLAGS) 
${TARGET}: ${OBJS} ${HPPS} 
	cp ${HEADERS} ${OUT_INCLUDE_DIR}
	cp gephi_streaming_cmds.txt ~/.gephi_streaming_templates
	${BUILD_LIB_CMD1}
	${BUILD_LIB_CMD2}
# Added ${HEADERS} as a dependency so that it will automatically
# recompile every time the headers are modified, so we don't
# have to always call 'clean'
$(OBJ_DIR)/%.o: %.cc ${HEADERS}
	@mkdir -p $(OBJ_DIR)
	${CXX} ${CFLAGS} -o $@ -c ${OPTFLAGS} $< $(IFLAGS) 
clean:
	rm ${TEST}
	rm -fr ${OBJ_DIR} 
	rm -f ${OUT_LIB}
	for file in ${HEADERS}; do \
	    rm ${OUT_INCLUDE_DIR}/$$file; \
	done
