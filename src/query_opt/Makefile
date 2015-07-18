OUT_INCLUDE_DIR=../../include

CXX=g++
OPTFLAGS=
CFLAGS=-g -DDEBUG -std=c++0x
IFLAGS=-I../../include -I../../deps/boost
LFLAGS=-L../../deps/boost_libs/lib -lboost_regex -L../../lib -ldyngraph -ldyngraph_search

OUT_LIB=../../lib/libquery_opt.a

TARGET=run_test.x
SRCS=$(shell ls *.cc)
HEADERS=$(shell ls *.h *.hpp)
OBJ_DIR=.obj
OBJS=$(SRCS:%.cc=$(OBJ_DIR)/%.o)

all: ${TARGET}

${TARGET}: ${HEADERS}  ${OBJS}
	#${CXX} ${OPTFLAGS} ${CFLAGS} -o $@ ${OBJS} ${LFLAGS}
	cp ${HEADERS} ${OUT_INCLUDE_DIR}
	ar -rcs ${OUT_LIB} ${OBJS}
	ranlib ${OUT_LIB}
$(OBJ_DIR)/%.o: %.cc ${HEADERS}
	@mkdir -p $(OBJ_DIR)
	${CXX} ${CFLAGS} -o $@ -c ${OPTFLAGS} $< $(IFLAGS) ${LFLAGS}
clean:
	rm -fr ${OBJ_DIR} 
	rm -fr ${OUT_LIB} 
	for file in ${HEADERS}; do \
	    rm ${OUT_INCLUDE_DIR}/$$file; \
	done
