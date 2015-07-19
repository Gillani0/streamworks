OUT_INCLUDE_DIR=../../include
OUT_LIB=../../lib/libnetflow.a

CXX=g++
OPTFLAGS=-O3
#CFLAGS=-DEXPORT_MAPVIEW

CFLAGS=-DTIMING -DVF2i -std=c++0x
#TARGET=run_test_vf2.x

#CFLAGS=-DTIMING -DTRY_LAZY_SEARCH
TARGET=run_test_lazy.x

#CFLAGS=-DTIMING
#TARGET=run_test_all.x

IFLAGS=-I../../include -I../commons/ -I../../deps/boost
LFLAGS=-L../../deps/boost_libs/lib -lboost_regex -L../../lib -ldyngraph -ldyngraph_search -lcommons -lquery_opt -lquery_filter
#LFLAGS=-L../../deps/boost_libs/lib -lboost_regex -L../../lib -ldyngraph -ldyngraph_search -lcommons -lquery_opt #-lquery_filter

#TARGET=run_gephi.x
#TARGET=compile_query.x
SRCS=$(shell ls *.cc)
HEADERS=$(shell ls *.hpp)
OBJ_DIR=.obj
OBJS=$(SRCS:%.cc=$(OBJ_DIR)/%.o)

all: ${TARGET}

${TARGET}: ${OBJS} ${HPPS} 
	${CXX} ${OPTFLAGS} ${CFLAGS} -o $@ ${OBJS} ${LFLAGS}
	ar -rcs ${OUT_LIB} .obj/netflow_parser.o
	ranlib ${OUT_LIB}
	cp schema_netflow.h netflow_parser.h ${OUT_INCLUDE_DIR}
$(OBJ_DIR)/%.o: %.cc ${HPPS}
	@mkdir -p $(OBJ_DIR)
	${CXX} ${CFLAGS} -o $@ -c ${OPTFLAGS} $< $(IFLAGS) 
clean:
	rm -fr ${OBJ_DIR} 
	for file in ${HEADERS}; do \
	    rm ${OUT_INCLUDE_DIR}/$$file; \
	done
