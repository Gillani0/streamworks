CXX=g++
OPTFLAGS=-O3
IFLAGS=-I../../include -I../../deps/boost_libs/include
#LFLAGS=-L../../deps/boost_libs/lib -lboost_regex -L../../lib -ldyngraph -ldyngraph_search -lcommons -lquery_opt
LFLAGS=-L../../deps/boost_libs/lib -lboost_regex -L../../lib -lquery_opt -ldyngraph -ldyngraph_search -lcommons

#TARGET=nyt
#NYT_BIN=run_test_all_nyt.x
#NYT_BIN=run_test_lazy_nyt.x

#CFLAGS=-DTIMING
#TARGET=run_test_all.x

CFLAGS=-DTIMING -DTRY_LAZY_SEARCH
TARGET=run_test_lazy_lj.x

#CFLAGS=-DTIMING -DVF2
#TARGET=run_test_vf2.x

SRCS=$(shell ls *.cc)
OBJ_DIR=.obj
#OBJS=$(SRCS:%.cc=$(OBJ_DIR)/%.o)
# OBJS=${OBJ_DIR}/rdf_driver.o ${OBJ_DIR}/rdf_stream_processor.o
OBJS=${OBJ_DIR}/rdf_driver.o ${OBJ_DIR}/livejournal_stream_processor.o
#OBJS=${OBJ_DIR}/new_york_times_parser.o 
HPPS=$(shell ls *.h *.hpp)

#all: ${TARGET}

#${TARGET}: ${OBJS} ${HPPS} 
	#${CXX} ${OPTFLAGS} -o $@ ${OBJS} ${LFLAGS} 
#nyt: ${OBJ_DIR}/new_york_times_parser.o
	#${CXX} ${OPTFLAGS} -o ${NYT_BIN} ${OBJ_DIR}/new_york_times_parser.o ${LFLAGS} 
${TARGET}: ${OBJS} ${HPPS}
	#${CXX} ${OPTFLAGS} -o $@ ${OBJ_DIR}/rdf_driver.o ${OBJ_DIR}/rdf_stream_processor.o ${LFLAGS} 
	${CXX} ${OPTFLAGS} -o $@ ${OBJ_DIR}/rdf_driver.o ${OBJ_DIR}/livejournal_stream_processor.o ${LFLAGS} 
$(OBJ_DIR)/%.o: %.cc
	@mkdir -p $(OBJ_DIR)
	${CXX} ${OPTFLAGS} -o $@ ${CFLAGS} -c $< $(IFLAGS) 
clean:
	rm -fr ${TARGET} ${OBJS} 
