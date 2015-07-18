#!/bin/bash

if [ $# -ne 2 ]; then
  echo "Usage: ./compile_and_link_query.sh pathToNetflow.schema pathToQuery.graph"
  exit
fi

origdir=$GRAPH_HOME/src/search
currdir=`pwd`
echo $origdir
echo $currdir

if [ $currdir != $origdir ]; then
	echo "please execute command from " $origdir
	exit
fi

schemaFile=$1
queryFile=$2
queryRegex="*.graph"
python $GRAPH_HOME/src/search/compile_query.py $schemaFile $queryFile

tmp=`echo ${queryFile##*\/}` 
queryPrefix=`echo ${tmp%\.graph}`
#echo $queryPrefix

tmp=`echo ${schemaFile##*\/}` 
schemaPrefix=`echo ${tmp%\.schema}`
#echo $schemaPrefix

parsedQueryFileCC="query_mod_"$schemaPrefix"_"$queryPrefix".cc"
parsedQueryFileObj="query_mod_"$schemaPrefix"_"$queryPrefix".o"

echo $parsedQueryFileCC
echo $parsedQueryFileObj

g++ -fPIC -g -c -I ../../include -Wall $parsedQueryFileCC
g++ -shared $parsedQueryFileObj -o query_filter.so
cp query_filter.so  $GRAPH_HOME/lib/libquery_filter.so

cd $GRAPH_HOME/src/search/
make; make clean
cd  $GRAPH_HOME/src/streaming_search/
make; make clean
cd  $GRAPH_HOME/src/netflow/
make; make clean

echo "UPdate query file path = "  $queryFile "and any other properties in "  $GRAPH_HOME/src/netflow/netflow.attributesi

echo "run using ./run_test_lazy.x path_netflow.propeties path_query_file"


