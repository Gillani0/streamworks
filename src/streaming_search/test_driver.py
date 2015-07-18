#!/usr/bin/python
import os, sys
#import argparse

gConf = dict()
gConfName = 'test/lsbench.properties'
gBin = 'run_test.x'
gQueryPath = ''

def count_files_of_type(dirpath, pattern):
    files = os.listdir(dirpath)
    num_files_processed = 0
    for f in files:
        if f.find(pattern) != -1:
            num_files_processed += 1
    return num_files_processed

def load_conf():
    #f = open('test/lsbench.properties')
    f = open(gConfName, 'r')
    for line in f:
        line = line.strip()
        if len(line) == 0 or line[0] == '#':
            continue
        tokens = line.split(' ')
        gConf[tokens[0]] = tokens[1]
    f.close()

def purge_plans(dirpath):
    files = os.listdir(dirpath)
    num_files_processed = 0
    for f in files:
        if f.find('plan') != -1:
            print('Removing ' + dirpath + '/' + f)
            os.system('rm ' + dirpath + '/' + f)

def make_query_list(dirpath):
    files = sorted(os.listdir(dirpath))
    query_list = []
    for f in files:
        plan1 = dirpath + '/' + f + '.plan.1'
        plan2 = dirpath + '/' + f + '.plan.2'
        if os.path.exists(plan1) and os.path.exists(plan2):
            query_list.append(dirpath + '/' + f)
            if len(query_list) == 100:
                break

    fout = open(dirpath + '/query100.list', 'w')
    print('Listed ' + str(len(query_list)) + \
        ' queries in ' + dirpath)
    for f in query_list:
        fout.write(f + '\n')
    fout.close()

def process_queries(dirpath):
    files = sorted(os.listdir(dirpath))
    num_files_processed = 0
    num_graphs = 0
    run_search = gConf['run_search']

    if run_search == 'yes':
        list_file = dirpath + '/query100.list'
        f_list = open(list_file, 'w')

    for f in files:
        if f.endswith('graph'):
            num_graphs += 1
            path = dirpath + '/' + f
            if run_search != 'yes':
                #cmd = 'run_test_lazy.x test/lsbench.properties ' \
                cmd = gBin + ' ' + gConfName + ' ' \
                    + path + ' >> query_gen.log'
                print('Executing: ' + cmd)
                os.system(cmd) 
            else:
                f_list.write(path + '\n')

    if run_search == 'yes':
        f_list.close()
        #cmd = 'run_test_lazy.x test/lsbench.properties ' + list_file
        cmd = gBin + ' ' + gConfName + ' ' + list_file
        print('***********************************************')
        print('Processing file list: ' + list_file)
        print('Running command : ' + cmd);
        print('***********************************************')
        os.system(cmd)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--test_type', \
            help='specify search technique [all](default): \
            searches for all subgraphs or [lazy]', default='all')
    parser.add_argument('--benchmark', help='Data source type: [netflow]|[lsbench]')
    parser.add_argument('--make_query_list', help='Prepare \
        .list file from specified directory')
    parser.add_argument('--queries', help='Directory with query graphs')
    args = parser.parse_args()

    if args.make_query_list != None:
        make_query_list(args.make_query_list)
        sys.exit(0)

    if args.test_type == 'all':
        gBin = 'run_test.x'
    elif args.test_type == 'lazy':
        gBin = 'run_test_lazy.x'
    else:
        print('Unknown search option: ' + args.test_type)
        args.print_help()
        sys.exit(1)

    if args.benchmark == None:
        print('Argument: --benchmark required')
        args.print_help()
        sys.exit(1)
    elif args.benchmark == 'netflow' or args.benchmark == 'lsbench':
        gConfName = 'test/' + args.benchmark + '.properties'
    else:
        print('Unknown search benchmark: ' + args.benchmark)
        sys.exit(1)

    if args.queries == None:
        print('Argument: --queries required')
        args.print_help()
        sys.exit(1)
    else:
        gQueryPath = args.queries

#parse_args()
load_conf()
#process_queries(gQueryPath)
process_queries('test/synthetic/queries/path4')
process_queries('test/synthetic/queries/path5')
make_query_list('test/synthetic/queries/path4')
make_query_list('test/synthetic/queries/path5')
