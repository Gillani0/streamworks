#!/usr/bin/python
import sys
import os
from sets import Set
import csv
import re

def gen_query_graph(fv_path, outpath):
        
    idmap = dict()
    reader = csv.reader(open(fv_path, 'r'), delimiter='\t')

    vertex_file_path = outpath + '/query.vertices'
    edge_file_path = outpath + '/query.edges'
    manifest_path = outpath + '/query.list'
    
    if os.path.exists(outpath) == False:
        os.makedirs(outpath)

    manifest_writer = open(manifest_path, 'w')
    manifest_writer.write(vertex_file_path + '\n')
    manifest_writer.write(edge_file_path + '\n')
    manifest_writer.close()

    edge_writer = open(edge_file_path, 'w')
    for line in reader:
        src_key = line[0] + '\tentity'
        if src_key in idmap:
            src_id = idmap[src_key]
        else:
            src_id = str(len(idmap))
            idmap[src_key] = src_id

        dst_key = line[1]
        if dst_key in idmap:
            dst_id = idmap[dst_key]
        else:
            dst_id = str(len(idmap))
            idmap[dst_key] = dst_id
         
        edge_writer.write(src_id + '\t' + dst_id + '\n')
    edge_writer.close()

    vertex_writer = open(vertex_file_path, 'w')
    for (k, v) in sorted(idmap.iteritems()):
        if k.rfind('entity', -6) == -1:
            pos = k.rfind(':')
            type = k[0:pos]
            label = k[pos+1:]
            if type != '':
                vertex_writer.write(v + '\t' + label + '\t' + type + '\n')
        else:
            vertex_writer.write(v + '\t' + k + '\n')
    vertex_writer.close()

    return manifest_path

# Main
data_home = '/home/d3m432/data/cassnlp/queries'
fv_dir_path = data_home + '/fvs/'
query_graph_dir_path = data_home + '/query_graphs/'

if len(sys.argv) == 2:
    files = [sys.argv[1],]
else:
    files = os.listdir(fv_dir_path)

pattern = re.compile('\.?\.tbd$')
f_manifest_list = open('query_manifest.list', 'w')

for file in files:
    entity_name = pattern.split(file)[0]
    file_path = fv_dir_path + file
    out_dir_path = query_graph_dir_path + entity_name
    manifest_path = gen_query_graph(file_path, out_dir_path)
    f_manifest_list.write(manifest_path + '\n')
    print('Wrote ' + manifest_path)

f_manifest_list.close()
