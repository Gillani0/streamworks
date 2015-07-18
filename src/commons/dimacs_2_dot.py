#!/usr/bin/python
import sys
f = open(sys.argv[1], 'r')
print('digraph nccdc_graph {')
for line in f:
    if line[0] == 'a':
        tokens = line.split(' ')
        print(tokens[1] + ' -> ' + tokens[2]) 

f.close()
print('}')
