#ifndef __GRAPH_ITERATION_MACROS__HPP__
#define __GRAPH_ITERATION_MACROS_HPP__

#define FORALL_VERTICES(graph) \
for (int i = 0, N = graph->NumVertices(); i < N; i++)

#define FORALL_EDGES(edges, num_edges) \
for (int i = 0; i < num_edges; i++) 

#define LOGSRC cout << __FILE__ << ":" << __LINE__ << " "
#endif
