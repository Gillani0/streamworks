/**
 * @author Sutanay Choudhury
 */
#ifndef __SUBGRAPH_HPP__
#define __SUBGRAPH_HPP__

#include "edge.hpp"
#include "utils.h"
using namespace std;

/**
 * This is intended to provide a "view" of a Graph
 * than be a edge-list based graph representation.
 * Unlike Graph, this class does not store any vertex
 * or edge property information.  
 */

class Subgraph {
public:
    typedef const uint64_t* VertexIterator;
    typedef const dir_edge_t* EdgeIterator;

    Subgraph() {}

    Subgraph(int vertex_count_est, int edge_count_est) :
            vertices__(vertex_count_est), edges__(edge_count_est)  {}

    Subgraph(const Subgraph& other) : vertices__(other.vertices__),
            edges__(other.edges__)  {}

    ~Subgraph() {}

    // Property accessors
    inline int VertexCount() const { return vertices__.len; }
    inline int EdgeCount() const { return edges__.len; }
    
    // Modifiers
    inline void AddVertex(int v) { vertices__.append(v); }
    inline void AddEdge(const dir_edge_t& e)
    {
        edges__.append(e);
        vertices__.append(e.s);
        vertices__.append(e.t);
    }

    // Turn the vertex set into a sorted set of unique elements
    // The vertex set is build by AddVertex and AddEdge operations
    // and may not contain unique elements.
    // example:  AddEdge(1, 2) and AddEdge(1, 3) -> {1,2,1,3}
    inline void Finalize()  
    { 
        vertices__.MakeSet(); 
        edges__.MakeSet(); 
    }

    // Range accessors
    inline VertexIterator BeginVertexSet() const { return vertices__.begin(); }
    inline VertexIterator EndVertexSet() const { return vertices__.end(); }
    inline EdgeIterator BeginEdgeSet() const { return edges__.begin(); }
    inline EdgeIterator EndEdgeSet() const { return edges__.end(); }

// private:
    d_array<uint64_t> vertices__;
    d_array<dir_edge_t> edges__;
};

ostream& operator<<(ostream& os, const Subgraph& graph);
void WriteStreamingGephiInput(const Subgraph& g, ostream& ofs);
void WriteStreamingGephiInput(const dir_edge_t& e, ostream& ofs);

template <typename Graph>
Subgraph Export(const Graph* g)
{
    typedef typename Graph::EdgeDataType E;    
    Subgraph subg;
    for (int i = 0, N = g->NumVertices(); i < N; i++) {
        int num_edges;
        const Edge<E>* edges = g->Neighbors(i, num_edges);
        for (int j = 0; j < num_edges; j++) {
            if (edges[j].outgoing) {
                dir_edge_t e;
                e.s = i;
                e.t = edges[j].dst;
                subg.AddEdge(e);
            }
        }
    }
    return subg; 
}

Subgraph graph_union(const Subgraph& g1, const Subgraph& g2);
Subgraph graph_intersection(const Subgraph& g1, const Subgraph& g2);

#endif
