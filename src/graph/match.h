#ifndef __MATCH_SET_H__
#define __MATCH_SET_H__

#include <limits.h>
#include <assert.h>
#include "graph.hpp"
#include "subgraph.hpp"
#include <iostream>
using namespace std;

struct MatchSet {

    typedef pair<dir_edge_t, dir_edge_t> EdgePair;
    typedef pair<uint64_t, uint64_t> VertexPair;
    
    d_array<pair<uint64_t, uint64_t> > vertex_map;
    d_array<pair<dir_edge_t, dir_edge_t> > edge_map;

    uint64_t GetMatch(uint64_t u) const
    {
        bool here = true;
        for (int i = 0; i < vertex_map.len; i++) {
            if (vertex_map[i].first == u) {
                return vertex_map[i].second;
            }
        }
        assert(here == false);
        return ULONG_MAX;
    }

    inline int NumEdges() const
    {
        return edge_map.len;
    }

    inline int NumVertices() const
    {
        return vertex_map.len;
    }

    dir_edge_t GetMatch(const dir_edge_t& e) const
    {
        bool here = true;
        for (int i = 0; i < edge_map.len; i++) {
            if (edge_map[i].first == e) {
                return edge_map[i].second;
            }
        }
        assert(here == false);
        dir_edge_t e_bogus;
        return e_bogus;
    }

    void Print() const
    {
        cout << "-------------------------------" << endl;
        for (int i = 0, num_vertices = NumVertices(); i < num_vertices; i++) {
            cout << "V[" << vertex_map[i].first << "] -> " << vertex_map[i].second << endl;
        }
        for (int i = 0, num_edges = NumEdges(); i < num_edges; i++) {
            pair<dir_edge_t, dir_edge_t> edge_pair = edge_map[i];
            cout << "E_q:" << edge_pair.first << endl;
            cout << "E_s:" << edge_pair.second << endl;
        } 
        cout << "..............................." << endl;
        return;
    }

    Subgraph GetMatchedSubgraph()
    {
        Subgraph matched_subgraph;
        for (int i = 0, M = edge_map.len; i < M; i++) {
            matched_subgraph.AddEdge(edge_map[i].second);
        }
        matched_subgraph.Finalize();
        return matched_subgraph;
    }
};

template <typename Graph>
void PrintMatch(const Graph* gSearch, const MatchSet& match)
{   
    cout << "-------------------------------" << endl;
    for (int i = 0; i < match.NumVertices(); i++) {
        int v = match.vertex_map[i].second;
        cout << "V["  << v << "] = " 
                << gSearch->GetVertexProperty(v).vertex_data.label 
                << endl;
    }   
    for (int i = 0; i < match.NumEdges(); i++) {
        pair<dir_edge_t, dir_edge_t> edge_pair = match.edge_map[i];
        cout << "E_q:" << edge_pair.first << endl;
        cout << "E_s:" << edge_pair.second << endl;
    }   
}   

#endif
