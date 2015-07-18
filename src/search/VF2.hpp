#ifndef __VF2_HPP__
#define __VF2_HPP__

#include "graph.hpp"
#include <algorithm>
#include <set>
using namespace std;

template <class Graph>
void Get2HopNeighborSummary(const Graph* g, uint64_t u, 
        int& neighbor_count_2_hop, set<int>& neighbor_types)
{
    typedef typename Graph::EdgeType edge_t;
    int neighbor_count1;
    neighbor_count_2_hop = 0;

    const edge_t* neighbors1 = g->Neighbors(u, neighbor_count1);

    for (int i = 0; i < neighbor_count1; i++) {
        int neighbor_count2;
        const edge_t* neighbors2 = g->Neighbors(neighbors1[i].dst,
            neighbor_count2);
        neighbor_count_2_hop += neighbor_count2;
        for (int j = 0; j < neighbor_count2; j++) {
            neighbor_types.insert(neighbors2[j].type);
        }
    }
    return;
}

template <class Graph>
bool VF2Check(const Graph* gSearch, 
        const Graph* gQuery, uint64_t v_s, uint64_t v_q)
{
    int neighbor_count_2_hop_s, neighbor_count_2_hop_q;
    set<int> type_s, type_q;

    Get2HopNeighborSummary(gSearch, v_s, neighbor_count_2_hop_s, type_s);
    Get2HopNeighborSummary(gQuery, v_q, neighbor_count_2_hop_q, type_q);

    if (neighbor_count_2_hop_s < neighbor_count_2_hop_q) {
        return false;
    }

    if (type_q.size() > 0 && 
        includes(type_s.begin(), type_s.end(), type_q.begin(), type_q.end()) == false) {
        return false;
    }
    return true;
}

#endif
