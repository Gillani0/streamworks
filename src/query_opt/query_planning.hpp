#ifndef __QUERY_PLANNING_HPP__
#define __QUERY_PLANNING_HPP__

#include <functional>
#include <map>
#include <ostream>
#include <limits.h>
#include "graph.hpp"
#include "counter.hpp"
#include "subgraph_search.hpp"
#include "sj_tree.h"
#include "query_planning.h"
using namespace std;

template <typename Graph>
std::map<int, float> GetEdgeDistribution(const Graph* graph)
{
    StreamingGraphDistribution edge_distribution_collector;
    typedef typename Graph::EdgeType Edge;

    for (int i = 0, N = graph->NumVertices(); i < N; i++) {
        int num_edges;
        const Edge* edges = graph->Neighbors(i, num_edges);
        for (int j = 0; j < num_edges; j++) {   
            if (i < edges[j].dst) {
                edge_distribution_collector.Update(edges[j].type);
            }   
        }   
    }   

    std::map<int, int> edge_distribution = 
            edge_distribution_collector.TakeSnapshot(graph->NumEdges());

    std::map<int, float> normalized_edge_distribution;

    long double sum = 0;
    for (std::map<int, int>::iterator it = edge_distribution.begin();
            it != edge_distribution.end(); it++) {
        sum += (float) it->second;
    }

    for (std::map<int, int>::iterator it = edge_distribution.begin();
            it != edge_distribution.end(); it++) {
        normalized_edge_distribution[it->first] = (float)(it->second/sum);
    }
    return normalized_edge_distribution;
}

/*
 * For now going with the min-degree heuristic.
 */
template <class Graph>
void InitSearchVertex(const Graph* g, set<uint64_t>& frontier)
{
    // int max_deg = -1;
    // uint64_t max_deg_v;
    int min_deg = 9999;
    uint64_t min_deg_v;

    for (int i = 0, N = g->NumVertices(); i < N; i++) {
        int deg = g->NeighborCount(i);
        /* if (deg > max_deg) {
            max_deg = deg;
            max_deg_v = i;
        } */
        if (deg < min_deg) {
            min_deg = deg;
            min_deg_v = i;
        }
    } 

    // frontier.insert(max_deg_v);
    cout << "Initialized frontier with " << min_deg_v << endl;
    frontier.insert(min_deg_v);
    return;
}

template <typename Graph>
float GetSingleEdgeDecompositionCost(const Graph* gQuery,
        std::map<int, float>& edge_type_distribution)
{
    typedef typename Graph::DirectedEdgeType DirectedEdge;
    typedef typename Graph::EdgeType Edge;

    float query_cost = 1.0;

    for (int i = 0, N = gQuery->NumVertices(); i < N; i++) {
        int neighbor_count;
        const Edge* edges = gQuery->Neighbors(i, neighbor_count);
        for (int j = 0; j < neighbor_count; j++) {
            if (i < edges[j].dst) {
                query_cost *= edge_type_distribution[edges[j].type];
            }
        } 
    }

    return query_cost;
}

template <typename Graph>
void SingleEdgeDecomposition(Graph* gQuery,
        const std::map<int, float>& edge_distribution,
        vector<Subgraph>& query_subgraphs, float* query_cost,
        const set<uint64_t>* init_frontier = NULL)
{
    typedef typename Graph::EdgeType edge_t;
    typedef typename Graph::DirectedEdgeType directed_edge_t;
        
    // uint64_t* frontier = new uint64_t[gQuery->NumVertices()];
    uint64_t* frontier = new uint64_t[1024];
    std::map<float, dir_edge_t>* candidates = new std::map<float, dir_edge_t>();
    int current_sz(0), num_removed_edges(0); 

    if (init_frontier && init_frontier->size()) {
        cout << "Initializing from init-frontier set ..." << endl;
        for (set<uint64_t>::const_iterator it = init_frontier->begin();
                it != init_frontier->end(); it++) {
            if (gQuery->NeighborCount(*it) > 0) {
                frontier[current_sz++] = *it;
                cout << "adding : " << *it << endl;
            }
        }
    }
    else {
        cout << "Initializing from vertex id set ..." << endl;
        for (int i = 0; i < gQuery->NumVertices(); i++) {
            if (gQuery->NeighborCount(i) > 0) {
                frontier[current_sz++] = i;
                cout << "adding : " << i << endl;
            }
        }
    }

    cout << "current_sz = " << current_sz << endl;
    while (gQuery->NumEdges()) {
        // std::map<float, dir_edge_t> candidates;
        for (int i = 0; i < current_sz; i++) {
            uint64_t u = frontier[i];
            int num_edges;
            const edge_t* edges = gQuery->Neighbors(u, num_edges);
            if (num_edges == 0) {
                continue;
            }

            dir_edge_t min_cost_edge;
            double min_cost = 100.0; // the entries in edge_distribution
            // are normalized to sum to 1.

            for (int j = 0; j < num_edges; j++) {
                float cost;
                std::map<int, float>::const_iterator it = 
                        edge_distribution.find(edges[j].type);
                // assert(it != edge_distribution.end());
                if (it == edge_distribution.end()) {
                    cout << "Probability unknown for edge type : " << edges[j].type << endl;
                    cout << "Setting probability to 0.0001" << endl;
                    cost = 0.0001;
                }
                else {
                    cost = it->second;
                }
                if (cost < min_cost) {
                    min_cost = cost;
                    dir_edge_t e;
                    if (edges[j].outgoing) {
                        e.s = u;
                        e.t = edges[j].dst;
                    }
                    else {  
                        e.t = u;
                        e.s = edges[j].dst;
                    }
                    min_cost_edge = e;
                }
            }

            if (min_cost != 100.0) {
                //candidates[min_cost] = min_cost_edge;
                // cout << "Adding candidate edge: " << min_cost_edge.s << " -> " << min_cost_edge.t << endl;
                candidates->insert(pair<float, dir_edge_t>(min_cost, min_cost_edge));
            }
        }

        assert(candidates->size());
        dir_edge_t remove_edge = candidates->begin()->second;
        *query_cost = (*query_cost)*candidates->begin()->first;
        Subgraph single_edge_subgraph;
        single_edge_subgraph.AddEdge(remove_edge);
        single_edge_subgraph.Finalize();
        query_subgraphs.push_back(single_edge_subgraph);

        num_removed_edges++;
        directed_edge_t dir_remove_edge;
        dir_remove_edge.s = remove_edge.s;
        dir_remove_edge.t = remove_edge.t;
        gQuery->RemoveEdge(dir_remove_edge, true);
        
        // If init_frontier was NULL, then frontier set contains all vertices in
        // gQuery.  Now that an edge has been removed, we want to reset the frontier
        // to onlt the endpoints of the removed edge.
        if (init_frontier == NULL && num_removed_edges == 1) {
            current_sz = 0;
        }
        
        frontier[current_sz++] = remove_edge.s;
        frontier[current_sz++] = remove_edge.t;
        sort(frontier, frontier + current_sz);
        uint64_t* last = unique(frontier, frontier + current_sz);
        current_sz = last - frontier;
        candidates->clear();
    }

    // delete[] frontier;
    // delete candidates;
    return;
}

template <typename Graph>
JoinTreeDAO* PathDecomposition(const Graph* gQuery, 
        const std::map<int, float>& edge_distribution,
        Graph** triad_graphs, float* triad_distribution, 
        int num_triads, float* query_cost)
{
    cout << "Decomposing query graph ..." << endl;
    typedef typename Graph::DirectedEdgeType DirectedEdge;
    typedef typename Graph::EdgeType Edge;
    set<uint64_t> frontier; 
    // float cost = 0; // When doing log
    float cost = 1;
    Graph* g = gQuery->Clone(); 

    GraphQuery<Graph>** triad_queries = new GraphQuery<Graph>*[num_triads];

    for (int i = 0; i < num_triads; i++) {
        triad_queries[i] = new GraphQuery<Graph>(g, 
                triad_graphs[i]);
    }

    // InitSearchVertex<Graph>(g, frontier);

    vector<Subgraph> query_subgraphs;
    int N_q = g->NumVertices();
    
    while (g->NumEdges()) {
        bool stat = false;
        // Greedy: Start by looking for the most frequent triad
        // Why was i initialized from 2?
        // for (int i = 2; i < num_triads; i++) {
        for (int i = 0; i < num_triads; i++) {
            GraphQuery<Graph>* triad_query = triad_queries[i];
            d_array<MatchSet*> match_results;
            MatchSet* match = NULL;
            if (frontier.size() == 0) {
                triad_query->Execute(match_results);
                if (match_results.len) {
                    // Take the first match
                    match = match_results[0];
                }
            }
            else {
                for (set<uint64_t>::iterator it = frontier.begin();
                        it != frontier.end(); it++) {
                    uint64_t search_vertex_in_query_graph = *it;
                    triad_query->Execute(search_vertex_in_query_graph, match_results);
                    if (match_results.len) {
                        match = match_results[0];
                        break;
                    }
                }
            }
            if (match) {
                for (int k = 0; k < match->vertex_map.len; k++) {
                     // cout << "Adding vertex to frontier: " 
                        // << match->vertex_map[k].second << endl;
                     frontier.insert(match->vertex_map[k].second);
                }
    
                Subgraph query_edge_list; // = Export(query_subgraph);
                cout << "------------" << endl;
                for (int k = 0; k < match->edge_map.len; k++) {
                    dir_edge_t e = match->edge_map[k].second;
                    query_edge_list.AddEdge(e);
                    DirectedEdge dir_e(e.s, e.t);
                    cout << "Removing edge: " << e.s << " -> " << e.t << endl;
                    bool stat = g->RemoveEdge(dir_e, true);
                    if (stat == false) {
                        cout << "Exiting on bad input ..." << endl;
                        exit(1);
                    }
                }
                cout << "------------" << endl;
                query_edge_list.Finalize();
                query_subgraphs.push_back(query_edge_list);
                stat = true;
                // cost += triad_distribution[i]; // if log
                cost *= triad_distribution[i];
                break;
            }
        }

        // No triads found, but the graph is non-empty
        if (stat == false || g->NumEdges() == 1) {
            // if (query_subgraphs.size() == 0) {
                // cout << "Warning: triad decomposition is not sufficient!" << endl;
            // }
            cout << "Decomposing graph with " << g->NumEdges() 
                 << " edges into single-edge subgraphs ..." << endl;
            cout << "Printing remaining graph ..." << endl;
            g->Print();
            SingleEdgeDecomposition(g, edge_distribution, query_subgraphs, &cost,
                    &frontier);
            break;
        }

        // If a vertex has no edges left, remove it from frontier
        for (int i = 0; i < N_q; i++) {
            if (g->NeighborCount(i) != 0) continue;
            set<uint64_t>::iterator it = frontier.find(i);
            if (it != frontier.end()) {
                frontier.erase(*it);
            }
        }

        if (g->NumEdges() == 1) {
            Subgraph single_edge_graph = Export(g);
            single_edge_graph.Finalize();
            query_subgraphs.push_back(single_edge_graph);
            break;
        }
    }

    cout << "**** Query Graph:" << endl;
    gQuery->Print(); 
    cout << "**** Number of decomposed subgraphs = " 
         << query_subgraphs.size() << endl;
    for (int i = 0, N = query_subgraphs.size(); i < N; i++) {
        const Subgraph& subg = query_subgraphs[i];
        cout << subg;
        cout << "----------------------" << endl; 
    }
    // delete[] triad_graphs;
    cout << "Query cost = " << cost << endl;
    *query_cost = cost;
    JoinTreeDAO* tree = SJTreeBuilder(query_subgraphs);
    return tree;
}

template <typename Graph>
JoinTreeDAO* DecomposeQuery(const Graph* gQuery, 
        const std::map<int, float>& edge_distribution,
        Graph** triad_graphs, float* triad_distribution, 
        int num_triads, float* query_cost, bool triad_decomposition = true)
{
    if (triad_decomposition) {
        return PathDecomposition(gQuery, edge_distribution, triad_graphs,
                        triad_distribution, num_triads, query_cost);
    }
    else {
        Graph* g = gQuery->Clone(); 
        vector<Subgraph> query_subgraphs;
        *query_cost = 1;
        cout << "Calling SingleEdgeDecomposition ..." << endl;
        SingleEdgeDecomposition(g, edge_distribution,
                query_subgraphs, query_cost);
        cout << "Freeing graph ..." << endl;
        // delete g;
        cout << "Building SJ-Tree ..." << endl;
        JoinTreeDAO* sj_tree_dao = SJTreeBuilder(query_subgraphs);
        cout << "Returning JoinTreeDAO*" << endl;
        return sj_tree_dao;
    }
}

#endif
