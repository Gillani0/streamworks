#include <algorithm>
#include <iosfwd>
#include "subgraph.hpp"
using namespace std;

Subgraph graph_union(const Subgraph& g1, const Subgraph& g2)
{
    Subgraph g_o;
    dir_edge_t* out_edge_list = new dir_edge_t[g1.EdgeCount() +
            g2.EdgeCount()]; 
    dir_edge_t* last = set_union(g1.BeginEdgeSet(), 
                g1.EndEdgeSet(), 
                g2.BeginEdgeSet(),
                g2.EndEdgeSet(), out_edge_list);
    int num_total_edges = last - out_edge_list;
    
    for (int i = 0; i < num_total_edges; i++) {
        g_o.AddEdge(out_edge_list[i]);
    }   

    uint64_t* all_nodes = new uint64_t[g1.VertexCount() + 
            g2.VertexCount()];
    uint64_t* last_node = set_union(g1.BeginVertexSet(),
            g1.EndVertexSet(), g2.BeginVertexSet(), g2.EndVertexSet(),
            all_nodes);
    int num_union_nodes = last_node - all_nodes;
    
    for (int i = 0; i < num_union_nodes; i++) {
        g_o.AddVertex(all_nodes[i]);
    }

    delete[] all_nodes;
    delete[] out_edge_list;
    g_o.Finalize();
    return g_o;
}

Subgraph graph_intersection(const Subgraph& g1, const Subgraph& g2) 
{
    Subgraph g_i;
    dir_edge_t* out_edge_list = new dir_edge_t[g1.EdgeCount() +
            g2.EdgeCount()]; 
    dir_edge_t* last = set_intersection(g1.BeginEdgeSet(), 
                g1.EndEdgeSet(), 
                g2.BeginEdgeSet(),
                g2.EndEdgeSet(), out_edge_list);
    int num_common_edges = last - out_edge_list;
    
    for (int i = 0; i < num_common_edges; i++) {
        g_i.AddEdge(out_edge_list[i]);
    }   

    uint64_t* all_nodes = new uint64_t[g1.VertexCount() + 
            g2.VertexCount()];
    uint64_t* last_common_node = set_intersection(g1.BeginVertexSet(),
            g1.EndVertexSet(), g2.BeginVertexSet(), g2.EndVertexSet(),
            all_nodes);
    int num_common_nodes = last_common_node - all_nodes;
    
    for (int i = 0; i < num_common_nodes; i++) {
        g_i.AddVertex(all_nodes[i]);
    }

    delete[] all_nodes;
    delete[] out_edge_list;
    g_i.Finalize();
    return g_i;
}

ostream& operator<<(ostream& os, const Subgraph& graph)
{
    for (Subgraph::EdgeIterator it = graph.BeginEdgeSet();
            it != graph.EndEdgeSet(); it++) {
        os << it->s << "->" << it->t << endl;
    }   
    return os; 
}

void WriteStreamingGephiInput(const dir_edge_t& e, ostream& ofs)
{
    // {"ae":{"AB":{"source":"A","target":"B","directed":false,"weight":2}}} 
    ofs << "{\"ae\":{\"" << e.s << "_" << e.t << "\":{\"source\":\"";
    ofs << e.s << "\",\"target\":\"" << e.t;
    ofs << "\",\"directed\":true,\"timestamp\":";
    ofs << e.timestamp << "}}}" << endl;
}

void WriteStreamingGephiInput(const Subgraph& graph, ostream& ofs)
{
    for (Subgraph::EdgeIterator it = graph.BeginEdgeSet();
            it != graph.EndEdgeSet(); it++) {
        WriteStreamingGephiInput(*it, ofs);
    }   
    return;
}
