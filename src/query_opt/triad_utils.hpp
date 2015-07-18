#ifndef __TRIAD_UTILS_HPP__
#define __TRIAD_UTILS_HPP__

#include "graph.hpp"
using namespace std;


template <typename Graph>
Graph* MakeTriadGraph(const string& signature)
{
    typedef typename Graph::DirectedEdgeType dir_edge;
    typedef typename Graph::VertexProp vertex_prop;
    Graph* triad = new Graph(3);
    // cout << "signature = " << signature << endl;
    int triad_type = signature[0] - 48;
    int u_type = 0; //signature[2] - 48;
    int v_type = 0; //signature[3] - 48;
    int w_type = 0; //signature[4] - 48;
    
    vertex_prop v_prop;
    v_prop.type = u_type;
    triad->AddVertex(0, v_prop);
    v_prop.type = v_type;
    triad->AddVertex(1, v_prop);
    v_prop.type = w_type;
    triad->AddVertex(2, v_prop);

    // cout << "*** MakeTriadGraph edge_type := 0" << endl;
    int uv_edge_type = 0; //signature[6] - 48;
    int uw_edge_type = 0; //signature[7] - 48;
    int vw_edge_type = 0; //signature[8] - 48;

    if (triad_type == 1) {
        dir_edge uv_edge(0, 1, uv_edge_type);
        dir_edge vw_edge(1, 2, vw_edge_type);
        triad->AddEdge(uv_edge);
        triad->AddEdge(vw_edge);
    }
    else if (triad_type == 2) {
        dir_edge uv_edge(0, 1, uv_edge_type);
        dir_edge vw_edge(2, 1, vw_edge_type);
        triad->AddEdge(uv_edge);
        triad->AddEdge(vw_edge);
    }
    else if (triad_type == 3) {
        dir_edge uv_edge(0, 1, uv_edge_type);
        dir_edge uw_edge(0, 2, uw_edge_type);
        triad->AddEdge(uv_edge);
        triad->AddEdge(uw_edge);
    }
    else if (triad_type == 4) {
        dir_edge uv_edge(0, 1, uv_edge_type);
        dir_edge uw_edge(1, 2, uw_edge_type);
        dir_edge vw_edge(2, 0, vw_edge_type);
        triad->AddEdge(uv_edge);
        triad->AddEdge(uw_edge);
        triad->AddEdge(vw_edge);
    }
    else if (triad_type == 5) {
        dir_edge uv_edge(0, 1, uv_edge_type);
        dir_edge uw_edge(2, 1, uw_edge_type);
        dir_edge vw_edge(2, 0, vw_edge_type);
        triad->AddEdge(uv_edge);
        triad->AddEdge(uw_edge);
        triad->AddEdge(vw_edge);
    }

    // cout << "Created triad graph +++++++++++++++++++++++++" << endl;
    // triad->Print();
    return triad;
}

#endif
