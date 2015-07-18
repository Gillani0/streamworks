#ifndef __EXPORT_GIRAPH_HPP__
#define __EXPORT_GIRAPH_HPP__
#include <fstream>
#include <iostream>
#include "graph.hpp"
using namespace std;

template <class VertexData, class EdgeData>
class GiraphExporter {
public:
    GiraphExporter() {}

    void SetOutpath(string p) 
    {
        outpath__ = p;
        return;
    }

    void JsonLongDoubleFloatDoubleVertexInputFormat(const Graph<VertexData, EdgeData>* g)
    {
        ofstream ofs(outpath__.c_str(), ios_base::out);
        if (ofs.is_open() == false) {
            cerr << "Failed to open: " << outpath__ << endl;
        }
        else {
            cout << "Saving graph in JsonLongDoubleFloatDoubleVertexInputFormat ..." << endl;
            cout << "outpath: " << outpath__ << endl;
        }

        for (uint64_t i = 0, N = g->NumVertices(); i < N; i++) {
            int neighbor_count;
            const Edge<EdgeData>* neighbors = g->Neighbors(i, neighbor_count);
            const VertexProperty<VertexData>& v_prop = g->GetVertexProperty(i);
            ofs << "[" << i << ", " << v_prop.type << ", [";
            if (neighbor_count) {
                for (uint64_t j = 0; j < neighbor_count-1; j++) {
                    if (neighbors[j].outgoing) {
                        ofs << "[" << neighbors[j].dst << ", " << (float) (neighbors[j].type) << "], ";
                    }
                }
                if (neighbors[neighbor_count-1].outgoing) {
                    ofs << "[" << neighbors[neighbor_count-1].dst << ", " 
                        << (float) (neighbors[neighbor_count-1].type) << "]";
                }
            }
            ofs << "]]" << endl;
        }
        ofs.close();
        return;
    }

    void operator()(const Graph<VertexData, EdgeData>* g) 
    {
        JsonLongDoubleFloatDoubleVertexInputFormat(g);
        return;
    }

private:
    string outpath__;
};


#endif
