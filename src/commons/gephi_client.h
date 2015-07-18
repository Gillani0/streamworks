#ifndef __GEPHI_CLIENT_H__
#define __GEPHI_CLIENT_H__

#include <fstream>
#include <iostream>
#include <stdlib.h>
#include <boost/unordered_set.hpp>
using namespace std;
using namespace boost;

class GephiStreamingClient {
public:
    static GephiStreamingClient& GetInstance()
    {
        static GephiStreamingClient c;
        return c;
    }

    void PostEdge(int s, int t, int type = 0)
    {
        unordered_set<int>::iterator it = vertex_set__.find(s); 
        if (it == vertex_set__.end()) {
            AddVertex(s);
            vertex_set__.insert(s);
        }
        it = vertex_set__.find(t); 
        if (it == vertex_set__.end()) {
            AddVertex(t);
            vertex_set__.insert(t);
        }
        AddEdge(s, t, type);
    }

private:
    GephiStreamingClient(const GephiStreamingClient& gc);
    void operator=(const GephiStreamingClient& gc);

    GephiStreamingClient() 
    {
        string home = getenv("HOME");
        string file = home + "/.gephi_streaming_templates";
        ifstream ifs(file.c_str(), ios_base::in);
        if (ifs.is_open() == false) {
            cout << "Failed to load Gephi streaming commands" << endl;
            cout << "From path=" << file << endl;
            return;
        }

        getline(ifs, add_vertex_cmd_format__);
        getline(ifs, add_edge_cmd_format__);
        getline(ifs, rm_edge_cmd_format__);
        ifs.close();
        colors[0] = "rgb(255, 255, 0)"; // yellow
        colors[1] = "rgb(0, 0, 204)"; // "blue"; 
        colors[2] = "rgb(0, 102, 0)"; // "green";
        colors[3] = "rgb(204, 0, 0)"; // "red";
        colors[4] = "rgb(0, 0, 0)"; // "black";
        weights[0] = 20;
        weights[1] = 40;
        weights[2] = 20;
        weights[3] = 60;
        weights[4] = 40;
        return;
    }

    void AddVertex(int u)
    {
        char cmd[1024];
        sprintf(cmd, add_vertex_cmd_format__.c_str(), u, u);
        system(cmd);
        return;
    }
    
    void AddEdge(int u, int v, int node_id = 0)
    {
        char cmd[1024];
        char edge_key[64];
        sprintf(edge_key, "%d_%d", u, v);
        unordered_set<string>::iterator it = edge_set__.find(edge_key);
        
        if (it != edge_set__.end()) {
            // First remove the edge
            sprintf(cmd, rm_edge_cmd_format__.c_str(), u, v); 
            system(cmd);
        }
        else {
            edge_set__.insert(edge_key);
        }

        sprintf(cmd, add_edge_cmd_format__.c_str(), u, v, u, v, weights[node_id]);
        // sprintf(cmd, add_edge_cmd_format__.c_str(), u, v, u, v, 
                // colors[node_id].c_str());
        // cout << cmd << endl;
        system(cmd);
        return;
    }

    unordered_set<int> vertex_set__;
    unordered_set<string> edge_set__;
    string add_vertex_cmd_format__;
    string add_edge_cmd_format__;
    string rm_edge_cmd_format__;
    string colors[5];
    int weights[5];
};

#endif
