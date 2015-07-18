#ifndef __PATH_INDEX_HPP__
#define __PATH_INDEX_HPP__

// #include "counter.hpp"
#include "graph.hpp"
#include <math.h>
#include <assert.h>
#include <map>
using namespace std;

struct edge_t {
    edge_t() {}
    edge_t(bool is_out, int t, int u, int v): 
            outgoing(is_out), type(t), u_t(u), v_t(v) 
    {
/*
        if (t < 0 || t >= 8) {
            cout << "Type = " << t << endl;
            exit(1);
        }
*/
    }

    bool outgoing;
    int  type; 
    int  u_t;
    int  v_t;
    bool operator==(const edge_t& other) const
    {
        if (outgoing == other.outgoing &&
            type == other.type && 
            u_t == other.u_t &&
            v_t == other.v_t) {
            return true;
        }
        else {
            return false;
        }
    }

    uint64_t Hash() 
    {
        char signature[32];
        sprintf(signature, "%d_%d_%d_%d", outgoing, u_t, type, v_t);
        uint64_t h = 5381;
        int len = strlen(signature);
        uint64_t i;
        for (i = 0; i < len; ++i) {
            h = ((h << 5) + h) + (uint64_t)signature[i];
        }    
        return h;
    }

    string String() const
    {
        char signature[32];
        sprintf(signature, "%d_%d_%d_%d", outgoing, u_t, type, v_t);
        return signature;
    }

    bool operator<(const edge_t& other) const
    {
        string s1 = this->String();
        string s2 = other.String();
        return s1 < s2;
/*
        int keys1[4];
        int keys2[4];
        keys1[0] = u_t;
        keys2[0] = other.u_t;
        keys1[1] = v_t;
        keys2[1] = other.v_t;
        keys1[2] = type;
        keys2[2] = other.type;
        keys1[3] = outgoing;
        keys2[3] = other.outgoing;
        int lex_small_key_count = 0;
        for (int i = 0; i < 4; i++) {
            // cout << "k_" << i << " : " << keys1[i] << " and " << keys2[i] << endl;
            if (keys1[i] < keys2[i]) {
                // cout << "lex small" << endl;
                lex_small_key_count++;
            }
            else if (keys1[i] == keys2[i]) {
                // cout << "eq" << endl;
                continue;
            }
            else {
                // cout << "lex large" << endl;
                break;
            }
        }
        return lex_small_key_count;
*/
    }
};

istream& operator>>(istream& is, edge_t& e_t)
{
    is >> e_t.outgoing >> e_t.type >> e_t.u_t >> e_t.v_t;
    return is;
}

ostream& operator<<(ostream& os, const edge_t& e_t)
{
    //os << "{outgoing=" << e_t.outgoing << ",type=" << e_t.type 
       //<< ",u_type=" << e_t.u_t << ",v_type=" << e_t.v_t << "}";
    os << e_t.outgoing << " " << e_t.type << " " << e_t.u_t 
       << " " << e_t.v_t;
    return os;
}

struct index_entry_t {
    index_entry_t() {}
    index_entry_t(edge_t e1, edge_t e2) 
    {
        string s1 = e1.String();
        string s2 = e2.String();
        if (s1 < s2) {
            uv = e1;
            uw = e2;
        }
        else {
            uv = e2;
            uw = e1;
        }
        return;
    }

    edge_t uv;
    edge_t uw;

    uint64_t Hash() 
    {
        char signature[64];
        sprintf(signature, "%d_%d_%d_%d-%d_%d_%d_%d", 
                uv.outgoing, uv.u_t, uv.type, uv.v_t,
                uw.outgoing, uw.u_t, uw.type, uw.v_t);
        uint64_t h = 5381;
        int len = strlen(signature);
        uint64_t i;
        for (i = 0; i < len; ++i) {
            h = ((h << 5) + h) + (uint64_t)signature[i];
        }    
        return h;
    }

    // bool uv_before_vw;
    // float order_timestamp_gap;
    bool operator==(const index_entry_t& other) const
    {
        if (uv == other.uv && uw == other.uw) {
            return true;
        }
        else {
            return false;
        }
    }

    bool operator<(const index_entry_t& other) const
    {
        if (uv < other.uv) {
            return true;
        }
        else if (uv == other.uv) {
            return uw < other.uw;
        }
        else {
            return false;
        }
    }
};

template <class Graph>
Graph* MakeTwigGraph(const index_entry_t& entry)
{
    typedef typename Graph::DirectedEdgeType dir_edge;
    typedef typename Graph::VertexProp vertex_prop;
    Graph* g = new Graph(3);
    vertex_prop v_prop;
    // Adding the root - u
    v_prop.type = entry.uv.u_t;
    g->AddVertex(0, v_prop);
    // Adding left child - v
    v_prop.type = entry.uv.v_t;
    g->AddVertex(1, v_prop);
    // Adding right child - w
    v_prop.type = entry.uw.v_t;
    g->AddVertex(2, v_prop);

    if (entry.uv.outgoing) {
        dir_edge uv(0, 1, entry.uv.type);
        g->AddEdge(uv);
    }
    else {
        dir_edge uv(1, 0, entry.uv.type);
        g->AddEdge(uv);
    }

    if (entry.uw.outgoing) {
        dir_edge uw(0, 2, entry.uw.type);
        g->AddEdge(uw);
    }
    else {
        dir_edge uw(2, 0, entry.uw.type);
        g->AddEdge(uw);
    }
    return g; 
}

istream& operator>>(istream& is, index_entry_t& e)
{
    is >> e.uv >> e.uw;
    return is;
}

ostream& operator<<(ostream& os, const index_entry_t& e)
{
    // os << "[uv=" << e.uv << ", vw=" << e.uw << "]";
    os << e.uv << " " << e.uw;
    return os;
}

template <class Graph>
Graph** SortTwigs(const std::map<index_entry_t, uint64_t>& path_counter, 
        float** path_distribution, int& num_paths)
{
    vector<pair<uint64_t, index_entry_t> > counts;
    num_paths = path_counter.size();
    if (num_paths == 0) {
        return NULL;
    }

    for (std::map<index_entry_t, uint64_t>::const_iterator it = path_counter.begin();
            it != path_counter.end(); it++) {
        counts.push_back(pair<uint64_t, index_entry_t>(it->second, it->first));
    }
    sort(counts.begin(), counts.end());

    Graph** paths = new Graph*[num_paths];
    float* dist = new float[num_paths];
    uint64_t sum = 0;

    for (int i = 0; i < counts.size(); i++) {
        const index_entry_t& e = counts[i].second;
        paths[i] = MakeTwigGraph<Graph>(e);
        // cout << paths[i]->String() << endl;
        sum += counts[i].first;
    }
    for (int i = 0; i < counts.size(); i++) {
        // dist[i] = log((float) counts[i].first/sum);
        dist[i] = ((float) counts[i].first)/sum;
        // cout << "dist[" << i << "] = " << dist[i] << endl;
    }
    *path_distribution = dist;
    cout << "NUMBER OF PATHS = " << num_paths << endl;
    return paths;
}

void StorePathDistribution(const std::map<index_entry_t, uint64_t>& path_counter,
        string outpath)
{
    cout << "Storing path distribution at: " << outpath << endl;
    ofstream ofs(outpath.c_str(), ios_base::out);
    for (std::map<index_entry_t, uint64_t>::const_iterator it = path_counter.begin();
            it != path_counter.end(); it++) {
        ofs << it->first << " " << it->second << endl;
    }
    ofs.close();
    return;
}

template <typename Graph>
Graph** LoadPathDistribution(string path, float** path_distribution,
        int& num_paths)
{
    cout << "Loading path distribution from: " << path << endl;
    ifstream ifs(path.c_str(), ios_base::in);
    std::map<index_entry_t, uint64_t> path_counter;
    char buf[1024];
    index_entry_t entry;
    uint64_t count;
    while (ifs.good()) {
        buf[0] = '\0';
        ifs.getline(buf, 1024);
        if (strlen(buf) <= 0) {
            continue;
        } 
        stringstream strm(buf);
        strm >> entry >> count;
        path_counter[entry] = count; 
    }
    ifs.close();
    return SortTwigs<Graph>(path_counter, path_distribution, num_paths);
}

template <typename Graph>
Graph** IndexPaths(const Graph* g, float** path_distribution, 
        int& num_paths, string outpath = "")
{
    cout << "Computing path distribution ..." << endl;
    typedef typename Graph::EdgeType edge;
    std::map<index_entry_t, uint64_t> path_counter;
    int MAX_PATHS = 4096;
    index_entry_t* path_types = new index_entry_t[MAX_PATHS];
    int64_t* path_counts = new int64_t[MAX_PATHS];
    for (int i = 0; i < MAX_PATHS; i++) {
        path_counts[i] = -1;
    }
    // set<edge_t> all_edge_types;
    uint64_t edge_count = 0;
    int MAX_EDGE_TYPES = 1024;
    int64_t* edge_type_counts = new int64_t[MAX_EDGE_TYPES];
    edge_t* edge_types = new edge_t[MAX_EDGE_TYPES];
    pair<edge_t, uint64_t>* per_node_edge_stats = new pair<edge_t, uint64_t>[MAX_EDGE_TYPES];
    int per_node_count, offset;

    cout << "INFO IndexPaths: MAX_EDGE_TYPES = " << MAX_EDGE_TYPES << endl;
    cout << "Iterating over " << g->NumVertices() << " nodes" << endl;
    cout << "Iterating over " << g->NumEdges() << " edges" << endl;

    for (int i = 0, N = g->NumVertices(); i < N; i++) {
        int neighbor_count;
        const edge* neighbors = g->Neighbors(i, neighbor_count);
        // std::map<edge_t, uint64_t> edge_types;
        for (int j = 0; j < MAX_EDGE_TYPES; j++) {
            edge_type_counts[j] = -1;
        }
        int u_t = g->GetVertexProperty(i).type;
        bool debug = false;
        
        for (int j = 0; j < neighbor_count; j++) {
            edge_count++;
            int v_t = g->GetVertexProperty(neighbors[j].dst).type;
            // assert(neighbors[j].type >= 0);
            // assert(neighbors[j].type < 8);
            edge_t e_t(neighbors[j].outgoing, neighbors[j].type, u_t, v_t);
            // all_edge_types.insert(e_t);
            /*
            std::map<edge_t, uint64_t>::iterator it = edge_types.find(e_t);
            if (it == edge_types.end()) {
                edge_types[e_t] = 1;
            }
            else {
                it->second += 1;
            }
            */
            offset = e_t.Hash() % MAX_EDGE_TYPES;
            edge_type_counts[offset] += 1;
            edge_types[offset] = e_t;
        }

        per_node_count = 0;
        for (int j = 0; j < MAX_EDGE_TYPES; j++) {
            if (edge_type_counts[j] >= 0) {
                per_node_edge_stats[per_node_count++] = 
                        pair<edge_t, uint64_t>(edge_types[j], edge_type_counts[j] + 1);
            }
        }
        sort(per_node_edge_stats, per_node_edge_stats + per_node_count);

        // for (map<edge_t, uint64_t>::const_iterator it1 = edge_types.begin();
                // it1 != edge_types.end(); it1++) {
        for (pair<edge_t, uint64_t>* it1 = per_node_edge_stats;
                it1 != (per_node_edge_stats + per_node_count); it1++) {
            index_entry_t key1(it1->first, it1->first);
            uint64_t value1 = it1->second*(it1->second-1)/2;
            std::map<index_entry_t, uint64_t>::iterator it = path_counter.find(key1);
            offset = key1.Hash() % MAX_PATHS;
            path_counts[offset] += value1;
            path_types[offset] = key1;
/*
            if (it == path_counter.end()) {
                path_counter[key1] = value1;
            }
            else {
                it->second += value1;
            }
*/
            // for (map<edge_t, uint64_t>::const_iterator it2 = edge_types.begin();
                // it2 != edge_types.end(); it2++) {
            for (pair<edge_t, uint64_t>* it2 = per_node_edge_stats;
                it2 != (per_node_edge_stats + per_node_count); it2++) {
                if (it1->first < it2->first) {
                    index_entry_t key2(it1->first, it2->first);
                    uint64_t value2 = it1->second*it2->second;
                    offset = key2.Hash() % MAX_PATHS; 
                    path_counts[offset] += value2;  
                    path_types[offset] = key2;
/*
                    it = path_counter.find(key2);
                    if (it == path_counter.end()) {
                        path_counter[key2] = value2;
                    }
                    else {
                        it->second += value2;
                    }
*/
                }
            }
        } 

    }

    delete[] per_node_edge_stats;
    delete[] edge_types;
    delete[] edge_type_counts;
    for (int i = 0; i < MAX_PATHS; i++) {
        if (path_counts[i] >= 0) {
            path_counter[path_types[i]] = path_counts[i];
        }
    }

    if (outpath.size() > 0) {
        StorePathDistribution(path_counter, outpath);
    }
    return SortTwigs<Graph>(path_counter, path_distribution, num_paths);
}

#endif
