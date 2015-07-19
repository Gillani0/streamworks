#include <iostream>
#include <fstream>
#include <string>
#include <set>
#include <vector>
#include "counter.hpp"
#include "rdf_stream_processor.h"
#include "query_planning.hpp"
using namespace std;

bool Label :: FilterVertex(int u, int deg, const Label& other) const
{
    return true;
}

RDFStreamProcessor :: RDFStreamProcessor(string path, 
        RECORD_FORMAT format, int num_header_lines):
        graph_path__(path)
{
    vertex_type_set__.load("test/lsbench_vertex.properties");
    predicates__.load("test/lsbench_edge.properties");
    return;
}

void RDFStreamProcessor :: Print()
{
/*
    cout << "Number of predicates = " << predicates__.size() << endl;
    ofstream ofs("lsbench_predicate_stats.csv", ios_base::out);
    for (Counter<string>::const_iterator it = predicate_counter__.begin();
            it != predicate_counter__.end(); it++) {
        ofs << it->first << "," << it->second << endl;
    }
    ofs.close();
*/
    return;
}

void RDFStreamProcessor :: CollectPredicateDistribution(string outpath)
{
    vector<string> file_list;
    StreamingGraphDistribution edge_distribution_collector;
    LoadRDFStream(graph_path__, file_list);

    uint64_t num_triples = 0;

    for (int i = 0, N = file_list.size(); i < N; i++) {
        vector<triple_t> triples;
        N3Loader(file_list[i], triples);
        for (vector<triple_t>::iterator it = triples.begin();
                it != triples.end(); it++) {
            const triple_t triple = *it;
            int edge_type = predicates__.GetKey(triple.predicate.c_str());
            edge_distribution_collector.Update(edge_type);
            num_triples++;
            if ((num_triples % 1000000) == 0) {
                edge_distribution_collector.TakeSnapshot(num_triples);
            }
        }
        triples.clear();
    }

    ofstream ofs(outpath.c_str(), ios_base::out);
    edge_distribution_collector.Print(ofs);
    ofs.close(); 
    return;
}

vector<DirectedEdge<Timestamp> >& RDFStreamProcessor :: GetEdges()
{
    return edges__;
}

void RDFStreamProcessor :: SetPropertyPath(string path)
{
    property_path__ = path;
    return;
}

void RDFStreamProcessor :: Load()
{
    vector<string> file_list;
    LoadRDFStream(graph_path__, file_list);

    for (int i = 0, N = file_list.size(); i < N; i++) {
        vector<triple_t> triples;
        N3Loader(file_list[i], triples);
        for (vector<triple_t>::iterator it = triples.begin();
                it != triples.end(); it++) {
            const triple_t triple = *it;
            uint64_t subject_id = vertex_ids__.GetKey(triple.subject.c_str());
            int edge_type = predicates__.GetKey(triple.predicate.c_str());
            // predicate_counter__.Add(triple.predicate);
            uint64_t object_id = vertex_ids__.GetKey(triple.object.c_str());
            DirectedEdge<Timestamp> edge;
            edge.s = subject_id;
            edge.t = object_id;
            edge.type = edge_type;
            edge.edge_data.timestamp = 0;
            edges__.push_back(edge);
        }
        triples.clear();
    }
    return;
}

/*
uint64_t RDFStreamProcessor :: GetVertexId(const char* str)
{
    unordered_map<string, uint64_t>::iterator it =
           vertex_ids__.find(str);
    uint64_t id;

    if (it == vertex_ids__.end()) {
        id = vertex_ids__.size();
        vertex_ids__.insert(pair<string, uint64_t>(str, id));
    }
    else {
        id = it->second;
    }
    return id;
}
*/

RDFGraph* RDFStreamProcessor :: InitGraph()
{
    VertexProperty<Label> v_prop;
    IdGenerator<string>::const_iterator v_it;
    gSearch__ = new RDFGraph(vertex_ids__.size());
    vertex_type_set__.GetKey("string");

    for (v_it = vertex_ids__.begin(); 
                v_it != vertex_ids__.end(); v_it++) {
        const string& s = v_it->first;
        int vertex_type;
        string label;

        if (s.find(":") == string::npos) {
            label = s;
            vertex_type = vertex_type_set__.GetKey("string");
        }
        else {
            size_t pos = s.find("xsd:");
            if (pos != string::npos) {
                vertex_type = vertex_type_set__.GetKey(s.substr(pos));
                label = s.substr(0, pos);
            }
            else {
                pos = s.rfind(":");
                vertex_type = vertex_type_set__.GetKey(s.substr(0, pos));
                label = s.substr(pos+1);
            }
        }
        v_prop.vertex_data.label = label;
        v_prop.type = vertex_type;
        gSearch__->AddVertex(v_it->second, v_prop);
    }   

    gSearch__->AllocateAdjacencyLists();
    return gSearch__;
}

void RDFStreamProcessor :: StoreTypeMappings()
{
    vertex_type_set__.save("test/rdf_stream_vertex.properties");
    predicates__.save("test/rdf_stream_edge.properties");
    return;
}

RDFGraph* RDFStreamProcessor :: LoadGraph(int num_edges_to_load) // = INT_MAX)
{   
    vector<DirectedEdge<Timestamp> >::iterator e_it;
    uint64_t num_edges = 0;
    for (e_it = edges__.begin(); e_it != edges__.end(); e_it++) {
        gSearch__->AddEdge(*e_it);
        assert(e_it->type >= 0);
        num_edges++;
        if (num_edges == num_edges_to_load) {
            break;
        }   
    }   
    return gSearch__;
}   


ostream& operator<<(ostream& os, const triple_t& triple)
{
    os << triple.subject << " [" << triple.predicate << "] " 
       << triple.object;
    return os;
}

void N3Loader(string path, vector<triple_t>& triples)
{
    cout << "Loading : " << path << endl;
    int bufsize = 1024*1024;
    ifstream ifs(path.c_str(), ios_base::in);
    if (!ifs.is_open()) {
        cout << "Failed to open: " << path << endl;
    }
    char* buf = new char[bufsize];

    while (ifs.good()) {
        ifs.getline(buf, bufsize);
        if (buf[0] == '\0' || 
                strncmp(buf, "@prefix", 7) == 0) {
            continue;
        }
        char* saveptr;
        char* subject = strtok_r(buf, " ", &saveptr);
        char* predicate = strtok_r(NULL, " ", &saveptr);
        int last_string_len = strlen(saveptr);
        if (saveptr[0] == '\"' && saveptr[last_string_len-3] == '\"') {
            saveptr[last_string_len-3] = '\0';
            saveptr += 1;
        }
        else {
            saveptr[last_string_len-2] = '\0';
        }

        triples.push_back(triple_t(subject, predicate, saveptr));
    }
    ifs.close();
    cout << "# of triples loaded = " << triples.size() << endl;
    delete[] buf;
    return;
}

// void LoadRDFStream(string path, vector<triple_t>& triples, int& num_lines)
void LoadRDFStream(string path, vector<string>& files)
{
    size_t offset = path.size() - 5;
    if (strcmp(path.c_str() + offset, ".list")) {
        // N3Loader(path, triples, num_lines);
        files.push_back(path);
        return;
    }

    cout << "Processing list : " << path << endl;
    ifstream ifs(path.c_str(), ios_base::in);
    char line[1024];
    string filepath;

    while (ifs.good()) {
        ifs.getline(line, 1024);
        int len = strlen(line);
        if (len == 0 || line[0] == '#') {
            continue;
        }
        if (line[len-1] == '\n') {
            filepath = string(line, len-1);
        }   
        else {
            filepath = string(line);
        }   
        // N3Loader(filepath, triples, num_lines);
        // cout << "Number of parsed triples = " << triples.size() << endl;
        files.push_back(filepath);
    } 
    ifs.close();
    return;
}
