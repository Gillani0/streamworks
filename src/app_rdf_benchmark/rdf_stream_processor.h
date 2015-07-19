#ifndef __RDF_STREAM_PROCESSOR_H__
#define __RDF_STREAM_PROCESSOR_H__
#include <boost/unordered_map.hpp>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <sys/time.h>
#include "graph.hpp"
#include "counter.hpp"
#include "utils.h"
#include "property_map.h"
using namespace std;
using namespace boost;

enum RECORD_FORMAT {
    RDF 
};

struct triple_t {
    triple_t(const char* s, const char* p, const char* o): 
            subject(s), predicate(p), object(o) {}
    string subject;
    string predicate;
    string object;
};
ostream& operator<<(ostream& os, const triple_t& triple);

typedef Graph<Label, Timestamp> RDFGraph;

class RDFStreamProcessor {
public:
    RDFStreamProcessor(string data_path, 
            RECORD_FORMAT format, int num_header_lines);
    void Load();
    vector<DirectedEdge<Timestamp> >& GetEdges();
    RDFGraph* InitGraph();
    // RDFGraph* LoadGraph(int num_edges_to_load);
    RDFGraph* LoadGraph(int num_edges_to_load = INT_MAX);
    void SetPropertyPath(string path);
    void CollectPredicateDistribution(string outpath);
/*
    {   
        vector<DirectedEdge<Timestamp> >::iterator e_it;
        int num_edges = 0;
        for (e_it = edges__.begin(); e_it != edges__.end(); e_it++) {
            gSearch__->AddEdge(*e_it);
            num_edges++;
            if (num_edges == 100000) {
                break;
            }   
        }   
        return gSearch__;
    }   
*/

    template <typename EdgeFn>
    uint64_t Run(EdgeFn& edge_functor, string log_path_prefix)
    {
        vector<DirectedEdge<Timestamp> >::iterator e_it;
        int num_edges(0), proc_time(0), num_results(0);
        vector<tuple<int, long, long> > timing_data;
        cout << "Streaming in " << edges__.size() << " edges " << endl;
        uint64_t total_proc_time_us = 0;
        uint64_t total_proc_time_ms = 0;
        // int search_offset = 0; 
        // int begin_search = -1;
        map<string, string> property_map;
        LoadPropertyMap(property_path__.c_str(), property_map);

        string begin_search_str = GetProperty(property_map,
                "search_offset");
        int begin_search = (int)(1000000*atof(begin_search_str.c_str()));
        string edges_in_millions = GetProperty(property_map,
                "num_edges_to_process_M");
        int end_search = begin_search + 
                1000000*atoi(edges_in_millions.c_str());
        cout << "Number of edges to process: " << end_search << endl;
        int REPORTING_INTERVAL = 100000;

        for (e_it = edges__.begin(); e_it != edges__.end(); e_it++) {
            gSearch__->AddEdge(*e_it);
            num_edges++;
            if (num_edges < begin_search) {
                continue;
            }

            struct timeval t1, t2;
            gettimeofday(&t1, NULL);
            num_results += edge_functor(*e_it);
            // cout << "Done processing" << endl;
            gettimeofday(&t2, NULL);

            proc_time = get_tv_diff(t1, t2);
            total_proc_time_us += proc_time;
            if (REPORTING_INTERVAL &&
                        (num_edges % REPORTING_INTERVAL) == 0) {
                cout << "# number of edges processed = " << num_edges << endl;
                // edge_functor.Report(log_path_prefix);
                total_proc_time_ms += (uint64_t) total_proc_time_us*0.001;
                total_proc_time_us = 0;
                // timing_data.push_back(tuple<int, long, long>(num_edges,
                        // total_proc_time_ms, num_results));
                // num_results = 0;
                // proc_time = 0;
            }
            if (num_edges == end_search) {
                break;
            }
        }
        edge_functor.Report(log_path_prefix);
        // Report(timing_data);
        // double tmp = total_proc_time_us;
        total_proc_time_ms += (uint64_t) total_proc_time_us*0.001;
        cout << "INFO Processed edges = " << num_edges << endl;
        cout << "Total_proc_time = " << total_proc_time_ms << endl;
        return total_proc_time_ms;
    }

    void Print();
    void StoreTypeMappings();

protected:
    void Load(string src_path, int num_header_lines);
    void Parse(char* line);
    // int GetVertexType(const char* str);
    // int GetEdgeType(const char* str);
    // uint64_t GetVertexId(const char* str);
    void PrintSummary();
    void Report(vector<tuple<int, long, long> >& timing_data);
private:
    // map<char, int> code__;
    IdGenerator<string> vertex_type_set__;
    IdGenerator<string> vertex_ids__;
    IdGenerator<string> predicates__;
    Counter<string> predicate_counter__;
    //unordered_map<string, uint64_t> vertex_ids__;
    vector<DirectedEdge<Timestamp> > edges__;
    Graph<Label, Timestamp>* gSearch__;
    string graph_path__;
    string property_path__;
};

void N3Loader(string path, vector<triple_t>& triples);
void LoadRDFStream(string path, vector<string>& file_list);

#endif
