#ifndef __NETFLOW_PROCESSOR_H__
#define __NETFLOW_PROCESSOR_H__

#include <string>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include <map>
#include <vector>
#include "graph.hpp"
#include "utils.h"
#include "property_map.h"
#include "netflow_parser.h"
// #include "edge_distribution.h"
#include <functional>
#include <boost/tuple/tuple.hpp>
using namespace std;
using namespace boost;

#define UDP_TRAFFIC 0
#define TCP_TRAFFIC 1
#define ICMP_TRAFFIC 2
#define UNKNOWN_TRAFFIC 2

// typedef Graph<Label,NetflowAttbs> StreamG;

struct test_conf_t {
    int frequent_subgraph_support;
    int frequent_vertex_degree;
};

class NetflowProcessor {
public:
    NetflowProcessor(string graph_path, RECORD_FORMAT format, 
            int num_header_lines); 

    int LineCount();
    void PrintSummary();
    void Load();
    StreamG* InitGraph();

    void SetPropertyPath(string path);

    StreamG* LoadGraph(uint64_t num_edges_to_load = 0)
    {
        vector<DirectedEdge<NetflowAttbs> >::iterator e_it;
        uint64_t count = 0;
        uint64_t scanned_count = 0;
        unordered_set<string> edge_set;

        for (e_it = edges__.begin(); e_it != edges__.end(); e_it++) {
            scanned_count++;
            if (num_edges_to_load && count > num_edges_to_load) {
                break;
            }
            if (SeenEdge(edge_set, *e_it)) {
                continue;
            }
            if (e_it->type < 0) {
                cout << "BAD: " << e_it->s << "," << e_it->type << "," << e_it->t << endl;
                exit(1);
            }
            gSearch__->AddEdge(*e_it);
            count++;
        }
        cout << "Added " << count << " edges from " << scanned_count 
             << " scanned edges." << endl;
        return gSearch__;
    }

    bool SeenEdge(unordered_set<string>& edge_set, DirectedEdge<NetflowAttbs>& e)
    {
        string s1 = GetLabel(gSearch__, e.s);
        string s2 = GetLabel(gSearch__, e.s);
        if (s1.find("10.", 0) != string::npos || 
                s1.find("192.168", 0) != string::npos) {
            return true;
        }
        if (s2.find("10.", 0) != string::npos || 
                s2.find("192.168", 0) != string::npos) {
            return true;
        }
        // return false;
        stringstream strm;
        strm << e.s << "-" << e.type << "-" << e.t;
        string s = strm.str();
        unordered_set<string>::iterator it = edge_set.find(s);
        if (it == edge_set.end()) {
            edge_set.insert(s);
            return false;
        }
        else {
            return true;
        }
    }

    template <typename EdgeFn>
    uint64_t Run(EdgeFn& edge_functor, string log_path_prefix)
    {   
        vector<DirectedEdge<NetflowAttbs> >::iterator e_it;
        int num_edges(0), proc_time(0), num_results(0);
        vector<boost::tuple<int, long, long> > timing_data;
        struct timeval t1, t2;
   
        uint64_t total_proc_time_ms = 0;
        uint64_t total_proc_time_us = 0;
        unordered_set<string> edge_set;
        uint64_t num_scanned_edges = 0;
        // int begin_search = -1;
        map<string, string> property_map;
        LoadPropertyMap(property_path__.c_str(), property_map);
        string begin_search_str = GetProperty(property_map,
                "search_offset");
        int begin_search = (int)(1000000*atof(begin_search_str.c_str()));
        string edges_in_millions = GetProperty(property_map,
                "num_edges_to_process_M");
        int num_edges_to_process = (int)(1000000*atof(edges_in_millions.c_str()));
        int end_search = begin_search + num_edges_to_process;
                // (int)(1000000*atof(edges_in_millions.c_str()));

        cout << "INFO Number of edges to process: " << end_search << endl;
        cout << "INFO Begin search = " << begin_search << endl;
        cout << "INFO End search = " << end_search << endl;
        int REPORTING_INTERVAL = 100000;


        for (e_it = edges__.begin(); e_it != edges__.end(); e_it++) {
            num_scanned_edges++;
            DirectedEdge<NetflowAttbs> e = *e_it;
            if (num_scanned_edges < begin_search) {
                continue;
            }
            // Changing the check from num_scanned_edges to num_edges
            /// if (num_scanned_edges == end_search) {
            if (num_edges == num_edges_to_process) {
                break;
            }

            if (SeenEdge(edge_set, e)) {
                continue;
            }
            gSearch__->AddEdge(e);
            num_edges++;

            gettimeofday(&t1, NULL);
            num_results += edge_functor(e);
            gettimeofday(&t2, NULL);
            proc_time = get_tv_diff(t1, t2);
            total_proc_time_us += proc_time;

            if (REPORTING_INTERVAL && ((num_scanned_edges % REPORTING_INTERVAL) == 0)) {
                cout << "INFO NetflowProcessor: Processed " <<  num_edges  << ","
                        << (uint64_t)(0.001*total_proc_time_us) << endl;
                cout << "INFO Scanned edge count = " << num_scanned_edges << endl;
                // timing_data.push_back(boost::tuple<int, long, long>(num_edges, 
                        // (long)(0.001*total_proc_time_us), num_results));
                total_proc_time_ms += (uint64_t) total_proc_time_us*0.001;
                total_proc_time_us = 0;
                // edge_functor.Report(log_path_prefix);
                // proc_time = 0;
                // num_results = 0;
                // break;
            }   
        }   

        // cout << "INFO Scanned flow records = " << num_scanned_edges << endl;
        edge_functor.Report(log_path_prefix);
        total_proc_time_ms += (uint64_t) total_proc_time_us*0.001;
        cout << "INFO Processed edges = " << num_edges << endl;
        cout << "Total_proc_time = " << total_proc_time_ms << endl;
        // Report(timing_data);
        return total_proc_time_ms;
    }   

    string logpath__;
protected:
    // void bad_but_still_better_strptime(const char* line, struct tm& tdata);
    void Load(string src_path, int num_header_lines);
    void FilterAddress(char* addr);
    uint64_t GetAddressId(const char* addr);
    // uint64_t GetProtocolType(const char* protocol);
    void ParsePcap(char* line);
    void ParseNetflow(char* line);
    void GetVertexProperties(vector<VertexProperty<Label> >& vertex_properties);
    void GetEdges(vector<DirectedEdge<NetflowAttbs> >& edge_list);
    void Report(vector<boost::tuple<int, long, long> >& timing_data);

private:
    int udp_count__;
    int icmp_count__;
    int tcp_count__;
    int unknown_count__;
    int num_lines__;
    // char timestamp_format__[32];
    unordered_map<string, uint64_t> ip_ids__;
    // map<string, int> protocol_types__;
    vector<DirectedEdge<NetflowAttbs> > edges__;
    string graph_path__;
    int num_header_lines__;
    RECORD_FORMAT in_format__;
    Graph<Label, NetflowAttbs>* gSearch__;
    LineParser parser__;
    string property_path__;
};


#endif
