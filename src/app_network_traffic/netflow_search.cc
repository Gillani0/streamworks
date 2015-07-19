/*
#include <stdlib.h>
#include <string>
#include "sj_tree_executor.hpp"
#include "query_parser.hpp"
#include <assert.h>
#include "property_map.h"
#include "path_index.h"
#include "query_planning.hpp"
#include <limits.h>
*/
#include "netflow_processor.h"
#include "dynamic_graph_search.hpp"
// #include "incr_pattern_discovery.hpp"
using namespace std;

/*
map<string, string> gPropertyMap;

string GetLogPathPrefix(string query_path)
{
    size_t pos = query_path.find_last_of("/");

    string dirpath, query_type;
    if (pos == string::npos) { 
        dirpath = ".";
    }
    else {
        dirpath = query_path.substr(0, pos);
    }
    
    char timestamp[64];
    time_t now = time(NULL);
    struct tm* timeinfo = localtime(&now);
    // strftime(buf, 1024, "cost_runtime_%F_%T.log", timeinfo);
    strftime(timestamp, 1024, "%F", timeinfo);

    if (pos == string::npos) {
        size_t pos1 = query_path.find("_");
        query_type = query_path.substr(0, pos1);
    }
    else {
        size_t pos1 = query_path.find("_", pos);
        query_type = query_path.substr(pos+1, pos1-pos-1);
    }

    string query_algo = GetProperty(gPropertyMap, "query_algo");
    string win_size = GetProperty(gPropertyMap, "num_edges_to_process_M");

    char buf[1024];
    sprintf(buf, "%s/%s_%s_%s_win%sM", dirpath.c_str(), 
            timestamp, query_algo.c_str(), query_type.c_str(),
            win_size.c_str());
    return string(buf);
}

string GetRuntimeLogPath(string query_path)
{
    string prefix = GetLogPathPrefix(query_path);
    char buf[1024];
    sprintf(buf, "%s_cost_runtime.csv", prefix.c_str());
    return string(buf);
}
 
void StoreRelativeSelectivity(float relative_selectivity)
{
    string rel_selectivity_path = GetProperty(gPropertyMap, "rel_selectivity_path");
    cout << "Saving selectivity at: "<< rel_selectivity_path << endl;
    string query_plan = GetProperty(gPropertyMap, "query_plan");

    ofstream ofs(rel_selectivity_path.c_str(), ios_base::app);
    ofs << query_plan << " " << relative_selectivity << endl;
    ofs.close();    
    return;
}

float GetRelativeSelectivity()
{
    string query_plan = GetProperty(gPropertyMap, "query_plan");
    string rel_selectivity_path = GetProperty(gPropertyMap, "rel_selectivity_path");

    cout << "Loading selectivity from: " << rel_selectivity_path << endl;
    map<string, string> s_map;
    LoadPropertyMap(rel_selectivity_path.c_str(), s_map);

    string rel_selectivity_str = GetProperty(s_map, query_plan);
    char* dummy;
    float relative_selectivity = strtof(rel_selectivity_str.c_str(),
            &dummy);
    return relative_selectivity;
}

void Usage(char* argv[])
{
    cout << "Usage: " << argv[0] << " [options ...]"  << endl;
    cout << "Required options:" << endl;
    cout << "  Properties file with data and query information" << endl;
    cout << "Optional:" << endl;
    cout << "Streaming algorithm name:" << endl;
    cout << "  SJTree   SJ-Tree based search" << endl;  
    cout << "  Count    Counting search" << endl;
    cout << "  None     Just stream in the edge" << endl;
    return;
}

void CollectSubgraphStatistics(NetflowProcessor* netflow_processor)
{
    string path_distribution_data = GetProperty(gPropertyMap,
            "path_distribution_data");
    string edge_distribution_data = GetProperty(gPropertyMap,
            "edge_distribution_data");

    bool collect_path_distribution = !Exists(path_distribution_data);
    bool collect_edge_distribution = !Exists(edge_distribution_data);

    if (!collect_path_distribution && !collect_edge_distribution) {
        return;
    }
    
    struct timeval t1, t2;
    cout << "INFO CollectSubgraphStatistics: Initializing graph ..." << endl;
    StreamG* gSearch = netflow_processor->InitGraph();
    // uint64_t num_edges_for_estimation = 1000000;
    netflow_processor->LoadGraph();   

    if (collect_path_distribution) {
        gettimeofday(&t1, NULL);
        float* path_distribution;
        int num_paths;
        StreamG** paths = IndexPaths(gSearch, &path_distribution, num_paths,
                path_distribution_data);
        gettimeofday(&t2, NULL);
        delete[] path_distribution;
        for (int i = 0; i < num_paths; i++) {
            delete paths[i];
        }
        delete[] paths;
        cout << "IndexPaths : " << get_tv_diff(t1, t2) <<  " usecs" << endl;
    }

    if (collect_edge_distribution) {
        gettimeofday(&t1, NULL);
        map<int, float> edge_dist = GetEdgeDistribution(gSearch);
        gettimeofday(&t2, NULL);
        cout << "Computed edge distribution : " << get_tv_diff(t1, t2) <<  " usecs" << endl;
        string edge_distribution_data = GetProperty(gPropertyMap,
                "edge_distribution_data");
        StoreEdgeDistribution(edge_dist, edge_distribution_data);
    }
    
    delete gSearch;
    return;
}

void CollectSubgraphStatistics(string data_path, RECORD_FORMAT input_format,
        int num_header_lines)
{
    string path_distribution_data = GetProperty(gPropertyMap, 
            "path_distribution_data");
    string edge_distribution_data = GetProperty(gPropertyMap, 
            "edge_distribution_data");

    bool collect_path_distribution = !Exists(path_distribution_data);
    bool collect_edge_distribution = !Exists(edge_distribution_data);

    if (!collect_path_distribution && !collect_edge_distribution) {
        return;
    }
    
    struct timeval t1, t2;
    NetflowProcessor netflow_processor(data_path, input_format, num_header_lines);
    cout << "Loading raw data ..." << endl;
    netflow_processor.Load();
    cout << "Initializing graph ..." << endl;
    StreamG* gSearch = netflow_processor.InitGraph();
    // uint64_t num_edges_for_estimation = 1000000;
    netflow_processor.LoadGraph();   

    if (collect_path_distribution) {
        gettimeofday(&t1, NULL);
        float* path_distribution;
        int num_paths;
        StreamG** paths = IndexPaths(gSearch, &path_distribution, num_paths,
                path_distribution_data);
        gettimeofday(&t2, NULL);
        cout << "IndexPaths : " << get_tv_diff(t1, t2) <<  " usecs" << endl;
    }

    if (collect_edge_distribution) {
        gettimeofday(&t1, NULL);
        map<int, float> edge_dist = GetEdgeDistribution(gSearch);
        gettimeofday(&t2, NULL);
        cout << "Computed edge distribution : " << get_tv_diff(t1, t2) <<  " usecs" << endl;
        string edge_distribution_data = GetProperty(gPropertyMap,
                "edge_distribution_data");
        StoreEdgeDistribution(edge_dist, edge_distribution_data);
    }
    
    delete gSearch;
    return;
}

float GenerateQueryPlan(StreamG* gQuery)
{
    // Load Statistics
    string path_distribution_data = GetProperty(gPropertyMap,
            "path_distribution_data");
    int num_paths;
    float* path_distribution;
    StreamG** paths = LoadPathDistribution<StreamG>(path_distribution_data,
            &path_distribution, num_paths);

    string edge_distribution_data = GetProperty(gPropertyMap,
            "edge_distribution_data");
    map<int, float> edge_distribution;
    LoadEdgeDistribution(edge_distribution_data, edge_distribution);
    
    // Run Decomposition
    string query_plan = GetProperty(gPropertyMap, "query_plan");
    bool decompose_paths = 
            (bool) atoi(GetProperty(gPropertyMap, "decompose_paths").c_str());
    if (decompose_paths) {
        cout << "INFO Performing path based decomposition." << endl; 
    }
    else {
        cout << "INFO Performing edge based decomposition." << endl; 
    }

    float sj_tree_cost;
    JoinTreeDAO* join_tree = DecomposeQuery(gQuery, edge_distribution,
            paths, path_distribution, num_paths, &sj_tree_cost, decompose_paths);

    cout << "Storing join tree at : " << query_plan << endl;
    if (join_tree->GetLeafCount() == gQuery->NumEdges()) {
        cout << "WARNING !!!" << endl;
        cout << "Query decomposed into 1-edge subgraphs" << endl;
    }
    join_tree->Store(query_plan.c_str());
    cout << "Query plan saved. " << endl;
    // Cleanup
    for (int i = 0; i < num_paths; i++) {
        // cout << "   ... deleting path[" << i << "]" << endl;
        delete paths[i];
    }
    // cout << "   ... delete[] paths" << endl;
    delete[] paths;
    // cout << "   ... delete[] path_distribution" << endl;
    delete[] path_distribution;
    // cout << "   ... delete join_tree" << endl;
    delete join_tree;
    cout << "Completed memory cleanup ..." << endl;

    if (decompose_paths) {
        // Compute Relative Selectivity
        cout << "Computing single edge distribution cost ...." << endl;
        float single_edge_decomposition_cost = 
                GetSingleEdgeDecompositionCost(gQuery, edge_distribution);
        float relative_selectivity = sj_tree_cost/single_edge_decomposition_cost;
        string selectivity_path = GetProperty(gPropertyMap, "selectivity_path");
        ofstream logger(selectivity_path.c_str(), ios_base::app);
        logger << single_edge_decomposition_cost << "," << sj_tree_cost << endl;
        logger.close();
        return relative_selectivity;
    }
    else {
        return sj_tree_cost;
    }
}

uint64_t RunSearch(NetflowProcessor* netflow_processor, 
        StreamG* gQuery, const SearchContext& context,
        string query_plan)
{
    cout << "++++++ Running search ...." << endl; 
    StreamG* gSearch = netflow_processor->InitGraph();

    SJTreeExecutor<StreamG> graph_search(gSearch, gQuery, 
            context, query_plan);
    string log_path_prefix = GetLogPathPrefix(query_plan);
    
    cout << "Starting incremental search ..." << endl;
    cout << "LOG PREFIX: " << log_path_prefix << endl;
    uint64_t run_time = netflow_processor->Run(graph_search, 
            log_path_prefix);
    cout << "Cleaning up temporary data graph ..." << endl;
    delete gSearch;
    cout << "Done." << endl;
    return run_time;
}

uint64_t RunSearch(StreamG* gQuery, const SearchContext& context, 
        string data_path, 
        RECORD_FORMAT input_format,
        int num_header_lines, string query_plan)
{
    NetflowProcessor netflow_processor(data_path, input_format, num_header_lines);
    netflow_processor.Load();
    StreamG* gSearch = netflow_processor.InitGraph();

    SJTreeExecutor<StreamG> graph_search(gSearch, gQuery, 
            context, query_plan);
    string log_path_prefix = GetLogPathPrefix(query_plan);
    
    cout << "Starting incremental search ..." << endl;
    uint64_t run_time = netflow_processor.Run(graph_search,
            log_path_prefix);
    return run_time;
}

NetflowProcessor* InitNetflowProcessor(string property_path)
{
    LoadPropertyMap(property_path.c_str(), gPropertyMap);
    string data_path = gPropertyMap["data_path"];
    RECORD_FORMAT input_format = 
            (RECORD_FORMAT) atoi(GetProperty(gPropertyMap, "input_format").c_str());
    int num_header_lines = atoi(GetProperty(gPropertyMap, "num_header_lines").c_str());; 
    NetflowProcessor* netflow_processor = 
            new NetflowProcessor(data_path, input_format, num_header_lines);
    netflow_processor->Load();
    return netflow_processor;
}

int NetflowSearchRunner(NetflowProcessor* netflow_processor, 
        string property_path, string query_path)
{
    string query_plan;
    bool decompose_paths = 
            (bool) atoi(GetProperty(gPropertyMap, "decompose_paths").c_str());

    string suffix = decompose_paths ? ".2" : ".1";
    query_plan = query_path + ".plan" + suffix;
    gPropertyMap["query_plan"] = query_plan;

    string vertex_property_path = gPropertyMap["vertex_property_path"];
    string edge_property_path = gPropertyMap["edge_property_path"];
    RECORD_FORMAT input_format = 
            (RECORD_FORMAT) atoi(GetProperty(gPropertyMap, "input_format").c_str());
    int num_header_lines = atoi(GetProperty(gPropertyMap, "num_header_lines").c_str());; 

    cout << "Query : " << query_path << endl;
    cout << "Vertex properties : " << vertex_property_path << endl;
    cout << "Edge properties : " << edge_property_path << endl;

    QueryParser<Label, NetflowAttbs> query_loader;
    StreamG* gQuery = query_loader.ParseQuery(query_path.c_str(),
            vertex_property_path.c_str(),
            edge_property_path.c_str());
    SearchContext context = query_loader.search_context;
    assert(gQuery->NumEdges() != 0);

    CollectSubgraphStatistics(netflow_processor);

    float relative_selectivity = 0;
    if (!Exists(query_plan)) {
        cout << "Generating query plan ..." << endl;
        relative_selectivity = GenerateQueryPlan(gQuery);
        if (decompose_paths) {
            StoreRelativeSelectivity(relative_selectivity);
        }
    }
    else {
        if (decompose_paths) {
            relative_selectivity = GetRelativeSelectivity();
        }
        else {
            relative_selectivity = 1;
        }
        cout << "Relative selectivity: " << relative_selectivity << endl;
    }

    string run_search = GetProperty(gPropertyMap, "run_search");
    if (run_search == "yes") {
        uint64_t run_time = RunSearch(netflow_processor, gQuery, context,
                query_plan);
    
        if (relative_selectivity) {
            string logpath = GetRuntimeLogPath(query_path);
            ofstream logger(logpath.c_str(), ios_base::app);
            logger << relative_selectivity << "," << run_time << endl;
            logger.close();
            ofstream logger1("test/search.log", ios_base::app);
            logger1 << query_path << "," << relative_selectivity 
                    << "," << run_time << endl;
            logger1.close();
        }
    }
    delete gQuery;
    return 0;
}

int NetflowSearchRunner(string property_path, string query_path)
{
    LoadPropertyMap(property_path.c_str(), gPropertyMap);
    string data_path = gPropertyMap["data_path"];
    string query_plan;

    bool decompose_paths = 
            (bool) atoi(GetProperty(gPropertyMap, "decompose_paths").c_str());
    string suffix = decompose_paths ? ".2" : ".1";
    query_plan = query_path + ".plan" + suffix;
    gPropertyMap["query_plan"] = query_plan;

    string vertex_property_path = gPropertyMap["vertex_property_path"];
    string edge_property_path = gPropertyMap["edge_property_path"];
    RECORD_FORMAT input_format = 
            (RECORD_FORMAT) atoi(GetProperty(gPropertyMap, "input_format").c_str());
    int num_header_lines = atoi(GetProperty(gPropertyMap, "num_header_lines").c_str());; 

    cout << "Query : " << query_path << endl;
    cout << "Vertex properties : " << vertex_property_path << endl;
    cout << "Edge properties : " << edge_property_path << endl;

    QueryParser<Label, NetflowAttbs> query_loader;
    StreamG* gQuery = query_loader.ParseQuery(query_path.c_str(),
            vertex_property_path.c_str(),
            edge_property_path.c_str());
    SearchContext context = query_loader.search_context;
    assert(gQuery->NumEdges() != 0);

    CollectSubgraphStatistics(data_path, input_format, num_header_lines); 

    float relative_selectivity = 0;
    if (!Exists(query_plan)) {
        cout << "Generating query plan ..." << endl;
        relative_selectivity = GenerateQueryPlan(gQuery);
        bool decompose_paths = 
                (bool) atoi(GetProperty(gPropertyMap, "decompose_paths").c_str());
        if (decompose_paths) {
            StoreRelativeSelectivity(relative_selectivity);
        }
    }
    else {
        relative_selectivity = GetRelativeSelectivity();
        cout << "Relative selectivity: " << relative_selectivity << endl;
    }

    string run_search = GetProperty(gPropertyMap, "run_search");
    if (run_search == "yes") {
        uint64_t run_time = RunSearch(gQuery, context, data_path, input_format, 
                num_header_lines, query_plan);
     
        if (relative_selectivity) {
            string logpath = GetRuntimeLogPath(query_path);
            ofstream logger(logpath.c_str(), ios_base::app);
             logger << relative_selectivity << "," << run_time << endl;
            logger.close();
        }
    }
    delete gQuery;
    return 0;
}

void NetflowMinerRunner(int argc, char* argv[])
{
    LoadPropertyMap("test/netflow.properties", gPropertyMap);
    string data_path = gPropertyMap["data_path"];
    RECORD_FORMAT input_format = 
            (RECORD_FORMAT) atoi(GetProperty(gPropertyMap, "input_format").c_str());
    int num_header_lines = atoi(GetProperty(gPropertyMap, "num_header_lines").c_str());; 
    NetflowProcessor netflow_processor(data_path, input_format, num_header_lines);
    cout << "Loading raw data ..." << endl;
    netflow_processor.Load();
    cout << "Initializing graph ..." << endl;
    StreamG* gSearch = netflow_processor.InitGraph();
    netflow_processor.LoadGraph();   

    int frequent_subgraph_support = atoi(argv[1]);
    int frequent_vertex_degree = atoi(argv[2]);

    IncrFreqGraphMiner<Label, NetflowAttbs> graph_miner(gSearch,
            frequent_subgraph_support, frequent_vertex_degree);
    cout << "Running graph miner ..." << endl;
    uint64_t total_proc_time_ms = netflow_processor.Run(graph_miner, "graph_miner");
    graph_miner.Report("graph_miner");
    char path[256];
    sprintf(path, "test/results/support_runtime_deg_%d.csv", 
            frequent_vertex_degree);
    ofstream ofs(path, ios_base::app);
    ofs << frequent_subgraph_support << " " << frequent_vertex_degree 
        << total_proc_time_ms << endl;
    ofs.close();
    return;
}

void NetflowExportRunner(int argc, char* argv[])
{
    map<string, string> p_map;
    if (argc == 1) {
        cout << "USAGE: " << argv[0] << " export.properties" << endl;
        exit(1);
    }
    LoadPropertyMap(argv[1], gPropertyMap);
    RECORD_FORMAT input_format = 
            (RECORD_FORMAT) atoi(GetProperty(gPropertyMap, "input_format").c_str());
    OUTPUT_FORMAT output_format = 
            (OUTPUT_FORMAT) atoi(GetProperty(gPropertyMap, "output_format").c_str());
    bool is_sliding_output = atoi(GetProperty(gPropertyMap, "is_sliding_output").c_str());
    string in_path = GetProperty(gPropertyMap, "in_path");
    string out_dir = GetProperty(gPropertyMap, "out_dir");
    string prefix = GetProperty(gPropertyMap, "prefix");
    int window_len = atoi(GetProperty(gPropertyMap, "window_len").c_str());

    FindClustersOverTime(in_path.c_str(), input_format, out_dir.c_str(), prefix.c_str(),
            output_format, window_len, is_sliding_output);
    // BuildClusteringInput(in_path.c_str(), input_format, 
            // "/pic/projects/mnms4graphs/nccdc/2013/clustering/nccdc2013_cluster_input_id.csv");
    return;
}

int main(int argc, char* argv[])
{
    if (argv[2] != NULL && strstr(argv[2], ".list")) {
        string property_path = argv[1];
        cout << "Parsing file list: " << argv[2] << endl;
        ifstream ifs(argv[2], ios_base::in);
        NetflowProcessor* netflow_processor = InitNetflowProcessor(property_path);
        string line;
        vector<string> file_list;
        while (ifs.good()) {
            getline(ifs, line);
            if (line.size()) {
                file_list.push_back(line);
            }
        }
        ifs.close();
        for (int i = 0, N = file_list.size(); i < N; i++) {
            NetflowSearchRunner(netflow_processor, property_path, 
                    file_list[i]);
        }
        cout << "Cleaning up netflow processor ..." << endl;
        delete netflow_processor;
    }
    else {
        NetflowSearchRunner(argv[1], argv[2]);
    }
    // NetflowSearchRunner(argc, argv);
    // NetflowExportRunner(argc, argv);
    // NetflowMinerRunner(argc, argv);
    return 0;
}
*/
/*
bool Label::FilterVertex(int u, int u_deg, const Label& lbl) const
{
    return true;
}

bool NetflowAttbs::FilterEdge(int e_id) const
{
    return true;
}
*/

int main(int argc, char* argv[])
{
    dynamic_graph_search_main<StreamG, NetflowProcessor>(argc, argv);
}
