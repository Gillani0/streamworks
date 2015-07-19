// #include "rdf_stream_processor.h"
#include "livejournal_stream_processor.h"
#include "dynamic_graph_search.hpp"
#include "giraph_exporter.hpp"
/*
#include "path_index.h"
#include "property_map.h"
#include "graph.hpp"
#include "query_parser.hpp"
#include "query_planning.hpp"
#include "sj_tree_executor.hpp"
*/
using namespace std;

/*
void RunRDFStreamSearch(int argc, char* argv[])
{
    cout << "Loading query configuration ..." << endl;
    map<string, string> property_map;
    LoadPropertyMap("test/lsbench.properties", property_map);
    string data_path = property_map["data_path"];

    string query_path, query_plan;
    if (argv[1] != NULL) {
        query_path = argv[1];
        query_plan = query_path + ".plan";
    }   
    else if (argv[1] != NULL && argv[2] != NULL) {
        query_path = argv[1];
        query_plan = argv[2];
    }
    else {
        query_path = property_map["query_path"];
        query_plan = property_map["query_plan"];
    }   
    string vertex_property_path = property_map["vertex_property_path"];
    string edge_property_path = property_map["edge_property_path"];

    QueryParser<Label, Timestamp> query_loader;
    cout << "Parsing query from=" << query_path << endl;
    cout << "vertex_property_path=" << vertex_property_path << endl;
    cout << "edge_property_path=" << edge_property_path << endl;
    RDFGraph* gQuery = query_loader.ParseQuery(query_path.c_str(),
            vertex_property_path.c_str(),
            edge_property_path.c_str());
    SearchContext search_context = query_loader.search_context;
    assert(gQuery);
    assert(gQuery->NumEdges() != 0); 
    cout << "#vertices in gQuery = " << gQuery->NumVertices() << endl;
    cout << "#edges in gQuery = " << gQuery->NumEdges() << endl;

    RDFStreamProcessor rdf_stream_processor(data_path.c_str());
    cout << "Loading graph data ..." << endl;
    rdf_stream_processor.Load();
    cout << "Initializing graph ..." << endl;
    RDFGraph* gSearch = rdf_stream_processor.InitGraph();
    assert(gSearch);

    string path_distribution_data = property_map["path_distribution_data"];

    if (Exists(path_distribution_data) == false) {
        cout << "Computing path distribution ..." << endl;
        time_t t1 = time(NULL);
        float* path_distribution;
        int num_paths;
        rdf_stream_processor.LoadGraph(); // for IndexPaths
        RDFGraph** paths = IndexPaths(gSearch, &path_distribution,
                num_paths, path_distribution_data);
        for (int i = 0; i < num_paths; i++) {
            delete paths[i];
        }
        delete[] paths;
        time_t t2 = time(NULL);
        cout << "Time = " << (t2 - t1) << " seconds" << endl;
        cout << "Computing edge distribution ..." << endl;
        t1 = time(NULL);
        map<int, int> edge_dist = GetEdgeDistribution(gSearch);
        string edge_distribution_data = GetProperty(property_map,
                "edge_distribution_data");
        StoreEdgeDistribution(edge_dist, edge_distribution_data);
        t2 = time(NULL);
        cout << "Time = " << (t2 - t1) << " seconds" << endl;
    }
    else { 
        if (Exists(query_plan) == false) {
            cout << "Generating query plan: " << query_plan << endl;
            float* path_distribution;
            int num_paths;
            RDFGraph** tmp_paths = 
                    LoadPathDistribution<RDFGraph>(path_distribution_data, 
                            &path_distribution,
                            num_paths);
            string edge_distribution_data = GetProperty(property_map,
                    "edge_distribution_data");
            map<int, float> edge_distribution;
            LoadEdgeDistribution(edge_distribution_data, edge_distribution);
            float triad_decomposition_cost;
            JoinTreeDAO* sj_tree = DecomposeQuery(gQuery, 
                    edge_distribution, 
                    tmp_paths, path_distribution, num_paths, 
                    &triad_decomposition_cost);
            sj_tree->Store(query_plan.c_str());
            delete[] path_distribution;
            for (int i = 0; i < num_paths; i++) {
                delete tmp_paths[i];
            }
            delete[] tmp_paths;
        }   
        else {
            cout << "Query plan exists: " << query_plan << endl;
            cout << "Skipping query decomposition ..." << endl;
        }
exit(1);
        cout << "Running search ..." << endl;
        SJTreeExecutor<RDFGraph> graph_search(gSearch, gQuery,
                search_context, query_plan);
        uint64_t run_time = rdf_stream_processor.Run(graph_search);
    }
    return;
}

void CollectEdgeDistribution()
{
    RDFStreamProcessor rdf_stream_processor("/Users/d3m432/dev/dynamic_networks/graph_search/src/social_media/test/stream.list", RDF, 0);
    string out_path = "/Users/d3m432/Documents/MATLAB/thesis/query_opt/data/lsbench_edge_type_distribution.csv";
    rdf_stream_processor.CollectPredicateDistribution(out_path);
    return;
}
*/

int main(int argc, char* argv[])
{
    //RunRDFStreamSearch(argc, argv);
    // dynamic_graph_search_main<RDFGraph, RDFStreamProcessor>(argc, argv);
    dynamic_graph_search_main<LivejournalGraph, LivejournalStreamProcessor>(argc, argv);
/*
    string p = 
        "/people/d3m432/dynamic_networks/graph_search/src/social_media/test/stream.list";
    string op = 
        "/people/d3m432/dynamic_networks/graph_search/src/social_media/test/lsbench.txt";
    GiraphExporter<Label, Timestamp> giraph_exporter;
    giraph_exporter.SetOutpath(op); 
    cout << "Exporting graph to Giraph format ..." << endl;
    ProcessGraph<RDFGraph, RDFStreamProcessor, GiraphExporter<Label, Timestamp> >(p, 
            RDF, 0, giraph_exporter);
*/
    // CollectEdgeDistribution();
    return 0;
}
