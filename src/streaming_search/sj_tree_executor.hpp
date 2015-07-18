#ifndef __SJ_TREE_EXECUTOR_HPP__
#define __SJ_TREE_EXECUTOR_HPP__

// #include "sj_tree_lazy_search.hpp"
#include "sj_tree_search.hpp"
#include "counting_search.hpp"
#include "query_parser.hpp"
using namespace std;

template <typename Graph>
class SJTreeExecutor {
public:
    typedef typename Graph::DirectedEdgeType dirE;

    SJTreeExecutor(Graph* gSearch, Graph* gQuery, 
            SearchContext context,
            string query_plan)
    {   
        query_type__ = context.query_type;

        if (context.query_type == GROUPBY) {
            if (query_plan == "None") {
                primitive_query__ = new GraphQuery<Graph>(gSearch, gQuery);
                simple_counting_search__ = 
                        CountingSearch<Graph, GraphQuery<Graph> >(gSearch, primitive_query__, 
                                context.group_by_vertices, context.min_count,
                                context.distinct);
                query_type__ = SIMPLE_GROUPBY;
                cout << "Query type: SIMPLE_GROUPBY" << endl;
            }
            else {
                sj_tree__ = new SJTree<Graph>(gSearch, gQuery, query_plan.c_str());
                counting_search__ = 
                        CountingSearch<Graph, SJTree<Graph> >(gSearch, 
                                sj_tree__, context.group_by_vertices,
                                context.min_count, context.distinct);
                cout << "Query type: Nested SJ-Tree in CountingSearch" << endl;
            }
        }
        else {
            sj_tree__ = new SJTree<Graph>(gSearch, gQuery, query_plan.c_str());
#ifdef TRY_LAZY_SEARCH
            // sj_tree__ = new LazySearch<Graph>(gSearch, gQuery, query_plan.c_str());
            sj_tree__->Init(gSearch->NumVertices());
            cout << "Query type: Lazy Search" << endl;
#elif VF2
            cout << "*** initializing VF2 based subgraph isomorphism checker ***" << endl;
            subgraph_isomorphism__ = new GraphQuery<Graph>(gSearch, gQuery);
#else
            // sj_tree__ = new SJTree<Graph>(gSearch, gQuery, query_plan.c_str());
            cout << "Query type: SJ-Tree" << endl;
#endif
        }
        total = 0;
        report_id__ = 0;
    }   

    inline int operator() (const dirE& edge) 
    {   
        int v;
        if (query_type__ == BGP) {
            // v = (*sj_tree__)(edge);
#ifdef TRY_LAZY_SEARCH
            v = sj_tree__->LazySearch(edge);
#elif VF2
            //cout << "Processing VF2 type" << edge.type << "\n"; 
            d_array<MatchSet* > match_results;
            subgraph_isomorphism__->Execute(edge, match_results);
            if(match_results.len > 0){
              cout << "found  a match\n";
            }
#else
            v = sj_tree__->AllSearch(edge);
#endif
        }
        else if (query_type__ == GROUPBY) {
            v = counting_search__(edge);
        }
        else if (query_type__ == SIMPLE_GROUPBY) {
            v = simple_counting_search__(edge);
        }
        return v;
    }   
    
    void Report(string log_path_prefix) 
    {   
        char outpath[128];
        sprintf(outpath, "/Users/d3m432/Sites/data/tree.%d.json", report_id__);

        if (query_type__ == BGP) {
            sj_tree__->PrintStatistics(log_path_prefix);
            // sj_tree__->ExportTreeStatistics(outpath);
        }
        else if (query_type__ == GROUPBY) {
            counting_search__.ExportStatistics(outpath);
        }
        else if (query_type__ == SIMPLE_GROUPBY) {
            simple_counting_search__.ExportStatistics(outpath);
        }

        // sprintf(outpath, "/Users/d3m432/Sites/data/routes.%d.csv", report_id__);
        // rename("/tmp/routes", outpath);
        report_id__++;
        return;
    }   

private:
    uint64_t total;
// #ifdef TRY_LAZY_SEARCH
    // LazySearch<Graph>* sj_tree__;
// #else
    SJTree<Graph>* sj_tree__;
// #endif
    CountingSearch<Graph, SJTree<Graph> > counting_search__;
    GraphQuery<Graph>* primitive_query__;
    GraphQuery<Graph>* subgraph_isomorphism__;
    CountingSearch<Graph, GraphQuery<Graph> > simple_counting_search__;
    QueryType query_type__;
    int report_id__;
};

#endif
