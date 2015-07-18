#include <set>
#include <map>
#include <iosfwd>

/*
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>

// #include "nytimes.hpp"
// #include "graph_loader.hpp"
// #include "subgraph_search.hpp"
using namespace std;
typedef Graph<Label, Timestamp> DynG;
typedef MatchSet* Match;

#define DYNG Graph<Label, Timestamp>

bool Label::FilterVertex(int u, int u_deg, const Label& lbl) const
{
    return true;
}
void run_tests() 
{
    using boost::property_tree::ptree;
    ptree pt;
    read_json("tests.json", pt);

    BOOST_FOREACH(ptree::value_type& v, 
            pt.get_child("tests")) {
        string test_name = v.second.get<std::string>("desc");
        string query_path = v.second.get<std::string>("query");
        string data_path = v.second.get<std::string>("data");
        int num_matches = v.second.get<int>("num_matches");
        cout << "----------------------------------------------" << endl;
        cout << "Running test: " << test_name << endl;
        
        // int N = 50000;
        Graph<Label, Timestamp>* gQuery = new Graph<Label, Timestamp>();
        GraphLoader<Label, Timestamp, NYTReader>(gQuery, query_path.c_str());
        Graph<Label, Timestamp>* gSearch = new Graph<Label, Timestamp>();
        GraphLoader<Label, Timestamp, NYTReader>(gSearch, data_path.c_str());

        d_array<Match> match_results;
        GraphQuery<DYNG>* graph_query = new GraphQuery<DYNG>(gSearch, gQuery);
        graph_query->Execute(match_results);
        cout << "Number of matches = " << match_results.len 
             << " expected = " << num_matches << endl;
        // for (int i = 0; i < match_results.len; i++) {
            // cout << "<Match>" << endl;
            // match_results[i]->Print();
        // } 
        assert(match_results.len == num_matches);
        delete gQuery;
        delete gSearch;
    }
}

void run_test()
{
    string query_path = "SetMe";
    string data_path = "SetMe";
    Graph<Label, Timestamp>* gQuery = new Graph<Label, Timestamp>();
    GraphLoader<Label, Timestamp, NYTReader>(gQuery, query_path.c_str());
    Graph<Label, Timestamp>* gSearch = new Graph<Label, Timestamp>();
    GraphLoader<Label, Timestamp, NYTReader>(gSearch, data_path.c_str());

    d_array<Match> match_results;
    GraphQuery<DYNG>* graph_query = new GraphQuery<DYNG>(gSearch, gQuery);
    graph_query->Execute(match_results);
    cout << "Number of matches = " << match_results.len << endl;
    for (int i = 0; i < match_results.len; i++) {
        cout << "<Match>" << endl;
        match_results[i]->Print();
    } 
    delete gQuery;
    delete gSearch;
}
*/

int main(int argc, char* argv[])
{
    // run_tests();
    // run_test();
    return 0;
}
