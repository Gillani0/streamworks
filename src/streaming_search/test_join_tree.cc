#include <iostream>
#include <assert.h>
#include "graph_loader.hpp"
#include "test_join_tree.h"

using namespace std;

void TestMatchset()
{
/*
    cout << "Testing GetSearchX() " << endl;
    DirectedEdge e_q_1(0, 1, 22);
    e_q_1.timestamp = 0;
    DirectedEdge e_q_2(0, 2, 0);
    e_q_2.timestamp = 0;

    DirectedEdge e_s_1(4, 5, 22);
    e_s_1.timestamp = 0;
    DirectedEdge e_s_2(4, 6, 0);
    e_s_2.timestamp = 0;

    MatchSet m1(3, 2);
    m1.AddMapping(e_q_1, e_s_1);
    m1.AddMapping(e_q_2, e_s_2);
    m1.Finalize();

    DirectedEdge result_e; 
    uint64_t result_v;
    m1.GetSearchEdge(e_q_1, result_e);
    assert(e_s_1 == result_e);
     
    cout << "Testing Join() with common vertex" << endl;

    DirectedEdge e_q_3(0, 3, 22);
    e_q_3.timestamp = 0;
    DirectedEdge e_q_4(0, 4, 0);
    e_q_4.timestamp = 0;

    DirectedEdge e_s_3(4, 7, 22);
    e_s_3.timestamp = 0;
    DirectedEdge e_s_4(4, 8, 0);
    e_s_4.timestamp = 0;

    MatchSet m2(3, 2);
    m2.AddMapping(e_q_3, e_s_3);
    m2.AddMapping(e_q_4, e_s_4);
    m2.Finalize();

    bool is_rhs = true;
    bool temporal_ordering = false;
    Subgraph join_subgraph1;
    join_subgraph1.AddVertex(0);
    cout << "Executing join ..." << endl;
    cout << "LHS" << endl;
    cout << m1 << endl;
    cout << "RHS" << endl;
    cout << m2 << endl;
    MatchSet* join_out1 = m1.Join(&m2, is_rhs, 
            join_subgraph1, temporal_ordering);
    if (join_out1) {
        cout << *join_out1 << endl;
    }
    else {
        cout << "Join output = NULL" << endl;
    }

    cout << "Testing Join() with common edge" << endl;

    MatchSet m3(3, 2);
    m3.AddMapping(e_q_2, e_s_2);
    m3.AddMapping(e_q_3, e_s_3);
    m3.Finalize();

    Subgraph join_subgraph2;
    join_subgraph2.AddEdge(e_q_2);

    cout << "Executing join ..." << endl;
    cout << "LHS" << endl;
    cout << m1 << endl;
    cout << "RHS" << endl;
    cout << m3 << endl;
    MatchSet* join_out2 = m1.Join(&m3, is_rhs, 
            join_subgraph2, temporal_ordering);
    if (join_out2) {
        cout << *join_out2 << endl;
    }
    else {
        cout << "Join output = NULL" << endl;
    }

    // cout << "Testing Join() with non-unique mapping" << endl;
    // cout << "Testing Join() with non-overlapping matches" << endl;
*/
    return;
}

void Test_JoinTreeDAO()
{
/*
    cout << "Running TestJoinTreeLoad()" << endl;
    const char* query_graph_path = "test/nyt_query_T4.graph";
    const char* join_tree_plan = "test/nyt_query_jtd_T4.plan";
    // Set the values of known variables from the join tree plan.
    int test_root_id = 5;
    int test_node_count = 7;
    int test_leaf_count = 4;
    int test_leaf_ids[] = { 0, 2, 4, 6}; 

    // Initialize the query graph, load and initialize the
    // Join Tree DAO object.
    Graph* gQuery = new Graph();
    GraphLoader(gQuery, query_graph_path);
    JoinTreeDAO* jtree_dao = new JoinTreeDAO(gQuery, join_tree_plan);
    jtree_dao->Load();

    // Assert that the loaded join tree has correct properties.
    assert(jtree_dao->GetRoot() == test_root_id);
    assert(jtree_dao->GetNodeCount() == test_node_count);
    assert(jtree_dao->GetLeafCount() == test_leaf_count);

    const int* leaf_ids = jtree_dao->GetLeafIds(test_leaf_count);
    for (int i = 0; i < test_leaf_count; i++) {
        assert(test_leaf_ids[i] == leaf_ids[i]);
    }   

    // Free the join tree and the query graph.
    delete jtree_dao;

    // TODO
    // Now test with bad inputs
    // jtree_dao = new JoinTreeDAO(NULL, join_tree_plan);
    // jtree_dao = new JoinTreeDAO(gQuery, "some_bad_path");
    // char* invalid_join_tree_plan = "test/nyt_query_jtd_T4.plan.bad";
    // jtree_dao = new JoinTreeDAO(gQuery, invalid_join_tree_plan);
    //->Load();
    //
    delete gQuery;
*/
}

void Test_JoinEntryTable()
{
/** TODO
    JoinEntryTable table;
    pair<MatchSetPtr*, MatchSetPtr*> range;
    range = table.EqualRange(NULL);
    assert(range.first == range.second);
    range = table.EqualRange("I'm not in table");
    assert(range.first == range.second);
    MatchSet m1;
    table.Insert("m1_sig", &m1);
    range = table.EqualRange("m1_sig");
    assert((range.second - range.first) == 1);
*/
    return;
}

int ExecuteTestDriver(int argc, char* argv[])
{
    // TestMatchset();
    // TestSearch();
    Test_JoinTreeDAO();
    return 0;
}

/*
#include <iostream>
#include "graph_loader.hpp"
#include "query_parser.hpp"
#include "sj_tree_search.hpp"
using namespace std;

typedef Graph<Label, Timestamp> StreamG;

class CaidaReader {
public:
    void ParseVertex(char* line, uint64_t& u,
            VertexProperty<Label>& v_prop) const
    {
        return; 
    }
    
    void ParseEdge(char* line, DirectedEdge<Timestamp>& edge) const
    {
    }
};

int drive_main(int argc, char* argv[]) 
{
    // TODO set
    string data_graph_path;
    // TODO set
    string query_plan_path;
    QueryParser<Label, Timestamp> query_loader;
    StreamG* gQuery = 
            query_loader.ParseQuery("test/ddos_query.graph",
                "test/ddos_vertex.property",
                "test/ddos_edge.property");
    int N = 10; 
    StreamG* gSearch = new StreamG(N);
    GraphLoader<Label, Timestamp, CaidaReader>(gSearch, 
            data_graph_path.c_str());
    SJTree<StreamG >* join_tree = 
            new SJTree<StreamG >(gSearch, gQuery, 
                    query_plan_path.c_str());
    return 0;
}
*/
