#include <ostream>
#include "query_planning.hpp"
#include "sj_tree.h"
using namespace std;

void StreamingGraphDistribution :: Update(int val)
{
    property_counts__.Add(val);
    return;
}

void StreamingGraphDistribution :: Print(ostream& os)
{
    int num_rows = summaries__.size();
    int num_cols = property_vals__.size();

    int col = 1;
    for (set<int>::iterator edge_type_it = property_vals__.begin();
            edge_type_it != property_vals__.end(); edge_type_it++) {
        os << *edge_type_it;
        if (col != num_cols) {
            os << ",";
        }   
        col++;
    }   
    os << endl;
    std::map<int, int>::iterator distribution_itr;
    for (int i = 0; i < num_rows; i++) {
        os << edge_counts__[i] << ",";
        std::map<int, int>& distribution = summaries__[i];
        col = 1;
        for (set<int>::iterator edge_type_it = property_vals__.begin();
                edge_type_it != property_vals__.end(); edge_type_it++) {
            distribution_itr = distribution.find(*edge_type_it); 
            if (distribution_itr != distribution.end()) {
                os << distribution_itr->second;
            }
            else {
                os << "0";
            }
            if (col != num_cols) {
                os << ",";
            }
            col++;
        }
        os << endl;
    }
    return;
}

std::map<int, int> StreamingGraphDistribution :: TakeSnapshot(uint64_t num_edges)
{
    Counter<int>::const_iterator it;
    std::map<int, int> distribution;

    for (it = property_counts__.begin(); it != property_counts__.end();
            it++) {
        distribution[it->first] = it->second;
        property_vals__.insert(it->first);
    }

    summaries__.push_back(distribution);
    property_counts__.reset();
    edge_counts__.push_back(num_edges);
    return distribution;
}

void LoadEdgeDistribution(string path, std::map<int, float>& edge_distribution)
{
    cout << "Loading edge distribution from: " << path << endl;
    ifstream ifs(path.c_str(), ios_base::in);

    if (ifs.is_open() == false) {
        cout << "ERROR Failed to open: " << path << endl;
    }

    char buf[1024];
    int edge_type;
    float count;
    while (ifs.good()) {
        buf[0] = '\0';
        ifs.getline(buf, 1024);
        if (strlen(buf) <= 0) {
            continue;
        }   
        stringstream strm(buf);
        strm >> edge_type >> count;
        edge_distribution[edge_type] = count; 
    }   
    ifs.close();
    return;
}

void StoreEdgeDistribution(const std::map<int, float> edge_distribution,
        string outpath)
{
    cout << "Storing edge distribution at: " << outpath << endl;
    ofstream ofs(outpath.c_str(), ios_base::out);
    for (std::map<int, float>::const_iterator it = edge_distribution.begin();
            it != edge_distribution.end(); it++) {
        ofs << it->first << " " << it->second << endl;
    }   
    ofs.close();
    return;
}

void FixSubgraphs(set<string> query_graph_edges, vector<Subgraph>& subgraphs)
{
    for (int i = 0, N = subgraphs.size(); i < N; i++) {
    }
    return;
}

JoinTreeDAO* SJTreeBuilder(const vector<Subgraph>& subgraphs)
{
    if (subgraphs.size() == 0) {
        cout << "Empty input to SJTreeBuilder()" << endl;
        return NULL;
    }

    int node_count = 2*subgraphs.size() - 1; 
    int leaf_count = subgraphs.size(); // + 1;
    int* leaf_ids = new int[leaf_count];
    JoinTreeNode* join_tree_nodes = new JoinTreeNode[node_count];
    int root_id = 0;
    Subgraph* query_subgraphs = new Subgraph[node_count];

    for (int i = 0, N = subgraphs.size(); i < N; i++) {
        if (i == 0) {
            join_tree_nodes[0].cut_query_subgraph = subgraphs[i];
            leaf_ids[0] = 0;
            query_subgraphs[0] = subgraphs[i];
            // current_root_index = 0;
        }
        else {
            int curr_leaf_id = 2*i;
            int root_id = curr_leaf_id - 1;
            int sibling_id = curr_leaf_id - 3;
            if (sibling_id < 0) {
                sibling_id = 0;
            }

            JoinTreeNode leaf_node;
            leaf_node.parent = root_id;
            leaf_node.sibling = sibling_id;
            leaf_node.cut_query_subgraph = subgraphs[i];
            query_subgraphs[curr_leaf_id] = subgraphs[i];
            leaf_ids[i] = curr_leaf_id;
            // cout << "   >>>>>Setting node = " << curr_leaf_id << endl;
            join_tree_nodes[curr_leaf_id] = leaf_node;

            JoinTreeNode& sibling_node = join_tree_nodes[sibling_id];
            sibling_node.parent = root_id;
            sibling_node.sibling = curr_leaf_id;

            // cout << "node = " << curr_leaf_id << endl;
            // cout << "sibling = " << sibling_id << endl;
            // cout << "LHS -> " << sibling_node.cut_query_subgraph << endl;
            // cout << "RHS -> " << subgraphs[i] << endl;
            Subgraph parent_cut_query_subgraph = 
                    graph_intersection(query_subgraphs[sibling_id],
                            subgraphs[i]);
            query_subgraphs[root_id] = 
                    graph_union(query_subgraphs[sibling_id],
                            subgraphs[i]);
            // cout << "OUT -> N = " << parent_cut_query_subgraph.VertexCount() << endl;
            // cout << "OUT -> M = " << parent_cut_query_subgraph.EdgeCount() << endl;
            assert(parent_cut_query_subgraph.VertexCount() != 0);

            JoinTreeNode root_node;
            root_node.children[0] = sibling_id;
            root_node.children[1] = curr_leaf_id;
            root_node.cut_query_subgraph = parent_cut_query_subgraph;
            root_node.num_children = 2;
            // cout << "   >>>>>Setting parent node = " << root_id << endl;
            join_tree_nodes[root_id] = root_node;
            // cout << "SET -> N = " << join_tree_nodes[root_id].cut_query_subgraph.VertexCount() << endl;
            // cout << "SET -> M = " << join_tree_nodes[root_id].cut_query_subgraph.EdgeCount() << endl;
        }
    }

    JoinTreeDAO* sj_tree = new JoinTreeDAO();
    sj_tree->node_count__ = node_count;
    sj_tree->leaf_count__ = leaf_count;
    sj_tree->root__ = subgraphs.size()*2 - 3;
    sj_tree->leaf_ids__ = leaf_ids;
    sj_tree->join_tree_nodes__ = join_tree_nodes;
    // sj_tree->join_entry_tables__ = new JoinEntryTable[node_count];
    delete[] query_subgraphs;
    return sj_tree;
}
