#include <assert.h>
#include <fstream>
#include "sj_tree.h"
#include "subgraph.hpp"
using namespace std;

void JoinEntryTable :: Insert(const char* signature, const MatchSet* match)
{
    if (signature[0] == '\0' || match == NULL) {
        return;
    }
    // cout << "Inserting match with key = [" << signature << "]" << endl;
#ifdef __MTA__
    MatchSetList* match_list;
#else 
    // cout << "Printing current entries ..." << endl;
    // for (JoinEntryIterator it1 = join_entry_table__.begin();
            // it1 != join_entry_table__.end(); it1++) {   
        // cout << "   key = " << it1->first << endl;
    // }

    JoinEntryIterator it = join_entry_table__.find(signature);
    if (it == join_entry_table__.end()) {
        MatchSetList match_list;
        match_list.append(match);
        join_entry_table__.insert(pair<const char*, MatchSetList>(signature, match_list));
        // cout << "Inserted new key to table, now size = " << join_entry_table__.size() << endl;
    }
    else {
        it->second.append(match);
        // cout << "Appending to existing list ... table size = " << join_entry_table__.size() << endl;
    }
#endif
}

pair<MatchSetPtr*, MatchSetPtr*> JoinEntryTable :: EqualRange(const char* subgraph_signature) 
{
    assert(subgraph_signature[0] != '\0');
#ifdef __MTA__
    MatchSetList* match_list;
    bool stat = join_entry_table__.lookup(subgraph_signature, match_list);
    if (stat) {
        return pair<MatchSetPtr, MatchSetPtr>(match_list.begin(), 
                                              match_list.end());
    }
    else {
        return pair<MatchSetPtr*, MatchSetPtr*>(empty__.begin(),
                                                empty__.end());
    }
#else
    // cout << "Finding match with key = [" << subgraph_signature << "]" << endl;
    // cout << "Queried hash table size = " << join_entry_table__.size() << endl;
    JoinEntryCIterator it = join_entry_table__.find(subgraph_signature);
    
    if (it == join_entry_table__.end()) {
        // cout << "No matches available ...." << endl;
        return pair<MatchSetPtr*,  MatchSetPtr*> (empty__.begin(), 
                empty__.end());
    }
    else {
        // cout << "Returning range ..." << endl;
        return pair<MatchSetPtr*, MatchSetPtr*>(it->second.begin(),
                it->second.end());
    }
#endif
}

//////////////////////////////////////////////////////////////////////////
// Implementation of the JoinTreeDAO Class
//////////////////////////////////////////////////////////////////////////

JoinTreeDAO :: JoinTreeDAO(const char* src_file_path) : 
        node_count__(0), leaf_count__(0), join_tree_nodes__(NULL),
        source__(src_file_path), leaf_ids__(NULL)
{
}

JoinTreeDAO :: ~JoinTreeDAO()
{
    if (join_tree_nodes__) {
        delete[] join_tree_nodes__;
    }
    
    if (leaf_ids__) {
        delete[] leaf_ids__;
    }
    return;
}

void JoinTreeDAO :: Load()
{
    FILE* fp = fopen(source__.c_str(), "r");

    if (fp == NULL) {
        cout << "Failed to open: " << source__ << endl;
        exit(1);
    }

    int bufsize = 512;
    char line[512];
    vector<string> lines;
    int parent, node;

    if (fgets(line, bufsize, fp)) {
        sscanf(line, "BEGIN_JOIN_TREE NODE_COUNT=%d", &node_count__);
    }
    else {
        cout << "Can't read" << endl;
    }

    assert(node_count__);
    join_tree_nodes__ = new JoinTreeNode[node_count__];

    while (fgets(line, bufsize, fp)) {
        if (strlen(line) == 0 || line[0] == '#') continue;
        if (strstr(line, "END_JOIN_TREE")) {
            break;
        }
        else {
            sscanf(line, "%d -> %d", &parent, &node);
            join_tree_nodes__[node].parent = parent;
            if (join_tree_nodes__[parent].num_children == 0) {
                join_tree_nodes__[parent].children[0] = node;
            }
            else {
                join_tree_nodes__[parent].children[1] = node;
            }
            join_tree_nodes__[parent].num_children++;
        }
    }

    bool load_subgraph = false;

    while (fgets(line, bufsize, fp)) {
        if (strlen(line) == 0 || line[0] == '#') continue;
        if (strstr(line, "BEG_SUBGRAPH")) {
            load_subgraph = true;
            sscanf(line, "BEG_SUBGRAPH %d", &node);
            continue;
        }
        else if (strstr(line, "END_SUBGRAPH")) {
            if (load_subgraph) {
                join_tree_nodes__[node].cut_query_subgraph = 
                        ParseSubgraph(lines);
                lines.clear();
            }
        }
        else if (load_subgraph) {
            lines.push_back(line);
        }
    }
    fclose(fp);

    // int sibling_data;
    leaf_ids__ = new int[node_count__];

    for (int i = 0; i < node_count__; i++) {
        if (join_tree_nodes__[i].num_children == 0) {
            leaf_ids__[leaf_count__++] = i;
        }
        int p = join_tree_nodes__[i].parent;
        if (p != -1) {
            int* children = join_tree_nodes__[p].children;
            // join_tree_nodes__[i].sibling = sibling_data;
            int sibling_id = (children[0] == i) ? 
                    children[1] : children[0];
            int rhs_sibling = (i < sibling_id) ? 1 : 0;
            join_tree_nodes__[i].sibling = (sibling_id << 1) | rhs_sibling;
        }
        else {
            root__ = i;
        }
        assert(join_tree_nodes__[i].num_children <= 2);
    }
    
    // cout << "Root : " << root__ << endl;
    // cout << "Number of nodes : " << node_count__ << endl;
    // cout << "Number of leaves : " << leaf_count__ << endl;

    return;
}

Subgraph JoinTreeDAO :: ParseSubgraph(const vector<string>& lines)
{
    int v1, v2, type;
    Subgraph query_subgraph;
    for (int i = 0, num_lines = lines.size(); i < num_lines; i++) {
        const string& line = lines[i];
        if (line[0] == 'v') {
            sscanf(line.c_str(), "v %d", &v1);
            query_subgraph.AddVertex(v1);
        }
        else if (line[0] == 'e') {
            sscanf(line.c_str(), "e %d %d %d", &v1, &v2, &type);
            //gQuery__->isNeighbor(v1, v2, type);
            dir_edge_t e;
            e.s = v1;
            e.t = v2;
            query_subgraph.AddEdge(e);
        }
    }    
    query_subgraph.Finalize();
    return query_subgraph;
}

JoinTreeNode* JoinTreeDAO :: GetJoinTree(int* node_count, int** leaf_ids, 
        int* leaf_count)
{
    *node_count = node_count__;
    *leaf_ids = leaf_ids__;
    *leaf_count = leaf_count__;
    return join_tree_nodes__;
}

int JoinTreeDAO :: GetNodeCount() const
{
    return node_count__;
}

int JoinTreeDAO :: GetLeafCount() const
{
    return leaf_count__;
}

const int* JoinTreeDAO :: GetLeafIds(int& leaf_count) const
{
    leaf_count = leaf_count__;
    return leaf_ids__;
}

void JoinTreeDAO :: Store(const char* path) const
{
    ofstream fp(path, ios_base::out);
    fp << "BEGIN_JOIN_TREE NODE_COUNT=" << node_count__ << endl;
    for (int i = 0; i < node_count__; i++) {
        if (join_tree_nodes__[i].num_children != 0) {
            fp << i << " -> " << join_tree_nodes__[i].children[0] << endl;
            fp << i << " -> " << join_tree_nodes__[i].children[1] << endl;
        }
    }  
    fp << "END_JOIN_TREE" << endl;
    for (int i = 0; i < node_count__; i++) {
        fp << "BEG_SUBGRAPH " << i << endl;
        const Subgraph& subg = join_tree_nodes__[i].cut_query_subgraph;
        if (subg.EdgeCount() == 0) {
            for (Subgraph::VertexIterator it = subg.BeginVertexSet();
                    it != subg.EndVertexSet(); it++) {
                fp << "v " << *it << endl;
            }
        }
        else {
            for (Subgraph::EdgeIterator it = subg.BeginEdgeSet();
                    it != subg.EndEdgeSet(); it++) {
                fp << "e " << it->s << " " << it->t << endl;
            }
        }
        fp << "END_SUBGRAPH" << endl;
    }
    fp.close();
    return;
}
