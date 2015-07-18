#ifndef __SJ_TREE_H__
#define __SJ_TREE_H__
#include "subgraph.hpp"
#include "subgraph_search.hpp"
#include <boost/unordered_map.hpp>
using namespace std;
using namespace boost;

struct JoinTreeNode {
    JoinTreeNode() : parent(-1), sibling(-1), num_children(0)
    {
        children[0] = -1;
        children[1] = -1;
    }
    ~JoinTreeNode() 
    {
    }

    JoinTreeNode(const JoinTreeNode& other) : 
            parent(other.parent), sibling(other.sibling),
            num_children(other.num_children),
            cut_query_subgraph(other.cut_query_subgraph)
    {
        children[0] = other.children[0];
        children[1] = other.children[1];
        return;
    }

    int parent;
    int sibling;
    int children[2];
    int num_children;
    Subgraph cut_query_subgraph;
};

typedef const MatchSet* MatchSetPtr;
typedef d_array<const MatchSet*> MatchSetList;

#ifdef __MTA__
typedef xmt_hash_table<const char*, MatchSetList> __JoinEntryTable;
typedef __JoinEntry* JoinEntryIterator;
#else
// typedef multimap<const char*, MatchSetPtr> __JoinEntryTable;
// typedef multimap<const char*, MatchSetPtr>::const_iterator JoinEntryIterator;

/*
typedef unordered_map<const char*, MatchSetList > __JoinEntryTable;
typedef unordered_map<const char*, MatchSetList >::iterator JoinEntryIterator;
typedef unordered_map<const char*, MatchSetList >::iterator JoinEntryCIterator;
*/

typedef unordered_map<string, MatchSetList > __JoinEntryTable;
typedef unordered_map<string, MatchSetList >::iterator JoinEntryIterator;
typedef unordered_map<string, MatchSetList >::iterator JoinEntryCIterator;
#endif


// template <typename Graph>
class JoinTreeDAO {
public:
    JoinTreeDAO(const char* src);
    JoinTreeDAO() {}
    ~JoinTreeDAO();
    void Load(); 
    void Store(const char* path) const;
    int GetRoot() { return root__; }
    JoinTreeNode* GetJoinTree(int* node_count, int** leaf_ids, 
                              int* leaf_count);
    int GetNodeCount() const;
    int GetLeafCount() const;
    const int* GetLeafIds(int& leaf_id_count) const;
// protected:
    Subgraph ParseSubgraph(const vector<string>& lines); 
// private:
    int node_count__;
    int leaf_count__;
    JoinTreeNode* join_tree_nodes__;
    string source__;
    int* leaf_ids__;
    int  root__; 
    int* query_leaf_ids__;
    // vector<GraphQuery<Graph> > query_functions__;
};

class JoinEntryTable {
public:
    void Insert(const char* signature, const MatchSet* match);
    pair<MatchSetPtr*, MatchSetPtr*>
    EqualRange(const char* subgraph_signature);    
    size_t Size() const { return join_entry_table__.size(); }
private:
    __JoinEntryTable join_entry_table__;
    MatchSetList empty__;
};

#endif
