#ifndef __SJ_TREE_SEARCH_HPP__
#define __SJ_TREE_SEARCH_HPP__

#include <map>
#include "graph.hpp"
#include "subgraph.hpp"
#include "match_join.hpp"
#include "subgraph_search.hpp"
#include "sj_tree.h"
#include "match_report.h"
#include <limits.h>
#include <boost/unordered_set.hpp>
using namespace std;
using namespace boost;

char graph_signature[4096];
uint64_t search_boundary[32];
pair<uint64_t, uint64_t> kv_scratchpad[32];
pair<uint64_t, uint64_t> map[32];

template <typename Graph>
class SJTree {
public:
    typedef typename Graph::EdgeDataType EdgeData;
    typedef typename Graph::EdgeType edge;
    typedef typename Graph::DirectedEdgeType dirE;

    uint64_t num_vertices__;
    int* search_bitmap__;
    time_t* search_enable_time__;

    void Init(uint64_t node_count)
    {   
        cout << "NODE COUNT: " << node_count << endl;
        cout << "LEAF COUNT: " << leaf_count__ << endl;
        num_vertices__ = node_count;

        search_bitmap__ = new int[node_count];
        bzero(search_bitmap__, node_count*sizeof(int));

        search_enable_time__ = new time_t[node_count*leaf_count__];
        bzero(search_enable_time__, node_count*leaf_count__*sizeof(time_t));
        return;
    }   

    /**
     * @param u: node from data graph being searched
     * @param n: node from SJ-Tree
     */
    inline bool SearchEnabled(uint64_t u, int n)
    {
        if (n == 0) {
            return true;
        }
        /*
        int mask = search_bitmap__[u];
        bool search_state = (mask >> n);
        */
        bool search_state = search_enable_time__[u*leaf_count__ + n] > 0 ? 1:0;
        return search_state;
    }

    /**
     * @param u: node from data graph being searched
     * @param n: node from SJ-Tree
     */
    inline bool EnableSearchOnNode(uint64_t u, int n, time_t timestamp = 1)
    {
        bool orig_state = SearchEnabled(u, n);
        /*
        search_enable_time__[u*leaf_count__ + n] = timestamp;
        int mask_orig = search_bitmap__[u];
        bool orig_state = (bool)(mask_orig >> n);
        int mask = 1;
        mask <<= n; 
        search_bitmap__[u] |= mask;
        */
        search_enable_time__[u*leaf_count__ + n] = timestamp;
        return orig_state;
    }

    inline void DisableSearch(uint64_t u, int n)
    {
        search_enable_time__[u*leaf_count__ + n] = 0;
    }

    void DisableSearch(time_t t_old)
    {
        for (uint64_t i = 0; i < num_vertices__; i++) {
            for (int j = 0; j < leaf_count__; j++) {
                time_t set_time = search_enable_time__[i*leaf_count__ + j];
                if (set_time < t_old) {
                    DisableSearch(i, j);
                }
            }
        }
        return;
    }

    int LazySearch(const DirectedEdge<EdgeData>& e_s)
    {
#ifdef TIMING
        struct timeval start, end;
        gettimeofday(&start, NULL);
#endif
        d_array<MatchSet*> full_match_list;

        for (int i = 0; i < leaf_count__; i++) {

            int node = leaf_ids__[i];
            uint64_t s = e_s.s;
            uint64_t t = e_s.t;
            // cout << "LOG SearchEnabled: Checking" << endl;
            if (!SearchEnabled(s, node) && !SearchEnabled(t, node)) {
                // cout << "LOG SearchEnabled: continue" << endl;
                continue;
            }
            // cout << "LOG SearchEnabled: done" << endl;
            // cout << "LOG Performing search on node=" << node << endl;
            PerformSearch(i, e_s);
            // cout << "LOG PerformSearch: done" << endl;
        }

        if (last_time__ < e_s.timestamp) {
            last_time__ = e_s.timestamp;
        }        

        if (first_time__ > e_s.timestamp) {
            first_time__ = e_s.timestamp;
        }
#ifdef TIMING
        gettimeofday(&end, NULL);
        total_search__ += get_tv_diff(start, end); 
#endif
        return 0;
    }

    void EnableSearchOnMatchNeighborhood(const MatchSet* m, 
            const Subgraph& g, int sj_tree_node)
    {
        for (Subgraph::VertexIterator v_it = g.BeginVertexSet();
                v_it != g.EndVertexSet(); v_it++) {
            uint64_t query_node = *v_it;
            uint64_t data_graph_node = m->GetMatch(query_node);
            EnableSearchOnNode(data_graph_node, sj_tree_node);
        }
        return;
    }

    /**
     * This function is called to process any match found in the graph.
     * The match may be found by processing the latest edge in the dynamic
     * graph or by doing a retro-active search.
     */
    void ProcessMatch(MatchSet* m, int sj_tree_node)
    {
        // int sibling = nodes__[sj_tree_node].sibling;
        bool is_rhs_sibling;
        int sibling = GetPeerNode(sj_tree_node, &is_rhs_sibling);
        // cout << "LOG Sibling of " << sj_tree_node << " is " << sibling << endl;
        int parent = nodes__[sj_tree_node].parent;

        // cout << "       LOG: ProjectSearchSubgraph" << endl;
        // TransformMatchSet(id_maps__[sj_tree_node], m);
        const Subgraph& join_subgraph = nodes__[parent].cut_query_subgraph;
        ProjectSearchSubgraph(m, join_subgraph, graph_signature);
        // cout << "       LOG: Insert" << endl;
        join_entry_tables__[sj_tree_node].Insert(graph_signature, m);

        // This is the left-most, deepest node in the SJ-Tree representing the
        // most selective subgraph.  If a match is found for it, insert it into
        // the match collection.  Then turn on the search for its sibling node
        // in the neighborhood, and do a retroactive search to find any missed 
        // matches.
        if (sj_tree_node == 0)  {
            // Enable search for sibling
            // cout << "       LOG: EnableSearchOnMatchNeighborhood" << endl;
            EnableSearchOnMatchNeighborhood(m, join_subgraph, sibling);
            // cout << "       LOG: RetroSearch" << endl;
            RetroSearch(m, 
                    nodes__[parent].cut_query_subgraph,
                    sibling);
            // cout << "       LOG: Done RetroSearch" << endl;
        }
        else {
            // Query sibling node for Join
            JoinEntryTable& sibling_entries = join_entry_tables__[sibling];
#ifdef TIMING
            struct timeval start, end;
            gettimeofday(&start, NULL);
#endif
            pair<MatchSetPtr*, MatchSetPtr*> join_range =
                    sibling_entries.EqualRange(graph_signature);
#ifdef TIMING
            gettimeofday(&end, NULL);
            hash_table_lookup__ += get_tv_diff(start, end);
#endif
            // delete[] graph_signature;
            for (MatchSetPtr* it = join_range.first; it != join_range.second;
                    it++) {
                MatchSet* super_match = Join(*it, m, join_subgraph);
                if (super_match == NULL) continue;
                // If Join is successful
                // Check if parent is root
                // cout << "       LOG: Finding grand parent" << endl;
                int grand_parent = nodes__[parent].parent;
                if (grand_parent != -1) {
                    char scratch[128];
                    sprintf(scratch, "%llu", complete_match_count__++);
                    join_entry_tables__[parent].Insert(scratch, super_match);
                }
                // If not, enable search for parent's sibling
                else {
                    // uint64_t enable_search_on_node = nodes__[parent].sibling;
                    bool is_rhs_sibling;
                    uint64_t enable_search_on_node = GetPeerNode(parent, &is_rhs_sibling);

                    // cout << "       LOG: EnableSearchOnMatchNeighborhood1" << endl;
                    EnableSearchOnMatchNeighborhood(super_match,
                            nodes__[grand_parent].cut_query_subgraph,
                            enable_search_on_node);
                    // cout << "       LOG: RetroSearch1" << endl;

                    // cout << "       LOG: ProjectSearchSubgraph1" << endl;
                    ProjectSearchSubgraph(super_match, 
                            nodes__[grand_parent].cut_query_subgraph, 
                            graph_signature);
                    // cout << "       LOG: Insert1" << endl;
                    join_entry_tables__[parent].Insert(graph_signature, super_match);

                    RetroSearch(super_match, 
                            nodes__[grand_parent].cut_query_subgraph,
                            enable_search_on_node);
                    // cout << "       LOG: Done with RetroSearch1" << endl;
                }
            }
            // delete[] graph_signature;
        }
        return;
    }

    void RetroSearch(const MatchSet* m, const Subgraph& join_subgraph, 
            int sj_tree_node)
    {
#ifdef TIMING
        struct timeval start, end;
        gettimeofday(&start, NULL);
#endif
        int next = 0;
        // cout << "               LOG Filling kv_scratchpad" << endl;
        // Extract the nodes from G_d where to extend the search.
        for (Subgraph::VertexIterator v_it = join_subgraph.BeginVertexSet();
                v_it != join_subgraph.EndVertexSet(); v_it++) {
            uint64_t query_node = *v_it;
            uint64_t data_graph_node = m->GetMatch(query_node);
            uint64_t degree = gSearch__->NeighborCount(data_graph_node);
            kv_scratchpad[next++] = 
                    pair<uint64_t, uint64_t>(degree, data_graph_node);
        }

        // assert(next > 0);
        if (next == 0) {
            return;
        }
        // cout << "               LOG sorting kv_scratchpad" << endl;
        sort(kv_scratchpad, kv_scratchpad + next);

        // cout << "               LOG Determining init_search" << endl;
        uint64_t init_search = kv_scratchpad[0].second;
        d_array<MatchSet*> match_list;
        // cout << "               LOG Executing search: " << sj_tree_node << endl;
        int leaf_index = -1;
        for (int i = 0; i < leaf_count__; i++) {
            // cout << "               LOG leaf node id: " << leaf_ids__[i] << endl;
            if (leaf_ids__[i] == sj_tree_node) {
                leaf_index = i;
                break;
            }
        }
        assert(leaf_index >= 0);
        leaf_queries__[leaf_index]->Execute(init_search, match_list);
        // cout << "               LOG Done executing search " << endl;

        for (int i = 0, N = match_list.len; i < N; i++) {
            // cout << "               LOG Transforming match results" << endl;
            TransformMatchSet(id_maps__[leaf_index], match_list[i]);
            bool match_found = true;
            for (Subgraph::VertexIterator v_it = join_subgraph.BeginVertexSet();
                    v_it != join_subgraph.EndVertexSet(); v_it++) {
                uint64_t query_node = *v_it;
                uint64_t match1 = m->GetMatch(query_node);
                uint64_t match2 = match_list[i]->GetMatch(query_node);
                if (match1 != match2) {
                    match_found = false;
                    break;
                } 
            }
            if (match_found) {
                // cout << "               LOG ProcessMatch" << endl;
                ProcessMatch(match_list[i], sj_tree_node); 
            }
        }
#ifdef TIMING
        gettimeofday(&end, NULL);
        retro_search_time__ += get_tv_diff(start, end);
#endif
        return;
    }

    void PerformSearch(int leaf_index, const DirectedEdge<EdgeData>& e_s)
    {
        // if (node > node_count__) {
            // return;
        // }

        d_array<MatchSet*> match_list;
#ifdef TIMING
        struct timeval start, end;
        gettimeofday(&start, NULL);
#endif
        // cout << "LOG PerformSearch: Executing" << endl;
/*
        int leaf_index = -1;
        for (int i = 0; i < leaf_count__; i++) {
            if (leaf_ids__[i] == node) {
                leaf_index = i;
                break;
            }
        }
        assert(leaf_index != -1);
*/
        leaf_queries__[leaf_index]->Execute(e_s, match_list);
        // cout << "LOG PerformSearch: Executed" << endl;
#ifdef TIMING
        primitive_match_count__ += match_list.len;
        gettimeofday(&end, NULL);
        search_time__ += get_tv_diff(start, end);
#endif
        // cout << "LOG Setting sibling and parent" << endl;
        bool is_rhs_sibling;
        // int sibling = nodes__[node].sibling;
        int node = leaf_ids__[leaf_index];
        int sibling = GetPeerNode(node, &is_rhs_sibling);
        int parent = nodes__[node].parent;

        // cout << "LOG Processing " << match_list.len << " matches" << endl;
        // For each match
        for (int j = 0, N = match_list.len; j < N; j++) {
            // cout << "LOG Transforming match" << endl;
            // TransformMatchSet(id_maps__[node], match_list[j]);
            TransformMatchSet(id_maps__[leaf_index], match_list[j]);
            // cout << "LOG Processing match" << endl;
            ProcessMatch(match_list[j], node);
            // cout << "LOG Done processing" << endl;
        }
        // cout << "LOG Processed matches" << endl;
        return;
    }
 
    SJTree(const Graph* gSearch, const Graph* gQuery, const char* file) :
            gSearch__(gSearch), gQuery__(gQuery), 
            join_tree_DAO__(new JoinTreeDAO(file)), 
            complete_match_count__(0), first_time__(LONG_MAX),
            last_time__(0), header_out__(false)
    {
#ifdef TIMING
        total_search__ = 0;
        hash_table_lookup__ = 0;
        search_time__ = 0;
        primitive_match_count__ = 0;
        total__ = 0;
        transform_match_set_time__ = 0;
        add_match_to_tree_time__ = 0;
        retro_search_time__ = 0;
#endif
        join_tree_DAO__->Load();
        SetTreeProperties();
        for (int i = 0; i < gQuery__->NumVertices(); i++) {
            select_vertex_set__.insert(i);
        }
        return;
    }
    
    SJTree() {}

    SJTree(JoinTreeDAO* join_tree_DAO) : 
            join_tree_DAO__(join_tree_DAO)
    {
        SetTreeProperties();
        return;
    }

    ~SJTree()
    {
        delete join_tree_DAO__;
        delete[] join_entry_tables__;
    }

    void SetTreeProperties()
    {
        root__ = join_tree_DAO__->GetRoot();
        nodes__ = join_tree_DAO__->GetJoinTree(&node_count__, &leaf_ids__,
                                               &leaf_count__);
        join_entry_tables__ = new JoinEntryTable[node_count__];
        leaf_queries__ = new GraphQuery<Graph>*[leaf_count__];
        id_maps__ = new uint64_t*[leaf_count__];

        for (int i = 0; i < leaf_count__; i++) {
            int node_id = leaf_ids__[i];
            const Subgraph& leaf_query_subgraph = 
                    nodes__[node_id].cut_query_subgraph;
            id_maps__[i] = BuildVertexIdMap(leaf_query_subgraph);
            Graph* leaf_query_graph = 
                    BuildQueryGraph(gQuery__, leaf_query_subgraph);
            leaf_queries__[i] = new GraphQuery<Graph>(gSearch__, 
                    leaf_query_graph);
                    // BuildQueryGraph(gQuery__, leaf_query_subgraph));
        }
        return;
    }

    uint64_t* BuildVertexIdMap(const Subgraph& subg)
    {
        uint64_t* id_map = new uint64_t[subg.VertexCount()];
        int next = 0;

        for (Subgraph::VertexIterator it = subg.BeginVertexSet();
                it != subg.EndVertexSet(); it++) {
            id_map[next++] = *it; 
        }

        return id_map;
    }
    
    Graph* BuildQueryGraph(const Graph* gQuery, const Subgraph& subg)
    {
        typedef typename Graph::VertexProp VertexProp;
        typedef typename Graph::EdgeType Edge;
        typedef typename Graph::DirectedEdgeType DirectedEdge;
        int num_vertices = subg.VertexCount();
        std::map<uint64_t, uint64_t> norm_id_map;

        Graph* g = new Graph(num_vertices);
   
        for (Subgraph::VertexIterator it = subg.BeginVertexSet();
                it != subg.EndVertexSet(); it++) {
            uint64_t u = *it;
            std::map<uint64_t, uint64_t>::iterator id_map_it = norm_id_map.find(u);
            if (id_map_it == norm_id_map.end()) {
                uint64_t mapped_u = norm_id_map.size();
                norm_id_map[u] = mapped_u;
                const VertexProp& u_prop = 
                        gQuery->GetVertexProperty(u);
                g->AddVertex(mapped_u, u_prop);
            }
        }

        g->AllocateAdjacencyLists();
 
        for (Subgraph::EdgeIterator it = subg.BeginEdgeSet(); 
                it != subg.EndEdgeSet(); it++) {
            const dir_edge_t& dir_e = *it;
            DirectedEdge dir_edge;
            dir_edge.s = norm_id_map[dir_e.s];
            dir_edge.t = norm_id_map[dir_e.t];
            int num_edges;
            const Edge* edges = gQuery->Neighbors(dir_e.s, num_edges);
            for (int i = 0; i < num_edges; i++) {
                if (edges[i].dst == dir_e.t) {
                    dir_edge.type = edges[i].type;
                    dir_edge.edge_data = edges[i].edge_data;
                    break;
                }
            }
            g->AddEdge(dir_edge);
        }

        return g;
    }

    void TransformMatchSet(const uint64_t* id_map,
            MatchSet* match_set)
    {
        for (int i = 0; i < match_set->vertex_map.len; i++) {
            pair<uint64_t, uint64_t>& v_pair = match_set->vertex_map[i];
            v_pair.first = id_map[v_pair.first];
        }

        for (int i = 0; i < match_set->edge_map.len; i++) {
            pair<dir_edge_t, dir_edge_t>& e_pair = match_set->edge_map[i];
            e_pair.first.s = id_map[e_pair.first.s];
            e_pair.first.t = id_map[e_pair.first.t];
        }
        return;
    }

    inline int operator()(const DirectedEdge<EdgeData>& e_s)
    {
        return AllSearch(e_s);
    }

    void Execute(const DirectedEdge<EdgeData>& e_s, 
            d_array<MatchSet* >& match_results)
    {
        int num_matches = complete_match_count__;

        for (int i = 0; i < leaf_count__; i++) {
            int node = leaf_ids__[i];
            d_array<MatchSet*> primitive_match_list;
            leaf_queries__[i]->Execute(e_s, primitive_match_list);
            for (int j = 0, N = primitive_match_list.len; j < N; j++) {
                TransformMatchSet(id_maps__[i], primitive_match_list[j]);
                AddMatchToTree(node, primitive_match_list[j], match_results, true);
            }
        }
        
        if (last_time__ < e_s.timestamp) {
            last_time__ = e_s.timestamp;
        }        

        if (first_time__ > e_s.timestamp) {
            first_time__ = e_s.timestamp;
        }
    
        return;
    }   
    
    double get_match_size(const MatchSet* m) 
    {
        double size = 0;
        for (int j = 0; j < m->vertex_map.len; j++) {
            size += 2*sizeof(uint64_t);
        }
        for (int j = 0; j < m->edge_map.len; j++) {
            size += 2*sizeof(dir_edge_t);
        }
        return size/1E9;
    }

    int AllSearch(const DirectedEdge<EdgeData>& e_s)
    {
#ifdef TIMING
        struct timeval start, end;
        gettimeofday(&start, NULL);
#endif
        d_array<MatchSet*> full_match_list;
#ifdef DEBUG
        LOGSRC << "\n\nSearching edge: " << GetLabel(gSearch__, e_s.s) 
               << " -> " << GetLabel(gSearch__, e_s.t) 
               << ", type = " << e_s.type << endl;
        LOGSRC << "Triple: " << e_s.s << "-[" << e_s.type << "]->" <<
                e_s.t << endl;
#endif
        int num_matches = complete_match_count__;

        for (int i = 0; i < leaf_count__; i++) {
            int node = leaf_ids__[i];
#ifdef DEBUG
            LOGSRC << "Searching for leaf node = " << node << endl;
            LOGSRC << "Query summary:" << endl;
            leaf_queries__[i]->PrintInputSummary();
            leaf_queries__[i]->GetQueryGraph()->Print();
#endif
            d_array<MatchSet*> match_list;
#ifdef TIMING
            struct timeval start, end;
            gettimeofday(&start, NULL);
#endif
            int num_matches = leaf_queries__[i]->Execute(e_s, match_list);
#ifdef TIMING
            gettimeofday(&end, NULL);
            search_time__ += get_tv_diff(start, end);
            primitive_match_count__ += match_list.len;
#endif
#ifdef DEBUG
            if (match_list.len) {
                cout << "Found match for leaf query with " << leaf_queries__[i]->GetQueryGraph()->NumEdges() << endl;
                cout << "#matches = " << match_list.len << endl;
            }
#endif
            for (int j = 0, N = match_list.len; j < N; j++) {
#ifdef DEBUG
                cout << "*** Partial Match ***" << endl;
                match_list[j]->Print();
#endif
                TransformMatchSet(id_maps__[i], match_list[j]);
#ifdef DEBUG
                cout << "*** After Transformation ***" << endl;
                match_list[j]->Print();
#endif
#ifdef TIMING
                gettimeofday(&start, NULL);
#endif
                const MatchSet* mptr = match_list[j];
                assert(mptr->vertex_map.len);
                assert(mptr->vertex_map.alloc_len);
                assert(mptr->edge_map.len);
                assert(mptr->edge_map.alloc_len);
                AddMatchToTree(node, match_list[j], full_match_list, true);
                // delete match_list[j];
#ifdef TIMING
                gettimeofday(&end, NULL);
                add_match_to_tree_time__ += get_tv_diff(start, end);
#endif
            }
        }
        
        if (last_time__ < e_s.timestamp) {
            last_time__ = e_s.timestamp;
        }        

        if (first_time__ > e_s.timestamp) {
            first_time__ = e_s.timestamp;
        }
            
#ifdef TIMING
        gettimeofday(&end, NULL);
        total_search__ += get_tv_diff(start, end); 
#endif
        return (complete_match_count__ - num_matches);
    }   
    
    void PrintStatistics(string log_path_prefix)  // const
    {
        cout << "--------------------------" << endl;
        cout << "SJ-Tree statistics" << endl;
        cout << "--------------------------" << endl;
        // cout << "Earliest edge timestamp = " << ctime(&first_time__);
        // cout << "Latest edge timestamp   = " << ctime(&last_time__);
        uint64_t num_matches_in_sjt = 0;
        for (int i = 0; i < node_count__; i++) {
            // int node = leaf_ids__[i];
            cout << "SJ-Tree node id # " << i << endl;
            cout << "        number of entries=" 
                 << join_entry_tables__[i].Size() << endl;
            num_matches_in_sjt += 
                    join_entry_tables__[i].Size();
        } 
#ifdef TIMING
        char log_path[1024];
        sprintf(log_path, "%s_stats.csv", log_path_prefix.c_str());
        ofstream logger(log_path, ios_base::app);
        logger << total_search__ << "," << search_time__ << "," 
               << (hash_table_lookup__ + JoinTimer()) << "," << add_match_to_tree_time__ << "," 
               << primitive_match_count__ << "," << num_matches_in_sjt << "," 
               << total__ << "," << retro_search_time__ << endl;
        logger.close();
        cout << "total_search = " << total_search__ << endl;
        cout << "search_time," << search_time__ << endl;
        cout << "primitive_match_count," << primitive_match_count__ << endl;
        cout << "hashtable_lookup," << hash_table_lookup__ << endl;
        cout << "join_time," << JoinTimer() << endl;
        cout << "addMatchToTree," << add_match_to_tree_time__ << endl;
        cout << "transformMatchset," << transform_match_set_time__ << endl;
        cout << "num_matches_in_sjt," << num_matches_in_sjt << endl;
        cout << "retro_search_time," << retro_search_time__ << endl;
        // double total = total__ + ((float)gSearch__->NumVertices()*8/1E9);
        // cout << "Total = " << total << endl;
        cout << "num_search_states," << SearchStateCounter::Count(0) << endl;
        total_search__ = 0;
        search_time__ = 0;
        primitive_match_count__ = 0;
        hash_table_lookup__ = 0;
        transform_match_set_time__ = 0;
        add_match_to_tree_time__ = 0;
        retro_search_time__ = 0;
        total__ = 0;
#endif
        return;
    }

    void PrintTree(uint64_t node, int offset, ostream& ofs, bool is_last_entry)
    {
        PrintSpaces(offset, ofs);
        ofs << "{" << endl;
        int value;
        if (node & 0x01) {
            value = (node + 1) / 2;
        }
        else {
            value = 0;
        }
        int count = join_entry_tables__[node].Size();
        PrintSpaces(offset+4, ofs);
        ofs << "\"value\": " << value << "," << endl;

        int num_children = nodes__[node].num_children;
        PrintSpaces(offset+4, ofs);
        if (num_children > 0) {
            ofs << "\"count\": " << count << "," << endl;
        }
        else {
            ofs << "\"count\": " << count << endl;
        }
        
        if (num_children > 0) {
            PrintSpaces(offset+4, ofs);
            ofs << "\"children\": [" << endl;
            for (int i = 0; i < (num_children-1); i++) {
                PrintTree(nodes__[node].children[i], offset+8, ofs, false);
            }
            PrintTree(nodes__[node].children[num_children-1], offset+8, ofs, true);
            PrintSpaces(offset+4, ofs);
            ofs << "]" << endl;
        }
        
        PrintSpaces(offset, ofs);
        if (is_last_entry) {
            ofs << "}" << endl;
        }
        else {
            ofs << "}," << endl;
        }
        return;
    }

    void ExportTreeStatistics(string path)
    {
        ofstream ofs(path.c_str(), ios_base::out);
        PrintTree(root__, 0, ofs, true);
        ofs.close(); 
    }

    void ExportCSV(const MatchSet* match) 
    {
        set<int>::const_iterator it;
        if (header_out__ == false) {
            cout << "EXPORT_MATCH";
            for (it = select_vertex_set__.begin(); 
                    it != select_vertex_set__.end(); it++ ) {
                cout << "," << *it;
            }
            cout << endl;
            header_out__ = true;
        }

        cout << "EXPORT_MATCH";
        for (int i = 0, N = match->NumVertices(); i < N; i++) {
            if ((it = select_vertex_set__.find(i)) == 
                    select_vertex_set__.end()) {
                continue;
            }
            cout << "," << GetLabel(gSearch__, match->vertex_map[i].second);
        }
        cout << endl;
    }

    void ExportMapView__(const MatchSet* match_set, int node) 
    {
        // if ((node & 0x01) == 0) {
            // Show only partial matches resulting from joins 
            // return;
        // }
        int value;
        if (node & 0x01) {
            value = (node + 1) / 2;
        }
        else {
            value = 0;
        }
        ExportMapView(match_set, value);
    }

protected:

    inline int GetPeerNode(int node, bool* is_rhs_sibling) const
    {
        int sibling_data = nodes__[node].sibling;
        *is_rhs_sibling = sibling_data & 0x01;
        return (sibling_data >> 1); 
    }

    void AddMatchToTree(int node, const MatchSet* m, 
            d_array<MatchSet* >& match_results,
            bool at_leaf = false)
    {
#ifdef EXPORT_GEPHI
        ExportGephiStreaming(m, node);
#endif
#ifdef EXPORT_MAPVIEW
        ExportMapView__(m, node);
#endif
        bool is_rhs_sibling;
        int sibling = GetPeerNode(node, &is_rhs_sibling);
        int parent = nodes__[node].parent;
        
#ifdef TIMING
        struct timeval start, end;
        gettimeofday(&start, NULL);
#endif
        const Subgraph& join_subgraph = nodes__[parent].cut_query_subgraph;
        // char* graph_signature = new char[4096];
        ProjectSearchSubgraph(m, join_subgraph, graph_signature);
        
#ifdef TIMING
        total__ += get_match_size(m);
#endif
        join_entry_tables__[node].Insert(graph_signature, m);
        JoinEntryTable& sibling_entries = join_entry_tables__[sibling];
        pair<MatchSetPtr*, MatchSetPtr*> join_range = 
                sibling_entries.EqualRange(graph_signature);
        // delete[] graph_signature;
#ifdef TIMING
        gettimeofday(&end, NULL);
        hash_table_lookup__ += get_tv_diff(start, end);
#endif
        
        bool is_temporal_join = false; 
        for (MatchSetPtr* it = join_range.first; it != join_range.second; 
                it++) {
            MatchSet* super_match;
            if (is_rhs_sibling) {
                super_match = Join(m, *it, join_subgraph);
            }
            else {
                super_match = Join(*it, m, join_subgraph);
            }
                                 
            if (super_match == NULL) continue;

            
            if (nodes__[parent].parent == -1) {
                match_results.append(super_match);
                char scratch[128];
                sprintf(scratch, "%llu", complete_match_count__++);
#ifdef TIMING
                total__ += get_match_size(super_match);
#endif
                join_entry_tables__[parent].Insert(scratch, super_match);
#ifdef EXPORT_GEPHI
                ExportGephiStreaming(super_match, parent);
#endif
#ifdef EXPORT_MAPVIEW
                ExportMapView__(super_match, parent);
#endif
            }
            else {
                AddMatchToTree(parent, super_match, match_results);
                delete super_match;
            }
        }
        return;        
    }
    
/*
    void AddMatchToTree(int node, const MatchSet* m, bool at_leaf = false)
    {
#ifdef EXPORT_GEPHI
        ExportGephiStreaming(m, node);
#endif
#ifdef EXPORT_MAPVIEW
        ExportMapView__(m, node);
#endif
        bool is_rhs_sibling;
        int sibling = GetPeerNode(node, &is_rhs_sibling);
        int parent = nodes__[node].parent;
        
        const Subgraph& join_subgraph = nodes__[parent].cut_query_subgraph;
        char* graph_signature = new char[4096];
        ProjectSearchSubgraph(m, join_subgraph, graph_signature);
        
#ifdef DEBUG
        cout << "Inserting match into Hash-Table for node = " << node << endl;
#endif
        // Sutanay 2013/09/11
        // Commenting out the following check.  This was meant for the case
        // when each leaf in the join tree corresponds to the same query.
        // if (node == 0 || at_leaf == false) {
            join_entry_tables__[node].Insert(graph_signature, m);
        // }
#ifdef DEBUG
        cout << "Querying sibling node " << sibling <<  " for join..." << endl;
#endif
        JoinEntryTable& sibling_entries = join_entry_tables__[sibling];
        pair<MatchSetPtr*, MatchSetPtr*> join_range = 
                sibling_entries.EqualRange(graph_signature);
        delete[] graph_signature;
#ifdef DEBUG
        cout << "# of candidates = " 
             << (join_range.second - join_range.first) << endl;
#endif
        
        bool is_temporal_join = false; // TODO
        for (MatchSetPtr* it = join_range.first; it != join_range.second; 
                it++) {
            MatchSet* super_match;// = m->Join(*it, is_rhs_sibling, 
            if (is_rhs_sibling) {
                super_match = Join(m, *it, join_subgraph);
            }
            else {
                super_match = Join(*it, m, join_subgraph);
            }
                                            //join_subgraph); //, is_temporal_join);
            if (super_match == NULL) continue;
            
            if (nodes__[parent].parent == -1) {
                char scratch[128];
                sprintf(scratch, "%llu", complete_match_count__++);
                join_entry_tables__[parent].Insert(scratch, super_match);
#ifdef DEBUG
                cout << "!!!! Full query match !!!" << endl;
                super_match->Print();
#endif
#ifdef EXPORT_GEPHI
                ExportGephiStreaming(super_match, parent);
#endif
#ifdef EXPORT_MAPVIEW
                ExportMapView__(super_match, parent);
#endif
            }
            else {
                AddMatchToTree(parent, super_match);
                delete super_match;
            }
        }
        
        return;        
    }
*/
    
    void ProjectSearchSubgraph(const MatchSet* match, 
            const Subgraph& filter_subgraph, char* sbuf) const
    {
        Subgraph result(filter_subgraph.VertexCount(), 
                        filter_subgraph.EdgeCount());
        
        for (Subgraph::VertexIterator v_it = filter_subgraph.BeginVertexSet();
                v_it != filter_subgraph.EndVertexSet(); v_it++) {
            uint64_t v;
            v = match->GetMatch(*v_it);
            result.AddVertex(v);
        }
        
        if (filter_subgraph.EdgeCount() != 0) {
            for (Subgraph::EdgeIterator e_it = filter_subgraph.BeginEdgeSet(); 
                    e_it != filter_subgraph.EndEdgeSet(); e_it++) {
                dir_edge_t mapped_edge = match->GetMatch(*e_it);
                result.AddEdge(mapped_edge);
            }
        }
        
        result.Finalize();
        
        int sbuf_offset = 0;
        int tmp_len = 0;
        sbuf[0] = '\0';
        char tmp[512];
        
        for (Subgraph::VertexIterator v_it = result.BeginVertexSet();
             v_it != result.EndVertexSet(); v_it++) {
            // sprintf(tmp, "%llu\n", *v_it);
            sprintf(tmp, "%llu_", *v_it);
            tmp_len = strlen(tmp);
            memcpy(sbuf + sbuf_offset, tmp, tmp_len);
            sbuf_offset += tmp_len;
        }
        
        for (Subgraph::EdgeIterator e_it = result.BeginEdgeSet();
             e_it != result.EndEdgeSet(); e_it++) {
            // sprintf(tmp, "%llu-%llu", e_it->s, e_it->t);
            sprintf(tmp, "%llu-%llu_", e_it->s, e_it->t);
            tmp_len = strlen(tmp);
            memcpy(sbuf + sbuf_offset, tmp, tmp_len);
            sbuf_offset += tmp_len;
        }
        
        sbuf[sbuf_offset] = '\0';
        return;
    }    
    
// protected:
    const Graph* gSearch__;
    const Graph* gQuery__;
    JoinTreeDAO* join_tree_DAO__;
    JoinTreeNode* nodes__;
    JoinEntryTable* join_entry_tables__;
    GraphQuery<Graph>** leaf_queries__;
    uint64_t** id_maps__;
    int* leaf_ids__;
    int node_count__;
    int leaf_count__;
    int root__;
    uint64_t complete_match_count__;
    time_t last_time__;
    time_t first_time__;
    set<int> select_vertex_set__;
    bool header_out__;

#ifdef TIMING
    uint64_t total_search__;
    uint64_t hash_table_lookup__;
    uint64_t search_time__;
    uint64_t primitive_match_count__;
    double total__;
    uint64_t add_match_to_tree_time__;
    uint64_t transform_match_set_time__;
    uint64_t retro_search_time__;
#endif    
};
#endif
