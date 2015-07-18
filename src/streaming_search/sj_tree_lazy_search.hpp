#ifndef __SJ_TREE_LAZY_SEARCH_HPP__
#define __SJ_TREE_LAZY_SEARCH_HPP__

template <typename Graph>
class LazySearch : public SJTree {
public:
    typedef typename Graph::EdgeDataType EdgeData;
    typedef typename Graph::EdgeType edge;
    typedef typename Graph::DirectedEdgeType dirE;

    init* search_bitmap__;
    int** node_dependencies__;
    
    void InitSearchBitmap(uint64_t node_count)
    {
        count = 0;
        search_bitmap__ = new int[node_count];
        bzero(search_bitmap__, node_count*sizeof(int));
    }

    /**
     * @param u: node from data graph being searched
     * @param n: node from SJ-Tree
     */
    bool IsSearchEnabled(uint64_t u, int n)
    {
        int mask = search_bitmap__[u];
        bool search_state = (mask >> n);
        return search_state; 
    }

    /**
     * @param u: node from data graph being searched
     * @param n: node from SJ-Tree
     */
    bool EnableSearch(uint64_t u, int n)
    {
        int mask = 1;
        mask <<= n; 
        search_bitmap__[u] |= mask;
        return;
    }

    int count;
    void Search(const DirectedEdge<EdgeData>& e_s)
    {
        cout << "Searching " << count++ << "-th edge" << endl;
        d_array<MatchSet*> full_match_list;

        for (int i = 0; i < leaf_count__; i++) {
            int node = leaf_ids__[i];
            uint64_t s = e_s.s;
            uint64_t t = e_s.t;
            if (!SearchEnabled(s, node) && !SearchEnabled(t, node)) {
                continue;
            } 
            d_array<MatchSet*> match_list;
            int num_matches = leaf_queries__[i]->Execute(e_s, match_list);
            for (int j = 0, N = match_list.len; j < N; j++) {
                TransformMatchSet(id_maps__[i], match_list[j]);
                const d_array<pair<uint64_t, uint64_t> > vmap = 
                        match_list[j].vertex_map;
                for (int k = 0, V = vmap.len; k < V; j++) {
                    for (int n = 0; n < node_count__; n++) {
                        if (node_dependencies__[node][n]) {
                            EnableSearch(vmap[k].second, n);
                        }
                    }
                }
                // AddMatchToTree(node, match_list[j], full_match_list, true);
                delete match_list[j];
            }
        }
    }

};

#endif
