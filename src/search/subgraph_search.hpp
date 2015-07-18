#ifndef __SEARCH_SUBGRAPH_SEARCH_HPP__
#define __SEARCH_SUBGRAPH_SEARCH_HPP__

#include <assert.h>
#include "graph.hpp"
#include "subgraph.hpp"
#include "utils.h"
#include "iteration_macros.hpp"
#include <vector>
#include <set>
#include <queue>
#include <iostream>
#include <fstream>
#include <sys/stat.h>
#include "search_order.hpp"
#include "gephi_client.h"
#include "match.h"
#ifdef VF2
#include "VF2.hpp"
#endif
using namespace std;

template <typename Graph>
struct SearchInput {
    typedef typename Graph::DirectedEdgeType directed_edge;
    int num_vertices;
    int num_edges;
    vector<int> query_vertices;
    vector<directed_edge> query_edges;
    const Graph* gQuery;
};

template <typename Graph>
void SetSearchInput(SearchInput<Graph>& search_input, const Graph* gQuery)
{
    typedef typename Graph::EdgeType edge;
    typedef typename Graph::DirectedEdgeType directed_edge;
    search_input.num_vertices = gQuery->NumVertices();
    search_input.num_edges = gQuery->NumEdges();

    for (int i = 0; i < search_input.num_vertices; i++) {
        search_input.query_vertices.push_back(i);
    }
    
    for (int i = 0; i < search_input.num_vertices; i++) {
        int num_edges;
        const edge* edges = gQuery->Neighbors(i, num_edges);
        for (int j = 0; j < num_edges; j++) {
            if (edges[j].outgoing) {
                directed_edge dir_edge(i, edges[j].dst,
                        edges[j].type, edges[j].edge_data);
                search_input.query_edges.push_back(dir_edge);
            }
        }
    }

    search_input.gQuery = gQuery; 
    return;
}

enum Status { UNMATCHED, MATCHED };

/*
template <typename EdgeData>
struct GraphWalkEntry {
    uint64_t u;
    Edge<EdgeData> u_edge;
    GraphWalkEntry(uint64_t v, const Edge<EdgeData>& v_edge):
        u(v), u_edge(v_edge) {}
};

template <typename VertexData, typename EdgeData>
void GetSearchOrder(const Graph<VertexData, EdgeData>* g, 
        uint64_t init_v,
        vector<GraphWalkEntry<EdgeData> >& walk_order)
{
    typedef pair<uint64_t, uint64_t> vertex_pair;
    set<vertex_pair> visited;

    queue<vertex_pair> visit_queue;
    visit_queue.push(vertex_pair(init_v, init_v));
    
    // cout << "Search order" << endl; 
    while (visit_queue.size()) {
        vertex_pair pair = visit_queue.front();
        uint64_t v = pair.first;
        uint64_t parent = pair.second;
        visit_queue.pop();

        int neighbor_count;
        const Edge<EdgeData>* neighbors = 
                g->Neighbors(v, neighbor_count);

        for (int i = 0; i < neighbor_count; i++) {

            const Edge<EdgeData>& e = neighbors[i];

            vertex_pair v_pair;
            if (v < e.dst) {
                v_pair.first = v;
                v_pair.second = e.dst;
            }
            else {
                v_pair.first = e.dst;
                v_pair.second = v;
            }
                
            set<vertex_pair>::iterator it = visited.find(v_pair);
        
            if (it != visited.end()) continue;

            // cout << "walk order = " << v << " -- " << e.dst << endl;
            walk_order.push_back(GraphWalkEntry<EdgeData>(v, e));
            visit_queue.push(vertex_pair(e.dst, v));
            visited.insert(v_pair);
        }        
    }
    return;
}
*/


class SearchStateCounter {
public:    
    static long long Count(int op) 
    {
        static SearchStateCounter counter;
        if (op < 0) {
            counter.count -= 1;
        }
        else if (op > 0) {
            counter.count += 1;
        }
        return counter.count;
    }

private:
    SearchStateCounter()
    {
        count = 0;
    }
    long long count;
};

template <typename Graph>
class SearchState {
public:
    typedef typename Graph::EdgeType edge;
    typedef typename Graph::DirectedEdgeType directed_edge;

    // SearchState()
    // {
        // SearchStateCounter::Count(1);
    // }

    SearchState(const SearchInput<Graph>* search_input, 
        uint64_t v_q, uint64_t v_s) :
            search_input__(search_input), 
            num_vertices__(search_input->num_vertices),
            num_edges__(search_input->num_edges)
    {
        Init();
        vertex_match_status__[v_q] = MATCHED;
        matched_vertices__[v_q] = v_s;
        // SearchStateCounter::Count(1);
        return;
    }

    SearchState(const SearchInput<Graph>* search_input, 
            const directed_edge& e_q, 
            const directed_edge& e_s):
            search_input__(search_input), 
            num_vertices__(search_input->num_vertices),
            num_edges__(search_input->num_edges)
    {
        Init();
        this->__AddEdgeMapping(e_q, e_s);
        // SearchStateCounter::Count(1);
        return;
    }

    ~SearchState() 
    {
        delete[] matched_vertices__;
        delete[] matched_edges__;
        delete[] vertex_match_status__;
        delete[] edge_match_status__;
        // SearchStateCounter::Count(-1);
    }

    SearchState* AddEdgeMapping(const directed_edge& e_q, 
            const directed_edge& e_s)
    {
        SearchState* new_state = new SearchState(*this);
        // SearchStateCounter::Count(1);
        new_state->__AddEdgeMapping(e_q, e_s); 
        return new_state;
    }

    inline bool GetMatchedVertex(bool query_to_data_map, 
            uint64_t v_1, uint64_t& v_2)
    {
        // cout << "num_vertices=" << num_vertices__ << endl;
        if (query_to_data_map) {
            if (vertex_match_status__[v_1] == MATCHED) {
                v_2 = matched_vertices__[v_1];
                return true;
            }   
            else {
                return false;
            }
        }
        else {
            for (int i = 0; i < num_vertices__; i++) {
                if (vertex_match_status__[i] == MATCHED && 
                        matched_vertices__[i] == v_1) {
                    v_2 = i;
                    return true;
                }
            }
            return false;
        }
    }

    bool GetMatchedEdge(bool query_to_data, 
            const directed_edge& query, directed_edge& mapped_edge)
    {
        if (query_to_data) {
            for (int i = 0; i < num_edges__; i++) {
                if (search_input__->query_edges[i] == query) {
                    if (edge_match_status__[i] == MATCHED) {
                        mapped_edge = matched_edges__[i];
                        return true;
                    }
                    else {
                        return false;
                    }
                }
            }
            return false;
        }
        else {
            for (int i = 0; i < num_edges__; i++) {
                if (matched_edges__[i] == query) {
                    if (edge_match_status__[i] == MATCHED) {
                        mapped_edge = search_input__->query_edges[i];
                        return true;
                    }
                    else {
                        return false;
                    }
                }
            }
            return false;
        }
    }

    bool IsMatchComplete()
    {
        bool match = true;
        for (int i = 0; i < num_vertices__; i++) {  
            if (vertex_match_status__[i] != MATCHED) {
               match = false;
                break;
            }
        }

        if (match == false) return match;
        match = true;

        for (int i = 0; i < num_edges__; i++) {
            if (edge_match_status__[i] != MATCHED) {
                match = false;
            }
        }
        return match;
    }

    void Print()
    {
        cout << "-------------------------------" << endl;
        for (int i = 0; i < num_vertices__; i++) {
            if (vertex_match_status__[i] == MATCHED) {
                cout << "V[" << i << "] -> " << matched_vertices__[i] << endl;
            }
        }
        for (int i = 0; i < num_edges__; i++) {
            if (edge_match_status__[i] == MATCHED) {
                cout << "E_q:" << search_input__->query_edges[i] << endl;
                cout << "E_s:" << matched_edges__[i] << endl;
            }
        } 
        cout << "..............................." << endl;
    }

    MatchSet* Export()
    {
        assert(num_edges__ > 0);
        assert(num_vertices__ > 0);
        MatchSet* match_set = new MatchSet();

        for (int i = 0; i < num_vertices__; i++) {
            match_set->vertex_map.append(pair<uint64_t, uint64_t>( 
                    search_input__->query_vertices[i],
                    matched_vertices__[i]));
        }
        for (int i = 0; i < num_edges__; i++) {
            dir_edge_t e_q = GetRelation(search_input__->query_edges[i]);
            dir_edge_t e_s = GetRelation(matched_edges__[i]);
            match_set->edge_map.append(pair<dir_edge_t, dir_edge_t>(e_q, e_s));
        }
   
        // match_set->num_vertices = num_vertices__;
        // match_set->num_edges = num_edges__;
        assert(match_set->NumEdges() > 0);
        assert(match_set->NumVertices() > 0);
        return match_set;
    }

private:
    void Init() 
    {
#ifdef DEBUG
        assert(num_vertices__);
        assert(num_edges__);
#endif
        matched_vertices__ = new uint64_t[num_vertices__];
        vertex_match_status__ = new Status[num_vertices__];
        matched_edges__ = new directed_edge[num_edges__];
        edge_match_status__ = new Status[num_edges__];

        for (int i = 0; i < num_vertices__; i++) {
            vertex_match_status__[i] = UNMATCHED;
        }
    
        for (int i = 0; i < num_edges__; i++) {
            edge_match_status__[i] = UNMATCHED;
        }
        return;
    }

    SearchState(const SearchState& other) : 
            search_input__(other.search_input__), 
            num_vertices__(other.num_vertices__),
            num_edges__(other.num_edges__)
    {
#ifdef DEBUG
        assert(num_vertices__);
        assert(num_edges__);
#endif
        matched_vertices__ = new uint64_t[num_vertices__];
        vertex_match_status__ = new Status[num_vertices__];
        matched_edges__ = new directed_edge[num_edges__];
        edge_match_status__ = new Status[num_edges__];

        for (int i = 0; i < num_vertices__; i++) {
            matched_vertices__[i] = other.matched_vertices__[i];
            vertex_match_status__[i] = other.vertex_match_status__[i];
        }
    
        for (int i = 0; i < num_edges__; i++) {
            matched_edges__[i] = other.matched_edges__[i];
            edge_match_status__[i] = other.edge_match_status__[i];
        }
        return;
    }

    void __AddEdgeMapping(const directed_edge& e_q, 
            const directed_edge& e_s)
    {
        uint64_t v_q_s = e_q.s;
        uint64_t v_q_t = e_q.t;
        uint64_t v_s_s = e_s.s;
        uint64_t v_s_t = e_s.t;
        matched_vertices__[v_q_s] = v_s_s;
        matched_vertices__[v_q_t] = v_s_t;
        vertex_match_status__[v_q_s] = MATCHED;
        vertex_match_status__[v_q_t] = MATCHED;
            
        bool search_edge_inserted = false;

        for (int i = 0; i < num_edges__; i++) {
            if (search_input__->query_edges[i] == e_q) {
                matched_edges__[i] = e_s;
                edge_match_status__[i] = MATCHED;
                search_edge_inserted = true;
                break;
            }
        }
#ifdef DEBUG
        assert(search_edge_inserted);
#endif
        return;
    }

    const SearchInput<Graph>* search_input__;
    int num_vertices__;
    int num_edges__;
    directed_edge*          matched_edges__;
    uint64_t*               matched_vertices__;
    Status*                 edge_match_status__;
    Status*                 vertex_match_status__;;
};

template <typename Graph>
struct GraphSearchImpl {

    typedef typename Graph::EdgeType edge;
    typedef typename Graph::DirectedEdgeType directed_edge;

    uint64_t GetInitQueryVertex(const Graph* gQuery);
    void GetMatchingVertexCandidates(const Graph* gSearch, 
            uint64_t v_q, vector<uint64_t>& candidates);

    bool MatchVertex(const Graph* gQuery, const Graph* gSearch,
            uint64_t v_q, uint64_t v_s);
    bool MatchEdge(const Graph* gQuery, const Graph* gSearch,
            const edge& e_q, const edge& e_s);

    bool SatisfyConstraints(SearchState<Graph>* state, 
            const edge& e_q, const edge& e_s);
    bool TerminateSearch() { return false; };
    // void ReportProgress() {}
};

template <typename Graph>
struct DefaultSubgraphSearchImpl : public GraphSearchImpl<Graph> {

    typedef typename Graph::EdgeType edge;
    typedef typename Graph::DirectedEdgeType directed_edge;

    uint64_t GetInitQueryVertex(const Graph* gQuery)
    {
        int max_degree(0);
        uint64_t max_deg_v(0);
        for (int i = 0, N = gQuery->NumVertices(); i < N; i++) {
            int num_edges = 0;
            const edge* edges = gQuery->Neighbors(i, num_edges); 
            bool sink_vertex = true;
            for (int j = 0; j < num_edges; j++) {
                if (edges[j].outgoing == true) { 
                    sink_vertex = false;
                    break;
                }
            }
            if (sink_vertex) {
                return i;
            }
        }

        for (int i = 0, N = gQuery->NumVertices(); i < N; i++) {
            int deg = gQuery->NeighborCount(i);
            if (deg > max_degree) {
                max_degree = deg;
                max_deg_v = i;
            }
        }
        return max_deg_v;
    }

    void GetMatchingVertexCandidates(const Graph* gSearch,
            const Graph* gQuery, uint64_t v_q, vector<uint64_t>& v_s_list)
    {
        FORALL_VERTICES(gSearch) {
            if (MatchVertex(gQuery, gSearch, v_q, i)) {
                v_s_list.push_back(i);
            }
        }
        return;
    }

    bool MatchVertex(const Graph* gQuery, const Graph* gSearch,
            uint64_t v_q, uint64_t v_s)
    {
#ifdef DEBUG
        LOGSRC << "     Comparing vertices v_q=" << v_q << " and v_s=" << v_s << endl;
#endif
        typename Graph::VertexProp v_s_prop = gSearch->GetVertexProperty(v_s);
        typename Graph::VertexProp v_q_prop = gQuery->GetVertexProperty(v_q);

        int deg_v_q = gQuery->NeighborCount(v_q);
        int deg_v_s = gSearch->NeighborCount(v_s);

        if (deg_v_s < deg_v_q) {
#ifdef DEBUG
            LOGSRC << "       MatchVertex::DegreeConstraintFailed " 
                 << deg_v_s << "/" << deg_v_q << endl;
#endif
            return false;
        }
        // else {
            // cout << "       DEGREE_CHECK_SUCCESS" << endl;
        // }

/*
        if (v_s_prop.type != v_q_prop.type) {
#ifdef DEBUG
            LOGSRC << "       MatchVertex::TypeConstraintFailed" << endl;
            LOGSRC << "          type(v_s) = " << v_s_prop.type 
                   << " type(v_q) = " << v_q_prop.type << endl;
#endif
            return false;
        }
*/

        bool vertex_label_check = FilterLabel(v_s_prop.vertex_data.label, 
                v_q_prop.vertex_data.label);

        if (vertex_label_check == false) {
#ifdef DEBUG
            LOGSRC << "       MatchVertex::LabelCheckFailed" << endl;
            LOGSRC << "       v_s_label = [" << v_s_prop.vertex_data.label << "]" << endl;
            LOGSRC << "       v_q_label = [" << v_q_prop.vertex_data.label << "]" << endl;
#endif
            return false;
        }
        else {
            bool stat = v_s_prop.vertex_data.FilterVertex(v_q, deg_v_s, 
                    v_q_prop.vertex_data);
#ifdef DEBUG
            if (stat) {
                LOGSRC << "     MatchVertex::OK" << endl;
            }
            else {
            LOGSRC << "       MatchVertex::FilterVertexFailed" << endl;
            }
#endif
            return stat;
        }
/*
        if (v_q_prop.vertex_data.label.size() == 0) {
            // cout << "       MatchVertex::SUCCESS" << endl;
            return true;
        }
        else {
            bool label_check;
            if (!strncmp("re:", v_q_prop.vertex_data.label.c_str(), 3)) {
                label_check = regex_match(v_s_prop.vertex_data.label.c_str(),
                        regex(v_q_prop.vertex_data.label.c_str() + 3));
            }
            else {
                label_check = v_q_prop.vertex_data.label == 
                        v_s_prop.vertex_data.label;
            }
            if (label_check) {
                return FilterVertex(v_q, dev_v_s, 
                        v_s_prop.vertex_data.label);
            }
            // return FilterVertex(v_q, deg_v_s, v_s_prop.vertex_data.label);
        }
        else {
            // cout << "       MatchVertex::VertexDataCheck" << endl;
            // cout << "       " << v_q_prop.vertex_data.label << endl;
            return v_q_prop.vertex_data == v_s_prop.vertex_data;
        }
*/
    }

    bool MatchEdge(const Graph* gQuery, const Graph* gSearch,
            const edge& e_q, const edge& e_s)
    {
        if (e_s.type != e_q.type || e_s.outgoing != e_q.outgoing) {
#ifdef DEBUG
            LOGSRC << "       MatchEdge::comparing(e.dst) v_q=" << e_q.dst << " with v_s=" << e_s.dst << endl;
            LOGSRC << "       Match edge failed for type/outgoing prop" << endl;
            LOGSRC << "       e_q.type = " << e_q.type << " e_s.type = " << e_s.type << endl;
            LOGSRC << "       type check = " << (e_s.type == e_q.type) << endl;
            LOGSRC << "       e_q.dir = " << e_q.outgoing << " e_s.dir = " << e_s.outgoing << endl;
            LOGSRC << "       dir check = " << (e_s.outgoing != e_q.outgoing) << endl;
#endif
            return false;
        }
        else {
#ifdef DEBUG
            LOGSRC << "       MatchEdge::comparing(e.dst) v_q=" << e_q.dst << " with v_s=" << e_s.dst << endl;
#endif
            bool flag = MatchVertex(gQuery, gSearch, e_q.dst, e_s.dst);
#ifdef DEBUG
            LOGSRC << "       MatchEdge::match_edge = " << flag << endl;
#endif
            if (flag && e_q.id != -1) {
                flag = e_s.edge_data.FilterEdge(e_q.id);
            }
            return flag;
        }
    }


    bool SatisfyConstraints(SearchState<Graph>* state,
            const edge& e_q, const edge& e_s)
    {
        // Take two graphs G1 = (AB, BC, AC) and
        // G2 = (PQ, QR, QS) and look at edge-mappings
        // for corner cases.
        uint64_t v_q = e_q.dst;
        uint64_t v_s = e_s.dst;
        uint64_t v_q_match, v_s_match;

        if (state->GetMatchedVertex(true, v_q, v_q_match) &&
                v_q_match != v_s) {
            // cout << "v_s " << v_s << " is mapped to " << v_q_match << " instead of "
                 // << v_q << endl;
            // state->Print();
            // cout << "SatisfyConstraints::GetMatchedVertex1 = FAIL"<< endl;
            return false;
        }

        if (state->GetMatchedVertex(false, v_s, v_s_match) &&
                v_s_match != v_q) {
            // cout << "v_q " << v_q << " is mapped to " << v_s_match << " instead of "
                 // << v_s << endl;
            // state->Print();
            // cout << "SatisfyConstraints::GetMatchedVertex2 = FAIL"<< endl;
            return false;
        }
        // cout << "SatisfyConstraints::SUCCESS" << endl;
        return true;
    }
};

template <typename Graph>
class GraphQuery {
public:
    typedef typename Graph::EdgeDataType EdgeData;
    typedef typename Graph::EdgeType edge;
    typedef typename Graph::DirectedEdgeType dirE;

    GraphQuery() 
    {
        cout << "Uninitialized GraphQuery object ..." << endl;
        gen_search_order = 0;
        gen_calls = 0;
    }

    GraphQuery(const Graph* gSearch, const Graph* gQuery, int init_v = -1) 
    {
        gen_search_order = 0;
        gen_calls = 0;
        assert(gSearch);
        assert(gQuery);
        assert(gQuery->NumEdges());

        this->gSearch = gSearch;
        this->gQuery = gQuery;
    
        // Setup the vertex and edge information to search for.
        SetSearchInput(search_input, gQuery);
        if (init_v == -1) {
            // Choose the vertex to begin the search.
            init_v_q__ = search_impl.GetInitQueryVertex(gQuery);
        }
        else {
            init_v_q__ = (uint64_t) init_v;
        }
        // Determine the order in which to search.
        GenerateSearchOrder(gQuery, init_v_q__, graph_walk_order__); 

        GraphWalkEntry<EdgeData>& first_walk_entry = 
                    graph_walk_order__[0];
        // uint64_t v_q = first_walk_entry.u;
        // const edge& first_query_edge = first_walk_entry.u_edge;
        first_query_edge = first_walk_entry.u_edge;
        // first_query_edge = MakeEdge(v_q, e_q);

        current_queue = &queue1;
        next_queue = &queue2;
        return;
    }

    void SetSearchOrder(vector<GraphWalkEntry<EdgeData> >& walk_order)
    {
        graph_walk_order__ = walk_order;
        first_query_edge = walk_order[0].u_edge; 
        return;
    }

    const Graph* GetDataGraph()
    {
        return gSearch;
    }

    const Graph* GetQueryGraph()
    {
        return gQuery;
    }

    void SetDataGraph(const Graph* data_graph)
    {
        gSearch = data_graph;
    }

    /*
     * Solves the subgraph isomorphism problem.  Find instances of
     * gQuery in gSearch.  The search is seeded by picking a v_q,
     * and finding all its matching vertices in gSearch.
     */
    void Execute(d_array<MatchSet* >& match_results)
    {
        current_queue->Clear();
        next_queue->Clear();
        InitSearch(init_v_q__, current_queue);
        Run(match_results);
        return;
    }

    /*
     * Performs subgraph isomorphism by searching around an edge in
     * the data graph. The source vertex of the edge is picked and 
     * compared with every vertex in the query graph to instantiate
     * a search.
     */
    uint64_t gen_search_order;
    uint64_t gen_calls;
    int Execute(const dirE& e_s, 
            d_array<MatchSet* >& match_results)
    {
        int num_matches = 0;
        uint64_t init_v_s = e_s.s;
        edge init_e = edge(e_s, true); 

        for (int i = 0, N = gQuery->NumVertices(); i < N; i++) {
#ifdef DEBUG
            LOGSRC << "Execute(e, matches)::Comparing v_q=" << i << " with v_s=" << init_v_s << endl;
#endif
            if (search_impl.MatchVertex(gQuery, gSearch, i, init_v_s)) {
                int neighbor_count;
                const edge* neighbors = gQuery->Neighbors(i, neighbor_count);
                for (int j = 0; j < neighbor_count; j++) {
                    if (search_impl.MatchEdge(gQuery, gSearch, neighbors[j], init_e)) {

                        dirE e_q = MakeEdge(i, neighbors[j]);
                        SearchState<Graph>* state = 
                                new SearchState<Graph>(&search_input, e_q, e_s);
                        if (state->IsMatchComplete()) {
 #ifdef DEBUG
                            LOGSRC << "FOUND FULL MATCH" << endl;
                            state->Print();
 #endif
                            match_results.append(state->Export());
                            num_matches++;
                            delete state;
                        }
                        else {
                            GraphQuery<Graph> query(gSearch, gQuery);
                            vector<GraphWalkEntry<EdgeData> > graph_walk_order;
                            struct timeval t1, t2;
                            gettimeofday(&t1, NULL);
                            GenerateSearchOrder(gQuery, i, neighbors[j].dst,
                                    graph_walk_order);
                            gettimeofday(&t2, NULL);
                            gen_search_order += get_tv_diff(t1, t2);
                            gen_calls++;
                            if (gen_calls == 1000000) {
                                cout << "*** gen_search_order_time = " << gen_search_order << endl;
                                gen_search_order = 0;
                                gen_calls = 0;
                            }
                            query.SetSearchOrder(graph_walk_order);
 #ifdef DEBUG
                            for (int k = 0; k < graph_walk_order.size(); k++) {
                                cout << "u= " << graph_walk_order[k].u <<
                                     " v= " << graph_walk_order[k].u_edge.dst << endl;
                            } 
#endif
                            // query.current_queue->Clear();
                            // query.next_queue->Clear();
                            query.current_queue->append(state); 
#ifdef DEBUG
                            LOGSRC << "**** CALLING query.Run(match_results, 1)" << endl;
                            LOGSRC << "Initializing with search state ..." << endl;
                            state->Print();
#endif
                            num_matches += query.Run(match_results, 1);
#ifdef DEBUG
                            cout << "MATCH RESULTS = " << match_results.len << endl;
#endif
                        }
                    }
                }
            }
        }

        return num_matches;
    }

/*
    void Execute__DEPRECATE(const dirE& e_s,
            d_array<MatchSet* >& match_results)
    {
        // cout << "Calling execute()" << endl;
        current_queue->Clear();
        next_queue->Clear();
        // cout << "Calling initSearch()" << endl;
        InitSearch(e_s, current_queue);
        // cout << "Calling run()" << endl;
        Run(match_results);
        return;
    }
*/

    /*
     * Performs subgraph isomorphism by searching around a vertex
     * in the data graph.  Each vertex in the query graph is 
     * tried aginst that data graph vertex to instaniate a search.
     */
    void Execute(uint64_t init_v_s,
            d_array<MatchSet*>& match_results)
    {
/*
        // cout << "Calling execute()" << endl;
        // cout << "current_queue_len:" << current_queue->len << endl;
        // current_queue->Clear();
        // cout << "current_queue_len:" << current_queue->len << endl;
        // cout << "Clearing next queue" << endl;
        // next_queue->Clear();
        // cout << "InitSearch()" << endl;
        InitSearchV(init_v_s, current_queue);
        // cout << "Run()" << endl;
        Run(match_results);
        // cout << "Done!" << endl;
        return;
*/
        for (int i = 0, N = gQuery->NumVertices(); i < N; i++) {
#ifdef DEBUG
            LOGSRC << "Execute(v, matches)::Comparing v_q=" << i << " with v_s=" << init_v_s << endl;
#endif
            if (search_impl.MatchVertex(gQuery, gSearch, i, init_v_s)) {
                GraphQuery<Graph> query(gSearch, gQuery, i);
                SearchState<Graph>* state = new SearchState<Graph>(&search_input, 
                        i, init_v_s);
                query.current_queue->Clear();
                query.next_queue->Clear();
                query.current_queue->append(state); 
                query.Run(match_results);
            }
        }
        return;
    }

    /*
     * Pick a vertex in the query graph and find all matching vertices 
     * in the data graph.  Then initiate the search queue with all these
     * pairs.
     * NOTE: It currently iterates over all vertices in the data graph 
     * to initialize the search.
     * Combinations considered:  One v_q, Set of v_s.
     */
    void InitSearch(uint64_t init_v_q, 
            d_array<SearchState<Graph>* >* current_queue)
    {
        vector<uint64_t> init_v_s_list;;
        search_impl.GetMatchingVertexCandidates(gSearch, 
                gQuery, init_v_q, init_v_s_list);

        for (int i = 0, N = init_v_s_list.size(); i < N; i++) {
            SearchState<Graph>* state = new SearchState<Graph>(&search_input, 
                    init_v_q, init_v_s_list[i]);
            // cout << "init_v_q = " << init_v_q << endl;
            // cout << "init_v_s = " << init_v_s_list[i] << endl;
            current_queue->append(state); 
        }
        return;
    }

    /*
     * Initialize the search based on a pair of v_q and v_s.
     */
/*
    void InitSearch(uint64_t init_v_q, uint64_t init_v_s, 
                    d_array<SearchState<Graph>* >* current_queue)
    {
        if (search_impl.MatchVertex(gQuery, gSearch, init_v_q, init_v_s)) {
            SearchState<Graph>* state = new SearchState<Graph>(&search_input, 
                    init_v_q, init_v_s);
            current_queue->append(state); 
        }
        return;
    }
*/

    /* 
     * Select a vertex in the query graph (done in constructor).
     * Initialize two searches that begin from the source and destination
     * vertex of the edge passed as the first argument.
     * Combinations considered:  One v_q, two v_s.
     */
/*
    void InitSearch(uint64_t init_v_q, const dirE& e_s,
                    d_array<SearchState<Graph>* >* current_queue)
    {
        if (search_impl.MatchVertex(gQuery, gSearch, init_v_q, e_s.s)) {
            SearchState<Graph>* state = new SearchState<Graph>(&search_input, 
                    init_v_q, e_s.s);
            current_queue->append(state); 
        }

        if (search_impl.MatchVertex(gQuery, gSearch, init_v_q, e_s.t)) {
            SearchState<Graph>* state = new SearchState<Graph>(&search_input, 
                    init_v_q, e_s.t);
            current_queue->append(state); 
        }
        return;
    }

    void InitSearch(uint64_t init_v_s,
            d_array<SearchState<Graph>* >* current_queue)
    {
        for (int i = 0, N = gQuery->NumVertices(); i < N; i++) {
            // TODO The search order changes as we change init_v_q
            InitSearch(i, init_v_s, current_queue);
        }
    }
    
    void InitSearch(const dirE& e_s,
            d_array<SearchState<Graph>* >* current_queue)
    {
        for (int i = 0, N = gQuery->NumVertices(); i < N; i++) {
            // TODO The search order changes as we change init_v_q
            InitSearch(i, e_s, current_queue);
        }
    }
*/
    
    int Run(d_array<MatchSet* >& match_results, int walk_index = 0)
    {
        // int walk_index = 0;
        int num_matches = 0;
        dirE e_dummy;
        int search_order_len = graph_walk_order__.size();
#ifdef DEBUG
        LOGSRC << "Graph walk size = " << search_order_len << endl;
        LOGSRC << "Walk_index = " << walk_index << endl;
        LOGSRC << "Current queue size = " << current_queue->len << endl;
#endif
        
        while (walk_index < search_order_len && 
                walk_index < search_impl.TerminateSearch() == false && current_queue->len) {
            
            GraphWalkEntry<EdgeData>& query_graph_walk_entry = 
                    graph_walk_order__[walk_index++];
            uint64_t v_q = query_graph_walk_entry.u;
            const edge& e_q = query_graph_walk_entry.u_edge;
#ifdef DEBUG
            LOGSRC << "v_q = " << v_q << " label=[" << GetLabel(gQuery, v_q) << "]" << endl;
            LOGSRC << "next = " << e_q.dst << " label=[" << GetLabel(gQuery, e_q.dst) << "]" << endl;
#endif
            // PrintEdge(gQuery, e_q);
            dirE dir_e_q = MakeEdge(v_q, e_q);
#ifdef DEBUG
            LOGSRC << "Finding match for query edge = " << dir_e_q.s << " -> " << dir_e_q.t << endl;
#endif
            for (int i = 0, N = current_queue->len; i < N; i++) {
                
                SearchState<Graph>* state = (*current_queue)[i];
#ifdef DEBUG
                cout << "Trying to expand the following state:" << endl;
                state->Print();
#endif
                uint64_t v_s;
                state->GetMatchedVertex(true, v_q, v_s);
                // cout << "v_s = " << v_s << " label=[" << GetLabel(gSearch, v_s) << "]" << endl;
                int N_s;
                const edge* neighbors_s = gSearch->Neighbors(v_s, N_s);

#ifdef DEBUG
                LOGSRC << "     exploring " << N_s << " neighbors of v_s=" << v_s << endl;
#endif
                
                for (int j = 0; j < N_s; j++) {
                    
                    const edge& e_s = neighbors_s[j];
#ifdef VF2
                    /*if (VF2Check(gSearch, gQuery, e_s.dst, e_q.dst) == false) {
                      
                        continue;
                    }
                    */
#endif                    
#ifdef DEBUG
                    LOGSRC << "     candidates for e_q.dst[" << e_q.dst << "] = " << e_s.dst << endl;
#endif
                    dirE dir_e_s = MakeEdge(v_s, e_s);
#ifdef DEBUG
                    LOGSRC << "   Candidate = " << dir_e_s.s << " -> " << dir_e_s.t << endl;
#endif
                    // cout << "           Trying to match data edge = " << endl;
                    // PrintEdge(gSearch, e_s);
                    bool dup_check = state->GetMatchedEdge(false, dir_e_s, e_dummy);
                    bool edge_cmp = search_impl.MatchEdge(gQuery, gSearch, e_q, e_s);
                    bool constraint_check = search_impl.SatisfyConstraints(state, e_q, e_s);

                    // if (state->GetMatchedEdge(false, dir_e_s, e_dummy) == false &&
                        // search_impl.MatchEdge(gQuery, gSearch, e_q, e_s) && 
                        // search_impl.SatisfyConstraints(state, e_q, e_s)) {
                    if (dup_check == false && edge_cmp == true && 
                            constraint_check == true) {
#ifdef DEBUG
                        LOGSRC << "   Candidate match: OK" << endl; 
#endif
                        SearchState<Graph>* new_state = 
                                state->AddEdgeMapping(dir_e_q, dir_e_s);
                        
                        if (new_state->IsMatchComplete()) {
#ifdef DEBUG
                            LOGSRC << "FOUND FULL MATCH" << endl;
                            new_state->Print();
#endif
                            match_results.append(new_state->Export());
                            num_matches++;
                            delete new_state;
                        }
                        else {
                            next_queue->append(new_state);    
                        }
                    } 
#ifdef DEBUG
                    else {
                        LOGSRC << "   Candidate match: Failed" << endl; 
                        if (dup_check) {
                            LOGSRC << "       reason = duplicate_check_failed" << endl;
                        }
                        if (edge_cmp == false) {
                            LOGSRC << "       reason = edge_cmp_failed" << endl;
                        }
                        if (constraint_check == false) {
                            LOGSRC << "       reason = constraint_check_failed" << endl;
                        }
                    }
#endif
                }
                delete state;
                (*current_queue)[i] = NULL;
            }
            
            current_queue->Clear();
            swap(current_queue, next_queue);
            // search_impl.ReportProgress();
        }
        // next_queue->ReleaseAndClear();
        queue1.ReleaseAndClear();
        queue2.ReleaseAndClear();
        return num_matches;
    }

    void PrintInputSummary() 
    {
        LOGSRC << "|V(G_q)|= " << gQuery->NumVertices() 
               << " |E(G_q)|=" << gQuery->NumEdges() << endl;
        LOGSRC << "|V(G_d)|= " << gSearch->NumVertices() 
               << " |E(G_d)|=" << gSearch->NumEdges() << endl;
    }
    
private:
    const Graph* gSearch;
    const Graph* gQuery;
    DefaultSubgraphSearchImpl<Graph> search_impl;
    SearchInput<Graph> search_input;
    uint64_t init_v_q__;
    edge first_query_edge;
    vector<GraphWalkEntry<EdgeData> > graph_walk_order__;
    d_array<SearchState<Graph>* > queue1, queue2;
    d_array<SearchState<Graph>* > *current_queue, *next_queue;
};


#endif
