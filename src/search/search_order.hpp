#ifndef __SEARCH_ORDER_HPP__
#define __SEARCH_ORDER_HPP__
#include "graph.hpp"
typedef pair<uint64_t, uint64_t> vertex_pair;

template <typename EdgeData>
struct GraphWalkEntry {
    uint64_t u;
    Edge<EdgeData> u_edge;
    GraphWalkEntry(uint64_t v, const Edge<EdgeData>& v_edge):
        u(v), u_edge(v_edge) {}
};

template <typename VertexData, typename EdgeData>
void __GenerateSearchOrder(const Graph<VertexData, EdgeData>* g, 
        set<pair<uint64_t, uint64_t> >& visited_edges, 
        queue<uint64_t>& visit_queue,
        vector<GraphWalkEntry<EdgeData> >& walk_order)
{
    set<uint64_t> visited_nodes;

    while (visit_queue.size()) {
        uint64_t v = visit_queue.front();
        visit_queue.pop();

        int neighbor_count;
        const Edge<EdgeData>* neighbors =
                g->Neighbors(v, neighbor_count);

        visited_nodes.insert(v);

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

            set<vertex_pair>::iterator it = visited_edges.find(v_pair);

            if (it != visited_edges.end()) continue;

            visited_edges.insert(v_pair);
            // cout << "walk order = " << v << " -- " << e.dst << endl;
            walk_order.push_back(GraphWalkEntry<EdgeData>(v, e));

            set<uint64_t>::iterator v_it = visited_nodes.find(e.dst);
            if (v_it == visited_nodes.end()) {
                visit_queue.push(e.dst);
            }
        }
    }

    return;
}

/*
template <typename VertexData, typename EdgeData>
void __GenerateSearchOrder(const Graph<VertexData, EdgeData>* g, 
        set<pair<uint64_t, uint64_t> >& visited, 
        queue<pair<uint64_t, uint64_t> >& visit_queue,
        vector<GraphWalkEntry<EdgeData> >& walk_order)
{
    typedef pair<uint64_t, uint64_t> vertex_pair;

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

template <typename VertexData, typename EdgeData>
void GenerateSearchOrder(const Graph<VertexData, EdgeData>* g,
        uint64_t init_v, 
        vector<GraphWalkEntry<EdgeData> >& walk_order)
{
    queue<uint64_t> visit_queue;
    set<pair<uint64_t, uint64_t> > visited_edges;
   
    visit_queue.push(init_v);
 
    __GenerateSearchOrder(g, visited_edges, visit_queue, walk_order);
    assert(walk_order.size());
    return;
}
        
template <typename VertexData, typename EdgeData>
void GenerateSearchOrder(const Graph<VertexData, EdgeData>* g,
        uint64_t u, uint64_t v, 
        vector<GraphWalkEntry<EdgeData> >& walk_order)
{
    // queue<pair<uint64_t, uint64_t> > visit_queue;
    queue<uint64_t> visit_queue;
    set<pair<uint64_t, uint64_t> > visited_edges;

    if (u > v) {
        swap(u, v);
    }
    
    visited_edges.insert(pair<uint64_t, uint64_t>(u, v));
    visit_queue.push(u);
    visit_queue.push(v);
    
    Edge<EdgeData> v_edge;
    int u_neighbor_count;
    const Edge<EdgeData>* edges = g->Neighbors(u, 
            u_neighbor_count);
    bool found_edge = false;
    for (int i = 0; i < u_neighbor_count; i++) {    
        if (edges[i].dst == v) {
            v_edge = edges[i];
            found_edge = true;
            break;
        }
    }
    assert(found_edge);
    walk_order.push_back(GraphWalkEntry<EdgeData>(u, v_edge));

    __GenerateSearchOrder(g, visited_edges, visit_queue, walk_order);
    assert(g->NumEdges() == walk_order.size());
    return;
}
        
/*
typedef vector<GraphWalkEntry<EdgeData> > SearchOrder;

template <class Graph>
class SearchOrderIndex {
public:
    typedef typename Graph::EdgeDataType EdgeData;
    typedef typename d_array<SearchOrder*> SearchOrderList;

    SearchOrderIndex(const Graph* gQuery)
    {
        this->gQuery = gQuery;
        int num_nodes__ = gQuery->NumVertices();
        int num_edges__ = gQuery->NumEdges();
        node_index__ = new SearchOrderList[num_nodes__];
        edge_index__ = new SearchOrderList[num_nodes__*num_nodes__];
        return;
    }

    ~SearchOrderIndex()
    {
        for (int i = 0; i < num_nodes__; i++) {
            SearchOrderList& list = GetSearchOrderLists(i);
            for (int j = 0; j < list.len; j++) {
                delete list[j];
            }
        }
        delete[] node_index__;
        delete[] edge_index__;
    }

    void GetSearchOrderList(uint64_t u, SearchOrderList& list)
    {
        list = node_index__[u];
        return;
    }

    void GetSearchOrderList(uint64_t u, uint64_t v, 
            SearchOrderList& list)
    {
        if (u > v) {
            swap(u, v);
        }

        list = edge_index__[u*num_nodes__ + v];
        return;
    }

    void BuildIndex()
    {
        for (int u = 0, num_vertices = gQuery->NumVertices();
                u < num_vertices; u++) {
            int neighbor_count;
            Edge<EdgeData>* neighbors = gQuery->Neighbors(u,
                    neighbor_count);
            for (int j = 0; j < neighbor_count; j++) {
                uint64_t v = neighbors[j].dst;
                if (u >= v) {
                    continue;
                }
                SearchOrderList search_order_list;
                GenerateSearchOrders(u, v, 
                        search_order_list);
                UpdateNodeIndex(u, search_order_list);
                UpdateNodeIndex(v, search_order_list);
                UpdateEdgeIndex(u, v, search_order_list);
            }
        }
        return;
    }

protected:

    void GenerateSearchOrders(uint64_t u, uint64_t v, 
            SearchOrderList& search_order_list)
    {
    }

    void UpdateNodeIndex(uint64_t u, const SearchOrderList& list)
    {
        SearchOrderList& node_list = node_index[u];
        for (int i = 0, N = list.len; i < N; i++) {
            node_list.append(list[i]);
        }
        return;
    }

    void UpdateEdgeIndex(uint64_t u, uint64_t v, 
            const SearchOrderList& list)
    {
        SearchOrderList& edge_list = edge_index__[u*num_nodes + v];
        for (int i = 0, N = list.len; i < N; i++) {
            edge_list.append(list[i]);
        }
        return;
    }

private:
    const Graph* gQuery;
    d_array<SearchOrder* > search_order_array__;
    SearchOrderList* node_index__;
    SearchOrderList* edge_index__;
    int num_nodes__;
    int num_edges__;
};
*/

#endif
