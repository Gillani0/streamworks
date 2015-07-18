#ifndef __GRAPH_HPP__
#define __GRAPH_HPP__

/**
 * @author Sutanay Choudhury
 * @author Patrick Mackey
 */

#include <stdlib.h>
#include <iostream>
#include <sstream>
#include <assert.h>
#include "edge.hpp"
#include "vertex.hpp"
using namespace std;

// Original value: 1
#define DEFAULT_ADJ_LIST_SIZE 2
// Original value: 16
#define DEFAULT_VERTEX_COUNT 128

/** Stores a Graph of vertices and edges and their associated metadata */
template <class VertexData = Label, class EdgeData = UnLabeledEdge> 
class Graph {
public:
    typedef VertexData VertexDataType;
    typedef EdgeData EdgeDataType;
    typedef VertexProperty<VertexData> VertexProp;
    typedef Edge<EdgeData> EdgeType;
    typedef DirectedEdge<EdgeData> DirectedEdgeType;
    /** 
      * Initialize a graph with DEFAULT_VERTEX_COUNT long vertex_properties array.
      * All adjacency lists are set to NULL.
      */
    Graph(int allocated_vertex_count = DEFAULT_VERTEX_COUNT,
            bool is_directed = true) : 
            is_directed__(is_directed), 
            //num_vertices__(0),
            vertex_count__(0),
            num_vertices_with_edges__(0),
            //num_edges__(0),
            num_total_edges__(0),
            allocated_vertex_count__(allocated_vertex_count)//,
            //is_neighbor_allocated__(false)
    {
        // cout << "Allocating vprops for " << allocated_vertex_count__ << endl;
#ifdef __MTA__
#pragma mta assert par_newdelete
#endif
        vertex_properties__ = new VertexProperty<VertexData>[allocated_vertex_count__];
        allocated_neighbor_counts__ = new int[allocated_vertex_count__];
        neighbor_counts__ = new int[allocated_vertex_count__];
        neighbors__ = new Edge<EdgeData>*[allocated_vertex_count__];
        // Fill counts to 0
        for(int i=0; i<allocated_vertex_count__; i++)
        {
            allocated_neighbor_counts__[i] = 0;
            neighbor_counts__[i] = 0;
            neighbors__[i] = NULL;
        } 
        
        //neighbor_counts__ = NULL;
        //allocated_neighbor_counts__ = NULL;
        //neighbors__ = NULL;
    }
    
    /**
     * Release all memory used to store vertex metadata and adjacency lists.
     */
    ~Graph()
    {
        if (vertex_properties__) {
#ifdef __MTA__
#pragma mta assert par_newdelete
#endif
            delete[] vertex_properties__;
        }
        
        if (neighbor_counts__) {
            delete[] neighbor_counts__;
        }
        
        if (allocated_neighbor_counts__) {
            delete[] allocated_neighbor_counts__;
        }
                
        if (neighbors__) {
#ifdef __MTA__
#pragma mta assert parallel
#endif
            //for (int i = 0, N = num_vertices__; i < N; i++) {
            for (int i = 0, N = vertex_count__; i < N; i++) {
                if (neighbors__[i]) {
// #ifdef __MTA__
// #pragma mta assert par_newdelete
// #endif
                    delete[] neighbors__[i];
                }
            }
            delete[] neighbors__;
        }
        return;
    }
    
    /** Adds a vertex to our graph, but without reallocating the size
        if exceeding the amount allocated.  Not recommended unless you
        know in advance how many vertices you will use and have already
        allocated the proper amount of memory.
        @param v    The vertex index, starting at 0
        @param properties   The associated properties of the vertex */
    void AddVertexUnchecked(int v, const VertexProperty<VertexData>& properties)
    {
        // assert(v < allocated_vertex_count__);
        vertex_properties__[v] = properties;
#ifdef __MTA__
        int_fetch_add(&num_vertices__, 1);        
#else 
        //num_vertices__ += 1;
        if(v >= vertex_count__)
            vertex_count__ = v+1;
#endif
        return;
    }
    
    /** Adds a vertex to our graph.  Reallocates memory as needed.
    @param v    The vertex index, starting at 0
    @param properties   The associated properties of the vertex */
    void AddVertex(const Vertex<VertexData> &vertex)
    {
        this->AddVertex(vertex.vertex, vertex.vertex_property);
    }

    /** Adds a vertex to our graph.  Reallocates memory as needed.
        @param v    The vertex index, starting at 0
        @param properties   The associated properties of the vertex */
    void AddVertex(int v, const VertexProperty<VertexData>& properties)
    {
        //cout << "Adding vertex" << endl;
#ifdef __MTA__
        vertex_properties__[v] = properties;
        int_fetch_add(&num_vertices__, 1);
        return;
        int allocated_vertex_count = readfe(&allocated_vertex_count__);
#else
        int allocated_vertex_count = allocated_vertex_count__;
#endif
        /* Beginning of the block for reallocation of the vertex properties */
        if (v >= allocated_vertex_count) {      
            //cout << "Reallocating memory" << endl;      
            VertexProperty<VertexData>* old_vertex_properties = vertex_properties__;
            int *old_allocated_neighbor_counts = allocated_neighbor_counts__;
            int *old_neighbor_counts = neighbor_counts__;
            Edge<EdgeData> **old_neighbors = neighbors__;
            uint64_t old_size = allocated_vertex_count;
            
            // P.Mackey: Changing this so that it should be a little
            // more efficient.
            uint64_t new_size = v*2;
            if(new_size < DEFAULT_VERTEX_COUNT)
                new_size = DEFAULT_VERTEX_COUNT;
            /*
            long new_size = 1;
            while (new_size <= v) {
                new_size <<= 1;
            }*/
            
#ifdef __MTA__
#pragma mta assert par_newdelete
#endif
            vertex_properties__ = new (nothrow) VertexProperty<VertexData>[new_size];
            allocated_neighbor_counts__ = new (nothrow) int[new_size];            
            neighbor_counts__ = new (nothrow) int[new_size];
            neighbors__ = new (nothrow) Edge<EdgeData>*[new_size];
            if (vertex_properties__ == NULL || allocated_neighbor_counts__ == NULL
               || neighbor_counts__ == NULL || neighbors__ == NULL) 
            {
                cerr << "Failed to reallocate vertex array of size = " 
                    << new_size << endl;
                exit(1);
            }
            
            allocated_vertex_count = new_size;
                        
#ifdef __MTA__
#pragma mta assert parallel
#endif
            for (int i = 0; i < old_size; i++) {
                vertex_properties__[i] = old_vertex_properties[i];
                allocated_neighbor_counts__[i] = old_allocated_neighbor_counts[i];
                neighbor_counts__[i] = old_neighbor_counts[i];
                neighbors__[i] = old_neighbors[i];                
            }
            for(int i=old_size; i<new_size; i++) {
                allocated_neighbor_counts__[i] = 0;
                neighbor_counts__[i] = 0;
                neighbors__[i] = NULL;
            }
            
#ifdef __MTA__
#pragma mta assert par_newdelete
#endif
            delete[] old_vertex_properties;
            delete[] old_allocated_neighbor_counts;
            delete[] old_neighbor_counts;
            delete[] old_neighbors;
        }
        
        /* End of vertex_properties__ reallocation */
        
#ifdef __MTA__
        writeef(&allocated_vertex_count__, allocated_vertex_count);
        int prev_count = readfe(&num_vertices__);
        vertex_properties__[v] = properties;
        writeef(&num_vertices__, prev_count+1);        
#else
        allocated_vertex_count__ = allocated_vertex_count;
        vertex_properties__[v] = properties;
        //num_vertices__++;
        if(v >= vertex_count__)
            vertex_count__ = v+1;
#endif
        
        return;
    }
    
    /** @return Returns the amount of memory allocated for the
        adjacency list for vertex u */
    inline int GetAllocatedListSize(int u) const 
    {
        return allocated_neighbor_counts__[u];
    }

    /** Allocates memory for adcancey list for the given vertex u.
        @param u    Vertex we wish to allocate adjcacency list for
        @param request_len  Size for allocation
        @deprecated Does nothing now! Allocation taken care of automatically. */
    void AllocateAdjacencyList(uint64_t u, int request_len)
    {
        // Do nothing
    }
    /*{
        if (request_len < allocated_neighbor_counts__[u]) {
            return;
        } 

        int alloc_len = GetReallocListSize(request_len);

        Edge<EdgeData>* old_list = neighbors__[u];
#ifdef __MTA__
#pragma mta assert par_newdelete
#endif
        neighbors__[u] = new Edge<EdgeData>[alloc_len];
        // neighbors__[u] = new Edge<EdgeData>[2*u_neighbor_alloced];
        if (neighbors__[u] == NULL) {
            cerr << "Failed to resize neighbor list with " 
                << (alloc_len) << " elements" << endl;
                exit(1);
        }
        #pragma mta assert parallel
        for (int i = 0, N = neighbor_counts__[u]; i < N; i++) {
            neighbors__[u][i] = old_list[i];            
        }
        allocated_neighbor_counts__[u] = alloc_len;
#ifdef __MTA__
#pragma mta assert par_newdelete
#endif
        delete[] old_list;
        return;
}*/

    /** Allocates memory for the adjacency lists of all the vertices 
        @deprecated Does nothing now! Allocation taken care of automatically. */
    void AllocateAdjacencyLists()
    {
        // Do nothing
    }
    /*{
        //neighbors__ = new Edge<EdgeData>*[num_vertices__];
        //allocated_neighbor_counts__ = new int[num_vertices__];
        //neighbor_counts__ = new int[num_vertices__];
        neighbors__ = new Edge<EdgeData>*[vertex_count__];
        allocated_neighbor_counts__ = new int[vertex_count__];
        neighbor_counts__ = new int[vertex_count__];
        
        if (!neighbors__ || !allocated_neighbor_counts__ || !neighbor_counts__) {
            cerr << "AllocateAdjacencyLists() failed for "
                 << vertex_count__ << " vertices" << endl;
            exit(1);
        }
        
        // cout << "Setting allocated neighbor counts" << endl;
        #pragma mta assert parallel
        //for (int i = 0, N = num_vertices__; i < N; i++) {
        for (int i = 0, N = vertex_count__; i < N; i++) {
            #ifdef __MTA__
                writexf(allocated_neighbor_counts__ + i, 
                        DEFAULT_ADJ_LIST_SIZE);
                writexf(neighbor_counts__ + i, 0);
            #else
                allocated_neighbor_counts__[i] = DEFAULT_ADJ_LIST_SIZE;
                neighbor_counts__[i] = 0;
            #endif
        }

        // cout << "Setting up adjacency lists for=" << num_vertices__ << endl;
        #pragma mta assert parallel
        //for (int i = 0, N = num_vertices__; i < N; i++) {
        for (int i = 0, N = vertex_count__; i < N; i++) {
// #ifdef __MTA__
// #pragma mta assert par_newdelete
// #endif
            neighbors__[i] = new Edge<EdgeData>[DEFAULT_ADJ_LIST_SIZE];
        }
        // cout << "Done setting up adjacency lists for=" << num_vertices__ << endl;

        
        // allocated_edge_count__ = num_vertices__;
        is_neighbor_allocated__ = true;
        return;
    }*/
    
    /** @return Returns true if adjacency lists have been allocated for the vertices 
        @deprecated Always returns true. */    
    inline bool IsAdjacencyListAllocated() { return true; } 
    //{ return is_neighbor_allocated__; }

    /** Adds edge to the graph, but does not reallocate extra memory for adjacency lists.
        Not recommended unless you already have allocated the amount you know you'll need. 
        @param edge     New edge added to graph */
    void AddEdgeWithoutRealloc(const DirectedEdge<EdgeData>& edge)
    {        
        // Update first and last date/times
        if(edge.timestamp < first_date__ || num_total_edges__ == 0)
        {
            first_date__ = edge.timestamp;
            first_date_ms__ = edge.timestamp_ms;
        }
        else if(edge.timestamp == first_date__ && edge.timestamp_ms < first_date_ms__)
            first_date_ms__ = edge.timestamp_ms;
        if(edge.timestamp > last_date__ || num_total_edges__ == 0)
        {
            last_date__ = edge.timestamp;
            last_date_ms__ = edge.timestamp_ms;
        }
        else if(edge.timestamp == last_date__ && edge.timestamp_ms > last_date_ms__)
            last_date_ms__ = edge.timestamp_ms;
        
        
        Edge<EdgeData> in_edge(edge, false);
        Edge<EdgeData> out_edge(edge, true);
        __AddEdgeWithoutRealloc(edge.s, out_edge);
        __AddEdgeWithoutRealloc(edge.t, in_edge);
#ifdef __MTA__
        //int_fetch_add(&num_edges__, 1);
        int_fetch_add(&num_total_edges__, 2);
#else
        //num_edges__++;
        num_total_edges__+=2;
#endif
    }

    /** Adds edge to the graph. Recommended method.
        Reallocates memory if needed for adjcacency lists. 
        @param edge     New edge added to graph */
    void AddEdge(const DirectedEdge<EdgeData>& edge)
    {        
        assert(edge.timestamp >= 0);
        assert(edge.type >= 0);
        // Update first and last date/times
        if(edge.timestamp < first_date__ || num_total_edges__ == 0)
        {
            first_date__ = edge.timestamp;
            first_date_ms__ = edge.timestamp_ms;
        }
        else if(edge.timestamp == first_date__ && 
                edge.timestamp_ms < first_date_ms__)
            first_date_ms__ = edge.timestamp_ms;
        if(edge.timestamp > last_date__ || num_total_edges__ == 0)
        {
            last_date__ = edge.timestamp;
            last_date_ms__ = edge.timestamp_ms;
        }
        else if(edge.timestamp == last_date__ && 
                edge.timestamp_ms > last_date_ms__)
            last_date_ms__ = edge.timestamp_ms;
        
        /*if (is_neighbor_allocated__ == false) {
            AllocateAdjacencyLists();
        }*/
        
        Edge<EdgeData> in_edge(edge, false);
        in_edge.id = edge.id;
        Edge<EdgeData> out_edge(edge, true);
        out_edge.id = edge.id;
        
        __AddEdge(edge.s, out_edge);
        __AddEdge(edge.t, in_edge);
#ifdef __MTA__
        //int_fetch_add(&num_edges__, 1);
        int_fetch_add(&num_total_edges__, 2);
#else
        //num_edges__++;
        num_total_edges__+=2;
#endif
        
        // if (!is_directed__) {
        // __AddEdge(edge);
        // }    
        return;
    }    

    /** Removes edge from graph. Does not affect memory allocated.
        Does not update first/last dates.
        @param dir_edge     DirectedEdge to remove. Removes Edges from both source and target nodes. */
    bool RemoveEdge(const DirectedEdge<EdgeData>& dir_edge,
            bool ignore_edge_type = false)
    {
        Edge<EdgeData> e_1(dir_edge, true);
        Edge<EdgeData> e_2(dir_edge, false);
        bool stat1 = RemoveEdge(dir_edge.s, e_1, ignore_edge_type);
        bool stat2 = RemoveEdge(dir_edge.t, e_2, ignore_edge_type);
        
        // If this edge was at the beginning or end of our time/date
        // make sure the first/last dates reflect the data.
        //if(dir_edge.timestamp == last_date__ || dir_edge.timestamp == first_date__)
            //UpdateFirstLastDates();
        
        return (stat1 && stat2);
    }

    /** Removes edge from graph. Does not affect memory allocated.    
        Does not update first/last dates.
        @param u       The vertex containing the edge
        @param rem_edge The Edge to remove. */
    bool RemoveEdge(uint64_t u, const Edge<EdgeData>& rem_edge,
            bool ignore_edge_type = false) 
    {   
        //cout << "Attempting to remove " << u << " - " << rem_edge.dst << endl;
        int num_edges(0);
        // num_edges gets set with the number of edges connected to u
        Edge<EdgeData>* edges = Neighbors(u, num_edges);
        bool copy_edges(false);

        for (int i = 0; i < num_edges; i++) {
            if (copy_edges) {
                //edges[i] = edges[i-1];
                edges[i-1] = edges[i];
            }   
            else if (ignore_edge_type 
                    && edges[i].dst == rem_edge.dst) {
                copy_edges = true;
            }
            else if (edges[i] == rem_edge) {
                copy_edges = true;
            }   
        }   

        // Only reduce counts if we actually found our edge
        if(copy_edges)
        {
            neighbor_counts__[u] -= 1;
    
            if (neighbor_counts__[u] == 0) {
                //num_vertices__--;
                num_vertices_with_edges__--;
            }
            num_total_edges__--;
        }
        else
        {
            cout << "ERROR1: Could not find the given edge to remove " 
                 << u << " - " << rem_edge.dst << endl;
            exit(1);
        }  
         
        // If this edge was at the beginning or end of our time/date
        // make sure the first/last dates reflect the data.
        //if(rem_edge.timestamp == last_date__ || rem_edge.timestamp == first_date__)
            //UpdateFirstLastDates();
    
        return copy_edges;
    }   
    
    /** Removes all edges from graph with a timestamp before the given time.
        @param timestamp    Minimum timestamp for edges we keep */
    int RemoveEdgesBefore(time_t timestamp)
    {
        int removed_edges = 0;
        for(uint64_t u=0; u<vertex_count__; u++)
        {
            int num_edges(0);
            // num_edges gets set with the number of edges connected to u
            Edge<EdgeData>* edges = Neighbors(u, num_edges);
            
            int new_neighbor_count = neighbor_counts__[u];
            int copyIndex = 0;
            // Loop through edges and delete those older than our timestamp
            for(int i=0; i<num_edges; i++)
            {
                // If older than our timestamp, skip it when copying our edges
                if(edges[i].timestamp < timestamp)
                {
                    removed_edges++;
                    new_neighbor_count--;
                    num_total_edges__--;
                }
                // Otherwise copy the edge to the next position in our new list
                else
                {
                    edges[copyIndex] = edges[i];
                    copyIndex++;
                }
            }
            // If no more neighbors, we can reduce the count of verts with edges
            if(new_neighbor_count == 0)
                num_vertices_with_edges__--;
            // Error checking
            if(new_neighbor_count < 0)
            {
                cerr << "ERROR: Neighbor count should never be less than 0: " << new_neighbor_count << endl;
                exit(EXIT_FAILURE);
            }
            // Set the new neighbor count
            neighbor_counts__[u] = new_neighbor_count;
        } 
                
        // Update earliest date/time
        first_date__ = timestamp;
        first_date_ms__ = 0;
        return removed_edges;
    }


    /** Return the number of vertices in the graph. 
        This includes vertices with indexes that weren't specifically added,
        but are lower than vertices indexes that had been already added. 
        It also includes vertices without edges. */
    inline uint64_t NumVertices() const { return vertex_count__; /*num_vertices__;*/ }
    
    /** Return the number of vertices in the graph that have edges. */
    inline uint64_t NumVerticesWithEdges() const { return num_vertices_with_edges__; }

    /** Return the number of edges in the graph. 
        Note: This returns half the number of actual Edge objects
        in our graph, because it counts the Edges going in both
        directions as one "edge".  We might want to revisit this. */
    inline uint64_t NumEdges() const 
    { 
        //return num_edges__;
        // Divide the num of total edges by 2 (using bit shifting) to
        // make the count match the definition used originally for this class
        return num_total_edges__ >> 1; 
    }

    /** Get the metadata associated with vertex u. */
    inline const VertexProperty<VertexData>&  GetVertexProperty(uint64_t u) const
    {   
        return vertex_properties__[u];
    }   

    /** Read-only access to the adjacency list of vertex u.
        @param u    The vertex we want the neighbors from
        @param neighbor_count   An output variable that is filled with the number of neighbors */
    inline const Edge<EdgeData>* Neighbors(uint64_t u, int& neighbor_count) const
    {   
        //if (is_neighbor_allocated__ == false || u >=  num_vertices__) {
        //if (is_neighbor_allocated__ == false || u >= vertex_count__) {
        if(u >= vertex_count__) {
            neighbor_count = 0;
            return NULL;
        }   
        neighbor_count = neighbor_counts__[u];
        return neighbors__[u];
    }  

    /** Read-write access to the adjacency list of vertex u. 
        @param u    The vertex we want the neighbors from
        @param neighbor_count   An output variable that is filled with the number of neighbors */
    inline Edge<EdgeData>* Neighbors(uint64_t u, int& neighbor_count) 
    {   
        //if (is_neighbor_allocated__ == false || u >= num_vertices__) {
        //if (is_neighbor_allocated__ == false || u >= vertex_count__) {
        if(u >= vertex_count__) {
            neighbor_count = 0;
            return NULL;
        }   
        neighbor_count = neighbor_counts__[u];
        return neighbors__[u];
    }   

    /** Get the number of neighbors of vertex u. */
    inline int NeighborCount(uint64_t u) const 
    {   
        //if (is_neighbor_allocated__ == false || u >= num_vertices__) {
        //if (is_neighbor_allocated__ == false || u >= vertex_count__) {
        if(u >= vertex_count__) {
            return 0;
        }   
        return neighbor_counts__[u];
    }   

    /** Function to determine if there is any edge between two vertices. 
        @param type     Output variable filled with the type of the edge 
                        (if it exists) between the two vertices. If more
                        than one edge exists, it's set to the first one found. */
    bool isNeighbor(uint64_t u, uint64_t v, int& type) const
    {
        int u_count = neighbor_counts__[u];
        int v_count = neighbor_counts__[v];
        Edge<EdgeData>* walk_list; 
        int walk_len, query_vertex;
        
        if (u_count < v_count) {
            walk_list = neighbors__[u];    
            walk_len = u_count;
            query_vertex = v;
        }
        else {
            walk_list = neighbors__[v];    
            walk_len = v_count;
            query_vertex = u;
        }
        
        bool found = false;
        
        for (int i = 0; i < walk_len; i++) {
            if (walk_list[i].dst == query_vertex) {
                type = walk_list[i].type;
                found = true;
                break;
            }
        }
        
        return found;
    }
    
    //Find a  node without Incoming Edges, if no such edge exists 
    //=> cyclic graph return 0;
    uint64_t getARootNode() const{
        
      uint64_t neighbour_count;
      const uint64_t vertex_count =  vertex_count__;  
      int indegree;

      // Neighbours[vertex] stores both incoming and outgoing edges for
      // vertex
        for (int i = 0; i < vertex_count; i++) {
            neighbour_count = neighbor_counts__[i];
            indegree =0;
            for (int j = 0; j < neighbour_count; j++) {
                if(!(neighbors__[i][j].outgoing)){
                  indegree++;
                }
            }
            if(indegree == 0) return i;
        }
        return 0;
    }
    
    /** @return The earliest date/time of an edge record in the graph. */
    time_t GetFirstDate() const
    {
        return first_date__;
    }
    /** @return The number of milliseconds the earliest date/time of an edge record appears after the first date. */
    int GetFirstDateMS() const
    {
        return first_date_ms__;
    }    
    /** @return The most recent date/time of an edge record in the graph. */
    time_t GetLastDate() const
    {
        return last_date__;
    }
    /** @return The number of milliseconds the earliest date/time of an edge record appears after the last date. */
    int GetLastDateMS() const
    {
        return last_date_ms__;
    }    

    Graph<VertexData, EdgeData>*
    Clone() const
    {
        Graph<VertexData, EdgeData>* g = new Graph<VertexData, EdgeData>(allocated_vertex_count__);
    
        g->is_directed__ = is_directed__;
        //g->num_vertices__ =  num_vertices__;
        g->vertex_count__ = vertex_count__;
        g->num_vertices_with_edges__ = num_vertices_with_edges__;
        //g->num_edges__ = num_edges__;
        g->num_total_edges__ = num_total_edges__;
        g->allocated_vertex_count__ = allocated_vertex_count__;
        //g->is_neighbor_allocated__ = is_neighbor_allocated__;

/*
        g->vertex_properties__ = 
                new VertexProperty<VertexData>[allocated_vertex_count__];
        g->allocated_neighbor_counts__ = 
                new int[allocated_vertex_count__];
        g->neighbor_counts__ = 
                new int[allocated_vertex_count__];
        g->neighbors__ = new Edge<EdgeData>*[allocated_vertex_count__];
*/

        for (int i = 0, N = allocated_vertex_count__; i < N; i++) {
            g->vertex_properties__[i] = vertex_properties__[i];
            g->allocated_neighbor_counts__[i] = 
                    allocated_neighbor_counts__[i];
            g->neighbor_counts__[i] = neighbor_counts__[i]; 
        }

        for (int i = 0, M = allocated_vertex_count__; i < M; i++) {
            g->neighbors__[i] = 
                new Edge<EdgeData>[allocated_neighbor_counts__[i]];
            for (int j = 0, N = neighbor_counts__[i]; j < N; j++) {
                g->neighbors__[i][j] = neighbors__[i][j]; 
            } 
        }

        return g;
    }

    string String() const
    {   
        stringstream strm;
        for (int i = 0; i < vertex_count__; i++) {
            int len;
            const Edge<EdgeData>* i_nbrs = Neighbors(i, len);
            for (int j = 0; j < len; j++) {
                if (i_nbrs[j].outgoing) {
                    strm << i << "_" << i_nbrs[j].type <<  "_" << i_nbrs[j].dst << ";";
                }   
            }   
        }   
        return strm.str();
    }   

    void Print() const
    {
        cout << "+++++++++++++++++++++++++" << endl;
        for (int i = 0; i < vertex_count__; i++) {
            int len;
            const Edge<EdgeData>* i_nbrs = Neighbors(i, len);
            for (int j = 0; j < len; j++) {
                if (i_nbrs[j].outgoing) {
                    cout << i << " -> " << i_nbrs[j].dst 
                         << " type(" << i_nbrs[j].type << ")" << endl;
                }
            }
        }
        cout << "-------------------------" << endl;
    }

protected:
    
    /** @return Returns the preferred new size to allocate for an adjacency list */
    inline int GetReallocListSize(int len)
    {
        int newlen = 1;
        while (newlen <= len) {
            if (newlen < 1024) {
                newlen <<= 1;
            }
            else {
                newlen += 1024;
            }
        }
        return newlen;
    }

    void __AddEdge(uint64_t u, const Edge<EdgeData>& edge)
    {
        //cout << "Adding edge " << u << " - " << edge.dst << endl;
        
        // Add vertices if not already in the graph
        if(u >= vertex_count__)
            this->AddVertex(u, VertexProperty<VertexData>());
        if(edge.dst >= vertex_count__)
            this->AddVertex(edge.dst, VertexProperty<VertexData>());
        
        //cout << "Adding neighbor to " << u << endl;
#ifdef __MTA__
        int u_neighbor_count = readfe(neighbor_counts__ + u);
        // cout << "U neighbor count = " << u_neighbor_count << endl;
#else
        int u_neighbor_count = neighbor_counts__[u];
        //cout << "Got u_neighbor_count: " << u_neighbor_count << endl;
#endif
        //cout << "u_neighbor_count = " << u_neighbor_count << endl;
        if(u_neighbor_count == 0)
            num_vertices_with_edges__++;
        //cout << "Getting u_neighbor_alloced" << endl;
        int u_neighbor_alloced = allocated_neighbor_counts__[u];
        //cout << "u_neighbor_alloced = " << u_neighbor_alloced << endl;
        //cout << "Got u_neighbor_alloced: " << u_neighbor_alloced << endl;
        
        //cout << "Checking count vs alloced: " << u_neighbor_count << " vs " << u_neighbor_alloced << endl;
        if(u_neighbor_count > u_neighbor_alloced)
        {
            cerr << "ERROR: the neighbor count for vertex " << u << " is " << u_neighbor_count
                    << " which is greater than the amount allocated: " << u_neighbor_alloced << endl;
            exit(EXIT_FAILURE);
        }
        if (u_neighbor_count == u_neighbor_alloced) {
            //cout << "Need to resize adj list" << endl;
            Edge<EdgeData>* old_list = neighbors__[u];
#ifdef __MTA__
#pragma mta assert par_newdelete
#endif
            int new_nbr_alloc = 2*u_neighbor_alloced;
            if(new_nbr_alloc < DEFAULT_ADJ_LIST_SIZE)
                new_nbr_alloc = DEFAULT_ADJ_LIST_SIZE;
            neighbors__[u] = new (nothrow) Edge<EdgeData>[new_nbr_alloc];
            if (neighbors__[u] == NULL) {
                cerr << "Failed to resize neighbor list with " 
                << new_nbr_alloc << " elements" << endl;
                exit(EXIT_FAILURE);
            }
            #pragma mta assert parallel
            for (int i = 0; i < u_neighbor_count; i++) {
                neighbors__[u][i] = old_list[i];            
            }
            allocated_neighbor_counts__[u] = new_nbr_alloc;
            //cout << "New allocated count for " << u << " = " << allocated_neighbor_counts__[u] << endl;
#ifdef __MTA__
#pragma mta assert par_newdelete
#endif
            delete[] old_list;
        }

        //cout << "Getting neighbors" << endl;
        Edge<EdgeData>* u_neighbors = neighbors__[u];
        //cout << "Adding edge to neighbors" << endl;
        neighbors__[u][u_neighbor_count] = edge;
        u_neighbor_count++;
        //cout << "Setting new neighbor count" << endl;
#ifdef __MTA__
        writeef(neighbor_counts__ + u, u_neighbor_count);
#else
        neighbor_counts__[u] = u_neighbor_count; 
#endif
        //cout << "Done" << endl;
        return;
    }

    void __AddEdgeWithoutRealloc(uint64_t u, const Edge<EdgeData>& edge)
    {
        int next = 0;
        #ifdef __MTA__
            next = int_fetch_add(neighbor_counts__ + u, 1);
        #else
            next = neighbor_counts__[u];        
            if(next == 0)
                num_vertices_with_edges__++;
            neighbor_counts__[u] = next + 1;
        #endif
        // assert(next < allocated_neighbor_counts__[u]);
        if (next >= allocated_neighbor_counts__[u]) {
            cout << "u = " << u << " Next = " << next << " Alloc = " << allocated_neighbor_counts__[u] << endl;
            return;
        }
        neighbors__[u][next] = edge;
        return;
    }    


private:
    Graph(const Graph& graph);

    /** Boolean flag indicating Directed/Undirected nature of the graph. */
    bool            is_directed__;

    /** Array of vertex metadata structures */
    VertexProperty<VertexData>* vertex_properties__;

    /** Adjacency list based storage of edges in the graph. */
    Edge<EdgeData>**          neighbors__;

    /** Array storing the number of neighbors for each vertex. 
        The actual array length may be bigger and is stored in allocated_neighbor_counts__ */
    int*            neighbor_counts__;

    // /** Number of vertices in the graph. */
    //uint64_t        num_vertices__;
    /** Number of vertices in the graph. 
        Assumes vertices are contiguous from 0 to N.
        E.g., Adding a vertex with the index 10000 will increase this to 10001. */
    uint64_t        vertex_count__;
    
    /** Number of vertices with edges. */
    uint64_t        num_vertices_with_edges__;

    // /** Number of edges in the graph. */
    //uint64_t        num_edges__;
    /** Number of edges in the graph, count each edge going in both directions 
        (This is necessary to make sure RemoveEdge code can keep track of
         edge count properly) */
    uint64_t        num_total_edges__;

    /** Size of the allocated array to store neighbor counts for for each vertex.
        Needed because the amount allocated for each vertex may be bigger than
        the actual number of neighbors. */
    int*            allocated_neighbor_counts__;

    /** Number of vertices used to allocate the vertex metadata and adjacency lists.*/
    uint64_t        allocated_vertex_count__;

    /** Stores the earliest and latest dates for edges in our graph (to the second). */
    time_t          first_date__, last_date__;
    /** Stores the number of milliseconds after the given first/last dates. */
    int             first_date_ms__, last_date_ms__;
    
    // /** Boolean flag indicating if the memory has been allocated to store
    // adjacency lists. */
    //bool            is_neighbor_allocated__;
    
};

/*

uint64_t* GetFOAFs(const Graph* g, uint64_t u, int& foaf_count)
{
    foaf_count = 0;
    int u_neighbor_count;
    const Edge* u_neighbors = g->Neighbors(u, u_neighbor_count);

    int foaf_est = 0;

    for (int i = 0; i < u_neighbor_count; i++) {
        foaf_est += g->NeighborCount(u_neighbors[i].dst);
    }

    if (foaf_est == 0) {
        return NULL;
    }

    uint64_t* foafs = new uint64_t[foaf_est];
    int next = 0;

    for (int i = 0; i < u_neighbor_count; i++) {
        int v_neighbor_count;
        uint64_t v = u_neighbors[i].dst;
        const Edge* v_neighbors = g->Neighbors(v, v_neighbor_count);
        if (v_neighbor_count) {
            for (int j = 0; j < v_neighbor_count; j++) {
                if (v_neighbors[j].dst != u) {
                    foafs[next++] = v_neighbors[j].dst;
                } 
            }
        }
    }

    sort(foafs, foafs + next);
    uint64_t* foafs_end = unique(foafs, foafs + next);
    foaf_count = foafs_end - foafs;
    return foafs;
}

class VertexFilter {
public:
    VertexFilter();
    void AddFilter(const char* type);
    bool operator()(const Graph* g, uint64_t u);
private:
    vector<int> filter_types__;
    vector<string> filter_type_strs__;
    int  num_filters__;
};

VertexFilter :: VertexFilter() : 
        num_filters__(0)
{
}

void VertexFilter :: AddFilter(const char* type)
{
    GraphParser& parser = GraphParser::Instance();
    parser.SelectTypes(type, filter_types__, filter_type_strs__);
    num_filters__ = filter_types__.size();
    cout << "Adding the following type to filter list ..." << endl;
    for (int i = 0; i < num_filters__; i++) {
        cout << "   " << filter_type_strs__[i] << endl;
    }
    return;
}

bool VertexFilter :: operator() (const Graph* g, uint64_t u)
{
    const VertexProperty& v_prop = g->GetVertexProperty(u);
    bool filter = true;

    for (int i = 0; i < num_filters__; i++) {
        if (filter_types__[i] == v_prop.type) {
            filter = false;
            break;
        }
    }
    return filter; 
}

 */
template <class VertexData, class EdgeData>
void PrintEdge(const Graph<VertexData, EdgeData>* g,  
        const Edge<EdgeData>& e)
{
    cout << "nbr = " << g->GetVertexProperty(e.dst).vertex_data.label 
         << " outgoing=" << e.outgoing 
         << " type = " << e.type << endl;
    return;
}

template <class VertexData, class EdgeData>
string GetLabel(const Graph<VertexData, EdgeData>* g, uint64_t u)
{
    return g->GetVertexProperty(u).vertex_data.label;
}

#endif

