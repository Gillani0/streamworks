#ifndef __EDGE_HPP__
#define __EDGE_HPP__
/**
 * @author Sutanay Choudhury
 * @author Patrick Mackey
 */

// #include <iosfwd>
#include <iostream>
#include <string>
#include <stdint.h>
using namespace std;


/** A kind of EdgeData.
    Used for a Graph with no Edge metadata */
struct UnLabeledEdge {
    
    /** Always returns true */
    bool operator==(const UnLabeledEdge& other) const
    {
        return true;
    }
};

/** Represents the basic Edge type for our Graph.
    Stores source and target nodes, the edge type,
    and the associated EdgeData. */
template <class EdgeData> 
struct DirectedEdge {
    /** Default constructor.
        @param u    The source vertex
        @param v    The target vertex
        @param t    The edge type
        @param edge_prop    The EdgeData
        @param time     The date/time the transaction occured
        @param ms       The milliseconds after the date/time it occured
      */
#ifdef AGGR_LOAD
    DirectedEdge(uint64_t u = 0, uint64_t v = 0, int t = 0, uint64_t num_bytes=0, 
            EdgeData edge_prop = EdgeData(), time_t time = 0, int ms = 0):
            s(u), t(v), type(t), edge_data(edge_prop), timestamp(time), timestamp_ms(ms) {}
#else
    DirectedEdge(uint64_t u = 0, uint64_t v = 0, int t = 0,  
            EdgeData edge_prop = EdgeData(), time_t time = 0, int ms = 0):
            s(u), t(v), type(t), edge_data(edge_prop), timestamp(time), timestamp_ms(ms) {}
#endif

    uint64_t        s, t;
    int             type;

#ifdef AGGR_LOAD
    uint64_t        num_bytes;
#endif

    EdgeData        edge_data;
    time_t          timestamp;
    int             timestamp_ms;
    int             id;

    /** Returns if another edge is less than this edge.
        This is based on index of source vertex first, and then
        target vertex next.  Useful for sorting.  */
    bool operator<(const DirectedEdge& other) const
    {
        if (s != other.s) {
            return s < other.s;
        }
        else {
            if (t != other.t) {
                return t < other.t;
            }
            else {
                return edge_data < other.edge_data;
            }
        }
    }

    /** Returns if two edges are equal, based on whether or
        not the indexes of the source and target are the same. 
        Question: Should we include time in this? */
    bool operator==(const DirectedEdge& other) const
    {
#ifdef AGGR_LOAD
        bool result = ((s == other.s) && (t == other.t) &&
                (edge_data == other.edge_data) && (type == other.type) && num_bytes==other.num_bytes) ;
#else
        bool result = ((s == other.s) && (t == other.t) &&
                (edge_data == other.edge_data) && (type == other.type)) ;
#endif
        return result;
    }

    string str() { return (std::to_string(s) + "-" + std::to_string(t)); }
};


/** A directed edge that contains a time stamp instead of edge data */
struct dir_edge_t {
    uint64_t s, t;
    int      type;
#ifdef AGGR_LOAD
    uint64_t        num_bytes;
#endif
    time_t   timestamp;
    int      timestamp_ms;

    /** Default constructor sets source, target and timestamp to 0 */
    dir_edge_t() : s(0), t(0), timestamp(0), timestamp_ms(0) {}

    dir_edge_t(uint64_t u, uint64_t v): 
        s(u), t(v), timestamp(0), timestamp_ms(0) {}

    /** Returns if another edge is less than this edge.
    This is based on index of source vertex first, and then
    target vertex next.  Useful for sorting.  */
    bool operator<(const dir_edge_t& other) const
    {
        if (s != other.s) {
            return s < other.s;
        }
        else {
            if (t != other.t) {
                return t < other.t;
            }
            else {
                if (timestamp != other.timestamp)
                    return timestamp < other.timestamp;
                else
                    return timestamp_ms < other.timestamp_ms;
            }
        }
    }

    /** Returns if two edges are equal, based on whether or
    not the indexes of the source and target are the same. */
    bool operator==(const dir_edge_t& other) const
    {
        return ((s == other.s) && (t == other.t) &&
                (timestamp == other.timestamp) && (timestamp_ms == other.timestamp_ms));
    }

    //string str() { return (std::to_string(s) + "-" + std::to_string(t)); }
};
/** Outputs dir_edge_t objects with C++ ostreams */
ostream& operator<<(ostream& os, const dir_edge_t& e);

/** Converts a DirectedEdge into a dir_edge_t object
    @param directed_edge    The DirectedEdge you want to convert
*/
template <class EdgeData>
dir_edge_t GetRelation(const DirectedEdge<EdgeData>& directed_edge)
{
    dir_edge_t relation;
    relation.s = directed_edge.s;
    relation.t = directed_edge.t;
    relation.type = directed_edge.type;
#ifdef AGGR_LOAD
    relation.num_bytes = directed_edge.num_bytes;
#endif
    return relation;
}

/** Outputs DirectedEdge objects with C++ ostreams */ 
template<class EdgeData>
ostream& operator<<(ostream& os, const DirectedEdge<EdgeData>& e)
{
    os << "edge(src=" << e.s << ", dst=" << e.t << ", type=" 
           << e.type << ", ts=" << e.edge_data << ")";
    return os;
}

/** Holds a basic edge structure for our Graph object */
template <typename EdgeData> 
struct Edge {
    Edge()  : dst(0), type(-1), outgoing(false), timestamp(0) {}
 
    /** Default constructor
        @param e    The DirectedEdge object that defines our edge
        @param is_outgoing  If false, t = source and s = target */        
    Edge(const DirectedEdge<EdgeData>& e, bool is_outgoing): 
            type(e.type),
            outgoing(is_outgoing),
            edge_data(e.edge_data),
            timestamp(e.timestamp),
            timestamp_ms(e.timestamp_ms)
    {   
        dst = is_outgoing ? e.t : e.s;
#ifdef AGGR_LOAD
        num_bytes = e.num_bytes;
#endif
    }   


    uint64_t        dst;
    int             type;
    EdgeData        edge_data;
#ifdef AGGR_LOAD
    uint64_t        num_bytes;
#endif
      bool            outgoing;
    time_t          timestamp;
    int             timestamp_ms;
    int             id;

    /** Sorts the Edge based on dst value */
    bool operator<(const Edge<EdgeData>& other) const
    {
        if (dst != other.dst) {
            return dst < other.dst;
        }
        else {
            if (type == other.type) {
                return edge_data < other.edge_data; 
            }
            else {
                return type < other.type;
            }
        }
    }

    /** @return Returns true only if dst, type, outgoing and edge_data
                are all equal
                Question: Should we include time in this? */
    bool operator==(const Edge<EdgeData>& other) const
    {
        return ((dst == other.dst) && (type == other.type)
                && (outgoing == other.outgoing)
                && (edge_data == other.edge_data));
    }
};

/** Creates a DirectedEdge from a basic graph Edge 
    @param u    The vertex at the other end of the edge that's not the vertex stored in e
    @param e    The Edge that stores the connecting vertex to u 
    @return     Returns the DirectedEdge representing the connection between u and e */
template <class EdgeData>
inline DirectedEdge<EdgeData> MakeEdge(uint64_t u, 
        const Edge<EdgeData>& e)
{
    DirectedEdge<EdgeData> dir_edge;
    if (e.outgoing) {
        dir_edge.s = u;
        dir_edge.t = e.dst;
    }
    else {
        dir_edge.s = e.dst;
        dir_edge.t = u;
    }
    dir_edge.type = e.type;
    dir_edge.edge_data = e.edge_data;
#ifdef AGGR_LOAD
    dir_edge.num_bytes = e.num_bytes;
#endif

    dir_edge.timestamp = e.timestamp;
    dir_edge.timestamp_ms = e.timestamp_ms;
    return dir_edge;
}

/** Outputs the Edge to C++ ostream */
template <class EdgeData>
ostream& operator<<(ostream& os, const Edge<EdgeData>& e)
{
    os << "edge(nbr=" << e.dst << ", outgoing=" << e.outgoing 
       << ", type=" << e.type << ", prop=" << e.edge_data << ")";
    return os;
}

/** Stores the time of an event, and allows for sorting against other TimeStamps */
struct Timestamp {

    time_t timestamp;

    Timestamp() : timestamp(0) {}

    inline bool operator<(const Timestamp& other) const
    {
        return timestamp < other.timestamp;
    }

    inline bool operator==(const Timestamp& other) const
    {
        return timestamp == other.timestamp;
    }
    
    inline bool FilterEdge(int edge_id) const
    {
        return true;
    }
};

/** Outputs the TimeStamp to C++ ostream */
ostream& operator<<(ostream& os, const Timestamp& ts);
// template class DirectedEdge<Timestamp>;
// template class Edge<Timestamp>;

#endif
