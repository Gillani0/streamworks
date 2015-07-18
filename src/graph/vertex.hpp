#ifndef __VERTEX_HPP__
#define __VERTEX_HPP__

/**
 * @author Sutanay Choudhury
 * @author Patrick Mackey
 */

#include <stdint.h>
#include <iosfwd>
// #include <boost/regex.hpp>
#include <string.h>
using namespace std;
// using namespace boost;

/** Stores information about a vertex including its type and associated VertexData */
template <typename VertexData>
struct VertexProperty {
    /** The type of this vertex */
    int             type;
    /** The metadata associated with the vertex */
    VertexData      vertex_data;

    /** Default constructor */
    VertexProperty(): type(0) {}
    
    /** Copy constructor */
    VertexProperty(const VertexProperty& vp) :
            vertex_data(vp.vertex_data), type(vp.type) {}

    /** Allows sorting.  If types are the same, sorting is based
        on vertex_data.  If different, sorting is based on type */
    inline bool operator<(const VertexProperty& other) const
    {
        if (type == other.type) {
            return vertex_data < other.vertex_data;
        }   
        else {
            return type < other.type;
        }   
    }

    /** Allows comparing two vertex properties to see if they are the same */ 
    inline bool operator==(const VertexProperty& other) const
    {
        // P.Mackey: Added code here.  Was blank before.
        return type == other.type & vertex_data == other.vertex_data;
    }
};

/** Represents a text string for use as VertexData or EdgeData */
struct Label {

    /** label for Vertex or Edge */
    string label;

    /** Allows sorting of labels */
    inline bool operator<(const Label& other) const
    { 
#ifdef __MTA__
        int cmp = strcmp(label.c_str(), other.label.c_str());
        return (cmp < 0);
#else
        return label < other.label; 
#endif
    }

    /** Comparison of two labels */
    inline bool operator==(const Label& other) const
    {
        return label == other.label;
    }

    bool FilterVertex(int u, int deg, const Label& v_q_label) const;
/*
    {
        if (v_q_label.label.size() == 0) {
            return true;
        }
        else {
            bool label_check;
            if (!strncmp("re:", v_q_label.label.c_str(), 3)) {
                label_check = regex_match(label.c_str(), 
                        regex(v_q_label.label.c_str() + 3));
            }
            else {
                label_check = v_q_label.label == label;
            } 
            return label_check;
        } 
    }
*/
};

// template class VertexProperty<Label>;

/** Outputs VertexProperty to C++ ostream */
template <class VertexData>
ostream& operator<<(ostream& os, const VertexProperty<VertexData>& v_prop)
{
    os << v_prop.vertex_data << "(" << v_prop.type << ")";
    return os;
}


/** Stores the vertex index along with its VertexProperty information */
template<typename VertexData>
struct Vertex {
    /** Index of this vector in the graph */
    uint64_t vertex;
    /** The property associated with the vertex */
    VertexProperty<VertexData> vertex_property;
};


#endif
