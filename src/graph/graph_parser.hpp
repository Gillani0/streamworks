#ifndef GRAPH_PARSER_INTERFACE__H
#define GRAPH_PARSER_INTERFACE__H

#include <vector>
#include "vertex.hpp"
#include "edge.hpp"

/** 
    @author Patrick Mackey 
*/

/** This abstract class contains prototypes for two virtual
    functions that can parse a file from our graph loader.
    It's not required to inherit from this class to use
    the graph loader.  Any class that contains methods that
    match these arguments can be used, but it might be useful
    to inherit from this class to make sure your graph parser
    has the required methods. */
template<typename VertexData, typename EdgeData>
class GraphParser
{
public:
    /** Virtual abstract function.
        When implemented, it should do the following:
        Parses a line of text from a file and fills the vectors
        with any vertex or edge information read in from the line.
        It is not required that the vertices be filled.
        @param line     A line of text from the file.
        @param vertices Output vector will be filled with vertex
                        data from the line (if any exists).
        @param edges    Output vector will be filled with edge
                        data from the line. */
    virtual void ParseEdge(char *line, 
         std::vector<Vertex<VertexData> > &vertices,
	     std::vector<DirectedEdge<EdgeData> > &edges) = 0;
    
    /** Virtual abstract function.
        When implemented it should do the following:
        Parses a line of text from a file and fills in the
        vertex object with the vertex information. 
    */
    virtual void ParseVertex(char *line,
         Vertex<VertexData> &vertex) = 0;	
};

#endif
