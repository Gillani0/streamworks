#ifndef __GRAPH_LOADER_HPP__
#define __GRAPH_LOADER_HPP__

#define MAX_LEN 1024
/**
 * @author Sutanay Choudhury
 * @author Patrick Mackey
 */

#include <fstream>
#include <vector>
#include <map>
#include "graph.hpp"
#include "utils.h"
#include "graph_parser.hpp"

using namespace std;

template <typename VertexData, typename EdgeData>
struct StreamingEdgeCounter {
    StreamingEdgeCounter(): edge_count(0) {}
    void operator()(const DirectedEdge<EdgeData>& edge) {
        edge_count++;
    }   
    int edge_count;
};


/** Loads just the vertices of a graph using the given vertex parser.    
    @param g        The output graph
    @param vertexFileName   The filename of the vertex file to load from
    @param parser       The line parser to use to load from our files
    @param hasHeader    True if parser should ignore first line (default value is false)
    @param stopAfterLimit   Stops the parser after a given number of lines are read.
                            Used for testing purposes, mostly. Default is false.
    @param limit    The max number of lines to read if stopAfterLimit is true.
*/
template<typename VertexData, typename EdgeData>
void LoadVertices(Graph<VertexData, EdgeData> *g, 
               const char *vertexFileName,
               GraphParser<VertexData, EdgeData> *parser,
               bool hasHeader = false,
               bool stopAfterLimit = false, int limit = 10)
{
    char* line = new char[MAX_LEN];
    ifstream ifs(vertexFileName, ios_base::in);
    // Check to make sure file can be loaded
    if(!ifs.good()) {
        cerr << "ERROR: Unable to open file " << vertexFileName << endl
                << "It may not exist, or might be currently used by another program." << endl;
        ifs.close();
        delete [] line;
        exit(EXIT_FAILURE);
    }
        
    if(hasHeader)
        ifs.getline(line, MAX_LEN);
    
    int num_lines = 0;    
    while (ifs.good()) {
        ifs.getline(line, MAX_LEN);
        if (line[0] == '\0') continue;
        num_lines++;
        
        Vertex<VertexData> v;
        // WARNING: This can't catch seg-faults
        try {
            parser->ParseVertex(line, v);
        }
        catch(...) {
            cerr << "ERROR: Unable to parse vertex data from this line:" << endl
                    << line << endl;
            delete [] line;
            exit(EXIT_FAILURE);
        }    
        g->AddVertex(v);    
        
        // For testing purposes
        if(stopAfterLimit && num_lines >= limit)
            break;
    }

    ifs.close();
    cout << "Processed " << num_lines << " lines from : " << vertexFileName << endl;    
          
    delete[] line;
}

/** Loads graph from the given edge file using the given parser.
    @param g        The output graph
    @param edgeFileName     The filename of the edge file to load from
    @param parser       The line parser to use to load from our files
    @param hasHeader    True if parser should ignore first line. Default is false.
    @param stopAfterLimit   Stops the parser after a given number of lines are read.
                            Used for testing purposes, mostly. Default is false.
    @param limit    The max number of lines to read if stopAfterLimit is true.
*/
template<typename VertexData, typename EdgeData>
void LoadEdges(Graph<VertexData, EdgeData> *g,
               const char *edgeFileName,
               GraphParser<VertexData, EdgeData> *parser,
               bool hasHeader = false,
               bool stopAfterLimit = false, int limit = 10)
{  
    char* line = new char[MAX_LEN];
    ifstream ifs(edgeFileName, ios_base::in);
    // Check to make sure file can be loaded
    if(!ifs.good()) {
        cerr << "ERROR: Unable to open file " << edgeFileName << endl
                << "It may not exist, or might be currently used by another program." << endl;
        ifs.close();
        delete [] line;
        exit(EXIT_FAILURE);
    }
    
    vector<Vertex<VertexData> > verts;
    vector<DirectedEdge<EdgeData> > edges;
        
    if(hasHeader)
        ifs.getline(line, MAX_LEN);
    
    int num_lines = 0;    
    while (ifs.good()) {
        ifs.getline(line, MAX_LEN);
        if (line[0] == '\0') continue;
        num_lines++;
    
        // WARNING: This can't catch seg-faults
        try {
            verts.clear();
            edges.clear();
            parser->ParseEdge(line, verts, edges);
        }
        catch(...) {
            cerr << "ERROR: Unable to parse edge data from this line:" << endl
                    << line << endl;
            delete [] line;
            exit(EXIT_FAILURE);
        }
        for(size_t i=0; i<verts.size(); i++) {
            g->AddVertex(verts[i]);
        }
        for(size_t i=0; i<edges.size(); i++) {
            g->AddEdge(edges[i]);
        }    
        
        // For testing purposes
        if(stopAfterLimit && num_lines >= limit)
            break;
    }

    ifs.close();
    cout << "Processed " << num_lines << " lines from : " << edgeFileName << endl;
    
    delete[] line;
}

/** Loads graph from the given edge file using the given parser.
    @param g        The output graph
    @param edgeFileName     The filename of the edge file to load from
    @param parser       The line parser to use to load from our files
    @param stream_processor       The streaming processor to process edge stream
    @param hasHeader    True if parser should ignore first line. Default is false.
    @param stopAfterLimit   Stops the parser after a given number of lines are read.
                            Used for testing purposes, mostly. Default is false.
    @param limit    The max number of lines to read if stopAfterLimit is true.
*/
template<typename VertexData, typename EdgeData, typename StreamProcessor>
void ProcessEdgeStream(Graph<VertexData, EdgeData> *g,
               const char *edgeFileName,
               GraphParser<VertexData, EdgeData> *parser,
               StreamProcessor& stream_processor,
               bool hasHeader = false,
               bool stopAfterLimit = false, int limit = 10)
{  
    char* line = new char[MAX_LEN];
    ifstream ifs(edgeFileName, ios_base::in);
    // Check to make sure file can be loaded
    if(!ifs.good()) {
        cerr << "ERROR: Unable to open file " << edgeFileName << endl
                << "It may not exist, or might be currently used by another program." << endl;
        ifs.close();
        delete [] line;
        exit(EXIT_FAILURE);
    }
    
    vector<Vertex<VertexData> > verts;
    vector<DirectedEdge<EdgeData> > edges;
        
    if(hasHeader)
        ifs.getline(line, MAX_LEN);
    
    int num_lines = 0;    
    while (ifs.good()) {
        ifs.getline(line, MAX_LEN);
        if (line[0] == '\0') continue;
        num_lines++;
    
        // WARNING: This can't catch seg-faults
        try {
            verts.clear();
            edges.clear();
            parser->ParseEdge(line, verts, edges);
        }
        catch(...) {
            cerr << "ERROR: Unable to parse edge data from this line:" << endl
                    << line << endl;
            delete [] line;
            exit(EXIT_FAILURE);
        }
        for(size_t i=0; i<verts.size(); i++) {
            g->AddVertex(verts[i]);
        }
        for(size_t i=0; i<edges.size(); i++) {
            g->AddEdge(edges[i]);
            stream_processor(edges[i]);
        }    
        
        // For testing purposes
        if(stopAfterLimit && num_lines >= limit)
            break;
    }

    ifs.close();
    cout << "Processed " << num_lines << " lines from : " << edgeFileName << endl;
    
    delete[] line;
}

/** Loads graph from the given vertex and edge files using the given parser.
    @param g        The output graph
    @param vertexFileName   The filename of the vertex file to load from
    @param edgeFileName     The filename of the edge file to load from
    @param parser       The line parser to use to load from our files
    @param vertexHasHeader  True if we should ignore first line in vertex file (default is false)
    @param edgeHasHeader    True if we should ignore first line in edge file (default is false)
*/
template<typename VertexData, typename EdgeData>
void LoadGraph(Graph<VertexData, EdgeData> *g, 
     const char *vertexFileName,
     const char *edgeFileName,
     GraphParser<VertexData, EdgeData> *parser,
     bool vertexHasHeader = false,
     bool edgeHasHeader = false)
{
    LoadVertices(g, vertexFileName, parser, vertexHasHeader);
    LoadEdges(g, edgeFileName, parser, edgeHasHeader);
}


/*
void GetFileList(const char* input_path, vector<string>& file_list)
{
    if (!input_path) {
        return;
    }
    
    bool process_list = false;
    int len = strlen(input_path);
    
    if (len > 5) {
        if (!strcmp(input_path + len - 5, ".list")) {
            process_list = true;
        }
    }
    
    if (process_list == false) {
        file_list.push_back(input_path);
        return;
    }
    
    FILE* fp = fopen(input_path, "r");
    
    if (fp == NULL) {
        cerr << "GetFileList::Failed to open file containing batch file list : " 
        << input_path << endl;
        return;
    }   
    
    char line[MAX_LEN];
    
    while (fgets(line, MAX_LEN, fp)) {
        if (strlen(line)) {
            string filepath;
            int len = strlen(line);
            if (line[len-1] == '\n') {
                filepath = string(line, len-1);
            }   
            else {
                filepath = string(line);
            }   
            file_list.push_back(filepath); 
        }   
    }   
    
    fclose(fp);
    return;
}

bool IsVertexFile(const char* path)
{
    bool stat;
    if (strstr(path, ".vertices")) {
        stat = true;
    }
    else if (strstr(path, ".edges")) {
        stat = false;
    }
    // Mackey 1/9/13: Added error checking
    else {
    	cerr << "ERROR: " << path << " is unknown file type." << endl
    		 << "Vertex file names must end with *.vertices" << endl
    	     << "Edge file names must end with *.edges" << endl;
    	exit(EXIT_FAILURE);
    }

    return stat;
}
*/

template <typename VertexData, 
          typename EdgeData, 
          typename InputLineParser> 
bool GraphFileLoader(Graph<VertexData, EdgeData>* g, const char* path,
        InputLineParser& parser)
{
    bool parse_net = strstr(path, ".net") ?  true : false;
    bool parse_vertex;
    if (parse_net == false) {
        parse_vertex = IsVertexFile(path);
        if (parse_vertex == false && g->IsAdjacencyListAllocated() == false) {
            g->AllocateAdjacencyLists();
        }
    }
#ifdef __MTA__

    cout << "Loading " << path << endl;
    struct timeval t1, t2, t3;
    gettimeofday(&t1, NULL);
    File file = get_lines(path);
    gettimeofday(&t2, NULL);
    // Mackey 1/9/13: Added error checking
    if(file.buf == NULL) {
    	cerr << "ERROR: Unable to open file " << path << endl
    	     << "It may not exist, or might be currently used by another program." << endl;
    	exit(EXIT_FAILURE);
    }

/*
    #pragma mta serial
    for (int i = 0, N = file.num_lines; i < N; i++) {
        char* line = file.lines[i];
        if (parse_vertex) {
            uint64_t u = 0;;
            VertexProperty<VertexData> v_prop;
            parser.ParseVertex(line, u, v_prop);
            g->AddVertex(u, v_prop);
        }
        else {
            cout << "Adding edge " << i << endl;
            DirectedEdge<EdgeData> edge;
            parser.ParseEdge(line, edge);
            g->AddEdge(edge);
        }
    }
*/
    if (parse_vertex) {
        #pragma mta assert parallel
        for (int i = 0, N = file.num_lines; i < N; i++) {
            char* line = file.lines[i];
            if (parse_vertex) {
                uint64_t u = 0;;
                VertexProperty<VertexData> v_prop;
                parser.ParseVertex(line, u, v_prop);
                g->AddVertex(u, v_prop);
            }
             else {
                DirectedEdge<EdgeData> edge;
                parser.ParseEdge(line, edge);
                g->AddEdge(edge);
            }
        }
    }
    else {
        int* neighbor_counts = new int[g->NumVertices()];
        cout << "Initializing count array ..." << endl;
        #pragma mta assert parallel
        for (int i = 0, N = g->NumVertices(); i < N; i++) {
            neighbor_counts[i] = g->NeighborCount(i);
        }
        int V_G = g->NumVertices();
        cout << "Estimating neighbor counts ..." << endl;
        #pragma mta assert parallel
        for (int i = 0, N = file.num_lines; i < N; i++) {
            char* line = file.lines[i];
            // strtok modifies the input buffer, need to 
            // preserve it for second pass
            char* tmp = new char[strlen(line) + 1];
            strcpy(tmp, line);
            DirectedEdge<EdgeData> edge;
            parser.ParseEdge(tmp, edge);
            int_fetch_add(neighbor_counts + edge.s, 1);
            int_fetch_add(neighbor_counts + edge.t, 1);
            delete[] tmp;
        }

        cout << "Reallocating neighbors ..." << endl;
        #pragma mta assert parallel
        for (int i = 0, N = g->NumVertices(); i < N; i++) {
            g->AllocateAdjacencyList(i, neighbor_counts[i]);
        }

        cout << "Inserting edges ..." << endl;
        #pragma mta assert parallel
        for (int i = 0, N = file.num_lines; i < N; i++) {
            char* line = file.lines[i];
            DirectedEdge<EdgeData> edge;
            parser.ParseEdge(line, edge);
            g->AddEdgeWithoutRealloc(edge);
        }

        cout << "Done inserting edges ..." << endl;
        delete[] neighbor_counts;
    }
    gettimeofday(&t3, NULL);
    cout << "Processed " << file.num_lines << " lines from :" << path << endl;
    cout << "Load time = " << get_tv_diff(t1, t2) << " parsing time = " << get_tv_diff(t2, t3) << endl;
    if (parse_vertex == false) {
        double gups = ((double) file.num_lines/get_tv_diff(t2, t3));
        cout << "GUPS = " << gups << endl;
    }

#else

    char* line = new char[MAX_LEN];
    ifstream ifs(path, ios_base::in);
    // Mackey 1/9/13: Added error checking
    if(!ifs.good()) {
    	cerr << "ERROR: Unable to open file " << path << endl;
    	ifs.close();
    	delete [] line;
    	exit(EXIT_FAILURE);
    }
    cout << "Opened : " << path << endl;
    if (!ifs.is_open()) {
        cout << "ERROR Failed to open " << path << endl;
        return false;
    }
    bool alloc_adj_list = true;
    int num_lines = 0;    
    while (ifs.good()) {
        ifs.getline(line, MAX_LEN);
        if (line[0] == '\0') continue;
        num_lines++;

        if (parse_net) {
            parse_vertex = (line[0] == 'v') ? true : false;
            if (parse_vertex == false 
                    && g->IsAdjacencyListAllocated() == false) {
                // cout << "Allocating adj lists ..." << endl;
                g->AllocateAdjacencyLists();
            }
        }

        if (parse_vertex) {
            uint64_t u;
            VertexProperty<VertexData> v_prop;
            // Mackey 1/9/13: Added error checking
            // WARNING: This can't catch seg-faults
            try {
	            parser.ParseVertex(line, u, v_prop);
	        }
	        catch(...) {
	        	cerr << "ERROR: Unable to parse vertex data from this line:" << endl
	        	     << line << endl;
	        	delete [] line;
	        	exit(EXIT_FAILURE);
	        }    
            g->AddVertex(u, v_prop);
        }
        else {
            DirectedEdge<EdgeData> edge;
            // Mackey 1/9/13: Added error checking
            // WARNING: This can't catch sig-faults
            try {
	            parser.ParseEdge(line, edge);
	        }
	        catch(...) {
	        	cerr << "ERROR: Unable to parse edge data from this line:" << endl
	        	     << line << endl;
	        	delete [] line;
	        	exit(EXIT_FAILURE);
	        }
            g->AddEdge(edge);
        }
    }

    ifs.close();
    cout << "Processed " << num_lines << " lines from : " << path << endl;
    delete[] line;

#endif
    
    return true;
}

template <class EdgeData>
struct NoOp {
    inline void operator()(const DirectedEdge<EdgeData>& edge) {}
};

/**
 * A front end to GraphFileLoader.
 */
template <typename VertexData, 
          typename EdgeData, 
          typename InputLineParser>
bool GraphLoader(Graph<VertexData, EdgeData>* g, const char* input_path)
{
    InputLineParser parser;
 
    vector<string> file_list;
    GetFileList(input_path, file_list);

    for (int j = 0, M = file_list.size(); j < M; j++) {
        cout << "Loading:" << file_list[j] << endl;
        GraphFileLoader(g, file_list[j].c_str(), parser);
    }
        
    return file_list.size();
}

/*
inline uint64_t GetId(unordered_map<uint64_t, uint64_t>& id_map, uint64_t id) 
{
    unordered_map<uint64_t, uint64_t>::iterator id_map_it;
    id_map_it = id_map.find(id);
    uint64_t mapped_id;
    if (id_map_it == id_map.end()) {
        mapped_id = id_map.size();
        id_map.insert(pair<uint64_t, uint64_t>(id, mapped_id));
    }   
    else {
        mapped_id = id_map_it->second;
    }
    return mapped_id;
}
*/

template <typename VertexData,
          typename EdgeData,
          typename InputLineParser>
class GraphBuilder {
public:
    inline
    void AddEdge(DirectedEdge<EdgeData> e)
    {
        edges.push_back(e);
        return;
    }
    
    Graph<VertexData, EdgeData>* Build()
    {
        for (int i = 0, N_E = edges.size(); i < N_E; i++) {
            DirectedEdge<EdgeData>& edge = edges[i];
            edge.s = GetId(id_map, edge.s);
            edge.t = GetId(id_map, edge.t);
        }
    
        int N = id_map.size();

        Graph<VertexData, EdgeData>* g = new Graph<VertexData, EdgeData>(N);
     
        for (int i  = 0, N_E = edges.size(); i < N_E; i++) {
            g->AddEdge(edges[i]);
        }
     
        return g;
    }

protected:
    inline 
    uint64_t GetId(std::map<uint64_t, uint64_t>& id_map, uint64_t id)
    {
        std::map<uint64_t, uint64_t>::iterator id_map_it;
        id_map_it = id_map.find(id);
        uint64_t mapped_id;
        if (id_map_it == id_map.end()) {
            mapped_id = id_map.size();
            id_map.insert(pair<size_t, size_t>(id, mapped_id));
        }   
        else {
            mapped_id = id_map_it->second;
        }
        return mapped_id;
    }

private:
    vector<DirectedEdge<EdgeData> > edges;
    std::map<size_t, size_t> id_map;
};

/*
void GraphBuilder(const char* input_path)
{
    FILE* fp = fopen(input_path, "r");
    char line[MAX_LEN];
    InputLineParser parser;
    bool parse_vertex;
    vector<DirectedEdge<Edge> > edges;
    
    while (fgets(line, MAX_LEN, fp)) {
        parse_vertex = parser.ParseLine(line);
        DirectedEdge<EdgeData> edge;
        parser.ParseEdge(line, edge);
        if (parse_vertex) {
        }
        else {
            raw_edges.push_back(edge);
        } 
    }

    fclose(fp);

    unordered_map<uint64_t, uint64_t> id_map;

    for (int i = 0, N_E = edges.size(); i < N_E; i++) {
        DirectedEdge<EdgeData>& edge = edges[i];
        edge.s = GetId(id_map, edge.s);
        edge.t = GetId(id_map, edge.t);
    }

    int N = id_map.size();

    Graph<VertexData, EdgeData>* g = new Graph<VertexData, EdgeData>(N);
    
    for (int i  = 0, N_E = edges.size(); i < N_E; i++) {
        g->AddEdge(edges[i]);
    }
    
    return g;
}
*/

#endif
