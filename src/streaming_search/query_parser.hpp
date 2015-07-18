#ifndef __QUERY_PARSER_HPP__
#define __QUERY_PARSER_HPP__

#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <map>
#include <vector>
#include <stdlib.h>
#include <boost/unordered_map.hpp>
#include "graph.hpp"
#include "utils.h"
using namespace std;
using namespace boost;


struct GraphProperty {
    
    typedef std::map<string, int> PropertyMap;
    typedef std::map<string, int>::const_iterator PropertyIterator;

    GraphProperty() {}
    GraphProperty(const char* vertex_property_map_path,
            const char* edge_property_map_path)
    {
        LoadPropertyMap(vertex_property_map_path,
                vertex_property_map);
        LoadPropertyMap(edge_property_map_path,
                edge_property_map);
    }

    void LoadPropertyMap(const char* map_path,
            PropertyMap& property_map)
    {
        ifstream ifs(map_path, ios_base::in);
        if (ifs.is_open() == false) {
            cout << "Failed to open: " << map_path << endl;
            _exit(1);
        }
        string token, line;
        while (ifs.good()) {
            getline(ifs, line); 
            vector<string> tokens;
            stringstream strm(line);
            while (strm.good()) {
                strm >> token;
                tokens.push_back(token);
            }
            if (tokens.size() == 2) {
                int val = atoi(tokens[1].c_str());
                property_map[tokens[0]] = val;
            }
        }
        ifs.close();
 
        return;
    }

    inline int GetId(const PropertyMap& map, const char* key) const
    {
        // unordered_map<string, int>::const_iterator p_itr = map.find(key);
        PropertyIterator p_itr = map.find(key);
        
        if (p_itr != map.end()) {
            return p_itr->second;
        }
        else {
            return -1;
        }
    }

    int GetVertexTypeId(const char* key) const
    {
        return GetId(vertex_property_map, key);
    }

    int GetEdgeTypeId(const char* key) const
    {
        return GetId(edge_property_map, key);
    }

    PropertyMap vertex_property_map;
    PropertyMap edge_property_map;
};

enum QueryType { BGP, GROUPBY, SIMPLE_GROUPBY };

struct SearchContext {
    vector<uint64_t> group_by_vertices;
    bool distinct;
    int min_count;
    QueryType query_type;
};

template <typename VertexData, typename EdgeData>
class QueryParser {
public:
    void Tokenize(string& line, vector<string>& tokens)
    {
        istringstream strm(line);
        string token;
        while (strm.good()) {
            strm >> token;
            // cout << "Read [" << token << "]" << endl;
            if (token.size()) {
                tokens.push_back(token);
            }
        }
        return;
    }

    void ParseVertex(string& line, uint64_t& u, 
            VertexProperty<VertexData>& v_prop)
    {
        vector<string> tokens;
        // cout << "Line = [" << line << "]" << endl;
        Tokenize(line, tokens);
        // for (int i = 0; i < tokens.size(); i++) {
            // cout << "   token = " << tokens[i] << endl;
        // }
        string vertex_label = tokens[1];
        u = GetVertexId(vertex_label);
        // cout << "Vertex_id = " << u << endl;
        string vertex_type = tokens[2];

        if (tokens.size() == 4) {
            bool pure_label = true;
            string label = tokens[3];
            if (label.find("==") != string::npos) {
                pure_label = false;
            }
            else if (label.find("!=") != string::npos) {
                pure_label = false;
            }
            else if (label.find("<") != string::npos) {
                pure_label = false;
            }
            else if (label.find(">") != string::npos) {
                pure_label = false;
            }
            if (pure_label) {
                v_prop.vertex_data.label = tokens[3];
            }
        }
        else {
            v_prop.vertex_data.label = "";
        }

        v_prop.type = graph_property.GetVertexTypeId(vertex_type.c_str());
        cout << "Vertex id=" << u << endl;
        cout << "Vertex label=[" << v_prop.vertex_data.label << "]" << endl;
        cout << "Vertex type=" << v_prop.type << endl;
        if (v_prop.type == -1) {
            cout << "WARNING: Loaded query graph with invalid vertex type!" << endl;
            exit(1);
        } 
        return;
    }
                                                         
    void ParseEdge(string& line, 
            DirectedEdge<EdgeData>& dir_edge)
    {
        vector<string> tokens;
        // cout << "Line = [" << line << "]" << endl;
        Tokenize(line, tokens);
        string src_vertex = tokens[1];
        string dst_vertex = tokens[2];
        string edge_type = tokens[3];
        dir_edge.s = GetVertexId(src_vertex);
        dir_edge.t = GetVertexId(dst_vertex);
        dir_edge.type = graph_property.GetEdgeTypeId(edge_type.c_str());
        // cout << "   edge type = " << dir_edge.type << endl;
        // cout << dir_edge.s << " -> " << dir_edge.t << endl;
        if (dir_edge.type == -1) {
            cout << "WARNING: Loaded query graph with invalid edge type!" << endl;
            exit(1);
        } 
        return;
    }

    Graph<VertexData, EdgeData>*
    ParseQuery(const char* path, const char* vertex_property_map,
            const char* edge_property_map)
    {
        cout << "Parsing query=" << path << endl;
        graph_property = GraphProperty(vertex_property_map,
                edge_property_map);

        ifstream ifs(path, ios_base::in);
        if (ifs.is_open() == false) {
            cout << "Failed to open: " << path << endl;
            _exit(1);
        }
        string line, token;
        search_context.query_type = BGP;

        while (ifs.good()) {
            getline(ifs, line);
            Strip(line);
            if (line.size() == 0) continue;
            if (line[0] == 'v') {
                vertex_lines.push_back(line);
            }
            else if (line[0] == 'e') {
                edge_lines.push_back(line);
            }
            else if (line[0] == 'q') {
                ParseCommand(line);
            }
        }

        ifs.close();

        int vertex_count = vertex_lines.size();
        Graph<VertexData, EdgeData>* g = 
                new Graph<VertexData, EdgeData>(vertex_count);
    
        for (int i = 0, N = vertex_lines.size();
               i < N; i++) {
            uint64_t u;
            VertexProperty<VertexData> v_prop;
            ParseVertex(vertex_lines[i], u, v_prop);
            g->AddVertex(u, v_prop);
        }
    
        g->AllocateAdjacencyLists();

        for (int i = 0, N = edge_lines.size();
               i < N; i++) {
            DirectedEdge<EdgeData> dir_edge;
            dir_edge.id = i;
            ParseEdge(edge_lines[i], dir_edge);
            // cout << "Query edge = " << dir_edge << endl;
            g->AddEdge(dir_edge);
        }
    
        return g;
    }

    int GetVertexId(const string& label)
    {
        return atoi(label.c_str());
        std::map<string, int>::iterator it;
        it = vertex_id_map.find(label);
        int id;
        if (it == vertex_id_map.end()) {
            id = vertex_id_map.size();
            vertex_id_map.insert(pair<string, int>(label, id));
        }   
        else {
            id = it->second;
        }
        return id;
    }

    void ParseGroupBy(vector<string>& tokens) 
    {
        if (tokens.size() != 5) {
            cout << "GROUPBY line format: " 
                 << "q GROUPBY <commad sep node list> COUNT_GT <threshold>" << endl;
            for (int i = 0; i < tokens.size(); i++) {
                cout << tokens[i] << " ";
            }
            cout << endl;
            exit(1);
        }
        string& node_list = tokens[2];
        vector<string> str_list;
        split((char *)node_list.c_str(), ",", str_list);
        char* dummy;
        for (int i = 0, N = str_list.size(); i < N; i++) {
            uint64_t u = strtoul(str_list[i].c_str(), &dummy, 10);
            search_context.group_by_vertices.push_back(u);
        }

        if (tokens[3].find("DISTINCT") != string::npos) {
            search_context.distinct = true;
        }
        else {
            search_context.distinct = false;
        }

        search_context.min_count = atoi(tokens[4].c_str());
        search_context.query_type = GROUPBY;
        return;
    }

    void ParseCommand(string& line) 
    {
        vector<string> tokens;
        Tokenize(line, tokens);
        if (tokens[1] == "GROUPBY") {
            ParseGroupBy(tokens);
        }
        return;
    }

    SearchContext search_context;
    GraphProperty graph_property;
    std::map<string, int> vertex_id_map;
    vector<string> vertex_lines;
    vector<string> edge_lines;
};

#endif
