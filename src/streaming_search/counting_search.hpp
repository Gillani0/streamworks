#ifndef __COUNTING_SEARCH_HPP__
#define __COUNTING_SEARCH_HPP__

#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include "counting_search.h"
#include "subgraph_search.hpp"
// #include "gephi_client.h"
#include <vector>
using namespace std;
using namespace boost;

// template <typename Graph>
template <typename Graph, typename SearchImpl>
class CountingSearch {
public:
    typedef typename Graph::DirectedEdgeType dir_edge;
/*
    CountingSearch() 
    {
        cout << "Uninitialized CountingSearch object ..." << endl;
    }
*/
    
    CountingSearch() {}

    CountingSearch(Graph* gSearch, SearchImpl* nested_query, 
            const vector<uint64_t>& group_by_vertices, int min_count,
            bool distinct = true):
            nested_query__(nested_query), max__(min_count),
            gSearch__(gSearch), distinct__(distinct)
    {
        min__ = max__ - 4;
        max_level__ = max__ - min__ + 2;
        group_by_vertices__ = group_by_vertices;

        search_time__ = 0;
        map_time__ = 0;
        hash_time__ = 0;
/*
        cout << "GROUP_BY_NODES = [";

        if (group_by_vertices.size() > 0) {
            cout << group_by_vertices[0];
        }
        for (int i = 1, N_g = group_by_vertices.size(); i < N_g; i++) {
            cout << "," << group_by_vertices[i];
        }
        cout << "]" << endl;
*/
    }

/*
    CountingSearch(GraphQuery<Graph> nested_query, 
            const vector<uint64_t>& group_by_vertices, int min_count):
            nested_query__(nested_query), min__(min_count)
    {
        max__ = min__ + 10;
        cout << "Initializing CountingSearch ..." << endl;
        // gSearch__ = nested_query__.GetDataGraph();
        group_by_vertices__ = group_by_vertices;
    }
*/

    void Map(const MatchSet* match, 
            string& key, string& match_signature, VertexSet& vertex_set)
    {
        stringstream strm;

        for (vector<uint64_t>::iterator it = group_by_vertices__.begin();
                it != group_by_vertices__.end(); it++) {
            strm << "_" << match->GetMatch(*it);
        }

        key = strm.str();

        stringstream strm1;
        for (int i = 0, N = match->NumVertices(); i < N; i++) {
            int i_match = match->vertex_map[i].second;
            vertex_set.insert(i_match);
            strm1 << "_" << i_match;
        }
    
        match_signature = strm1.str();
        return;
    }
 
    int Search(const dir_edge& edge)
    {
        d_array<MatchSet*> match_list;
#ifdef TIMING
        struct timeval start, end;
#endif

#ifdef TIMING
        gettimeofday(&start, NULL);
#endif
        nested_query__->Execute(edge, match_list); 
#ifdef TIMING
        gettimeofday(&end, NULL);
        search_time__ += get_tv_diff(start, end);        
#endif

        int total_match_count = 0;
        
        for (int i = 0, N = match_list.len; i < N; i++) {
            const MatchSet* match = match_list[i];
#ifdef TIMING
            gettimeofday(&start, NULL);
#endif
            string key;
            string match_signature;
            VertexSet value; 
            Map(match, key, match_signature, value);
#ifdef TIMING
            gettimeofday(&end, NULL);
            map_time__ += get_tv_diff(start, end);
#endif
#ifdef TIMING
            gettimeofday(&start, NULL);
#endif
            int level = UpdateMatchTable(key, match_signature, value);
#ifdef TIMING
            gettimeofday(&end, NULL);
            hash_time__ += get_tv_diff(start, end);
#endif
            // if (level > 0) {
#ifdef EXPORT_GEPHI
                ExportGephiStreaming(match, level);
#endif
#ifdef EXPORT_MAPVIEW
                ExportMapView(match, level);
#endif
            // }
        }

        return match_list.len;
    }

    inline int operator()(const dir_edge& edge)
    {
        return Search(edge);
    }

    void PrintMatchKeys(string key, const VertexSet& s) 
    {
        vector<string> tokens;
        split((char*)key.c_str(), "_", tokens);
        if (tokens.size() == 0) {
            return;
        }
        cout << "MATCH,"; 

        vector<int> pivots;
        for (int i = 0; i < tokens.size(); i++) {
            if (tokens[i].size() > 0) {
                int u = atoi(tokens[i].c_str());
                cout << GetLabel(gSearch__, u) << ", ";
                pivots.push_back(u);
            }
        }
        for (unordered_set<uint64_t>::const_iterator s_it = s.begin();
                s_it != s.end(); s_it++) {
            cout << GetLabel(gSearch__, *s_it) << ", ";
        }
        cout << endl;
/*
        for (unordered_set<uint64_t>::const_iterator s_it = s.begin();
                s_it != s.end(); s_it++) {
            for (int j = 0; j < pivots.size(); j++) {
                GephiStreamingClient::GetInstance().PostEdge(*s_it, pivots[j]);
            }
        }
*/
    }

    int UpdateMatchTable(const string& key, const string& match_signature, 
            const VertexSet& vertex_set)
    {
        unordered_map<string, MatchSummary>::iterator match_it =
                match_table__.find(key);
        int match_level;

        if (match_it == match_table__.end()) {
            match_table__.insert(pair<string, MatchSummary>(key, 
                    MatchSummary(vertex_set, match_signature)));
            match_level = 0;
        } 
        else {
            int curr_support = match_it->second.GetCount();
            if (curr_support < max__) {
                if (distinct__) {
                    curr_support = match_it->second.Update(vertex_set, 
                            match_signature);
                }
                else {
                    curr_support = match_it->second.Update(vertex_set);
                }
                // if (curr_support == max__) {
                    // cout << "Full match found !" << endl;
                // }
            }
            else {
                curr_support = match_it->second.IncreaseCount();
            }
            // cout << "Current support = " << curr_support << " min " << min__ << endl;
            if (curr_support < min__) {
                // match_it->second.Update(vertex_set, match_signature);
                match_level = 0;
            }
            else {
                // if (curr_support == min__) {
                    // const VertexSet& s = match_it->second.GetVertexSet();
                    // PrintMatchKeys(key, s);
                // }
                // match_it->second.IncreaseCount();
                match_level = curr_support - min__ + 1;
                if (match_level > max_level__) {
                    match_level = max_level__;
                }
            }
        }

        return match_level;
    }

    void PrintLevel(int level, ostream& ofs, int* counts, int offset)
    {
        PrintSpaces(offset, ofs);
        ofs << "{" << endl;
        PrintSpaces(offset+4, ofs);
        ofs << "\"value\": " << level << "," << endl;
        PrintSpaces(offset+4, ofs);
        if (level > 0) {
            ofs << "\"count\": " << counts[level] << "," << endl;
            PrintSpaces(offset+4, ofs);
            ofs << "\"children\": [" << endl; 
            PrintLevel(--level, ofs, counts, offset+4);
            PrintSpaces(offset, ofs);
            ofs << "]" << endl; 
         }
        else {
            ofs << "\"count\": " << counts[level] << endl;
        }
        PrintSpaces(offset, ofs);
        ofs << "}" << endl;
        return;
    }

    void ExportStatistics(string path)
    {
        int num_buckets = max__ - min__ + 3;        
        int* counts = new int[num_buckets];
        for (int i = 0; i < num_buckets; i++) {
            counts[i] = 0;
        }

        unordered_map<string, MatchSummary>::iterator it;
        for (it = match_table__.begin(); it != match_table__.end(); it++) {
            int count = it->second.GetCount();
            int level;
            if (count < min__) {
                level = 0;
            }
            else if (count > max__) {
                level = num_buckets-1;
            }
            else {
                level = count - min__ + 1;
            }
            counts[level] += 1;
        }

        ofstream ofs(path.c_str(), ios_base::out);
        PrintLevel(num_buckets-1, ofs, counts, 0);
        ofs.close();
        cout << "Search = " << search_time__ << endl;
        cout << "Map = " << map_time__ << endl;
        cout << "Hash = " << hash_time__ << endl;
        search_time__ = 0;
        map_time__ = 0;
        hash_time__ = 0;
        return;
    }

private:
    unordered_map<string, MatchSummary> match_table__; 
    // GraphQuery<Graph> nested_query__;
    const Graph* gSearch__;
    SearchImpl* nested_query__;
    vector<uint64_t> group_by_vertices__;
    int min__;
    int max__;
    int max_level__;
    bool distinct__;
    uint64_t search_time__;
    uint64_t map_time__;
    uint64_t hash_time__;
};

#endif
