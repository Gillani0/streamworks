#ifndef __COUNTING_SEARCH_H__
#define __COUNTING_SEARCH_H__

#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include <stdint.h>
#include "match_report.h"
using namespace std;
using namespace boost;

typedef unordered_set<uint64_t> VertexSet;

class MatchSummary {
public:
    MatchSummary() 
    {
        match_count = 0;
    }

    MatchSummary(const VertexSet& v_set, const string& signature)
    {
        match_count = 0;
        Update(v_set, signature);
        return;
    }

    MatchSummary(const VertexSet& v_set)
    {
        match_count = 0;
        Update(v_set);
        return;
    }

    int Update(const VertexSet& v_set, const string& signature)
    {
        unordered_set<string>::iterator it = match_signatures.find(signature);
        if (it != match_signatures.end()) {
            return match_count;
        }

        match_signatures.insert(signature);

        for (VertexSet::const_iterator v_it = v_set.begin();
                v_it != v_set.end(); v_it++) {
            vertex_set.insert(*v_it);
        }   
        match_count++;
        return match_count;
    }

    int Update(const VertexSet& v_set)
    {
        for (VertexSet::const_iterator v_it = v_set.begin();
                v_it != v_set.end(); v_it++) {
            vertex_set.insert(*v_it);
        }   
        match_count++;
        return match_count;
    }

    inline int GetCount() { return match_count; }
    inline int IncreaseCount() 
    { 
        match_count++; 
        return match_count;
    }

    const VertexSet& GetVertexSet() const { return vertex_set; }
private:
    int match_count;
    VertexSet vertex_set;
    unordered_set<string> match_signatures;
};


#endif
