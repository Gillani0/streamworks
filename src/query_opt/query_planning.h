#ifndef __QUERY_PLANNING_H__
#define __QUERY_PLANNING_H__

#include <functional>
#include <map>
#include <ostream>
#include <limits.h>
#include "counter.hpp"
#include "sj_tree.h"
using namespace std;

class StreamingGraphDistribution {
public:
    void Update(int val);
    void Print(ostream& os);
    std::map<int, int> TakeSnapshot(uint64_t num_edges);

private:
    Counter<int> property_counts__;
    set<int> property_vals__;
    vector<std::map<int, int> > summaries__;
    vector<uint64_t> edge_counts__;
};

void LoadEdgeDistribution(string path, std::map<int, float>& edge_distribution);
void StoreEdgeDistribution(const std::map<int, float> edge_distribution,
        string outpath);
JoinTreeDAO* SJTreeBuilder(const vector<Subgraph>& subgraphs);

#endif
