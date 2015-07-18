#ifndef __EDGE_DISTRIBUTION_H__
#define __EDGE_DISTRUBUTION_H__
#include <map>
#include <ostream>
#include "counter.hpp"
using namespace std;

class StreamingGraphDistribution {
public:
    void Update(int val);
    void Print(ostream& os);
    map<int, int> TakeSnapshot(uint64_t num_edges);
private:
    Counter<int> property_counts__;
    set<int> property_vals__;
    vector<map<int, int> > summaries__;
    vector<uint64_t> edge_counts__;
};

#endif
