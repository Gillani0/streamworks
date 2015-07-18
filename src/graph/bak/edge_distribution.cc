#include "edge_distribution.h"
using namespace std;

void StreamingGraphDistribution::Update(int val)
    {
        property_counts__.Add(val);
        return;
    }

    void StreamingGraphDistribution::Print(ostream& os)
    {
        int num_rows = summaries__.size();
        int num_cols = property_vals__.size();

        int col = 1;
        for (set<int>::iterator edge_type_it = property_vals__.begin();
                edge_type_it != property_vals__.end(); edge_type_it++) {
            os << *edge_type_it;
            if (col != num_cols) {
                os << ",";
            }
            col++;
        }
        os << endl;

        map<int, int>::iterator distribution_itr;
        for (int i = 0; i < num_rows; i++) {
            os << edge_counts__[i] << ",";
            map<int, int>& distribution = summaries__[i];
            col = 1;
            for (set<int>::iterator edge_type_it = property_vals__.begin();
                    edge_type_it != property_vals__.end(); edge_type_it++) {
                distribution_itr = distribution.find(*edge_type_it); 
                if (distribution_itr != distribution.end()) {
                    os << distribution_itr->second;
                }
                else {
                    os << "0";
                }
                if (col != num_cols) {
                    os << ",";
                }
                col++;
            }
            os << endl;
        }
        return; 
    }

    map<int, int> StreamingGraphDistribution::TakeSnapshot(uint64_t num_edges)
    {
        Counter<int>::const_iterator it;
        map<int, int> distribution;

        for (it = property_counts__.begin(); it != property_counts__.end();
                it++) {
            distribution[it->first] = it->second;
            property_vals__.insert(it->first); 
        }

        summaries__.push_back(distribution);
        property_counts__.reset();
        edge_counts__.push_back(num_edges);
        return distribution; 
    }
