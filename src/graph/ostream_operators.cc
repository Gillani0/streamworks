#include "edge.hpp"
#include <iostream>
using namespace std;

ostream& operator<<(ostream& os, const dir_edge_t& e)
{
    os << e.s << "-" << e.t;
    return os; 
}

ostream& operator<<(ostream& os, const Timestamp& ts)
{
    os << ts.timestamp;
    return os;
}
