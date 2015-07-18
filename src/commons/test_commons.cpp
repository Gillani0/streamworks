#include <stdlib.h>
#include <time.h>
#include "gephi_client.h"
#include "utils.h"
using namespace std;

void TestMapLoadAndStore()
{
    map<int, float> m;
    m[1] = 1.0;
    m[2] = 10.0;
    m[3] = 100.0;
    m[4] = 1000.0;
    m[5] = 10000.0;

    StoreMap(m, "map.txt");
    map<int, float> m1 = SimpleLoadMap<int, float>("map.txt");
    unlink("map.txt");
    assert(m1.size() == 5);
    map<int, float>::iterator it = m.begin();
    assert(it->first == 1);
    assert(it->second == 1.0);
    it++;
    assert(it->first == 2);
    assert(it->second == 10.0);
    it++;
    assert(it->first == 3);
    assert(it->second == 100.0);
    it++;
    assert(it->first == 4);
    assert(it->second == 1000.0);
    it++;
    assert(it->first == 5);
    assert(it->second == 10000.0);
    cout << __func__ << " PASSED" << endl;
    return;
}

void TestGephi()
{
    int N = 1000;
    int M = 1000;
    
    srandom(time(NULL));
    for (int i = 0; i < M; i++) {
        int u = (random() % N);
        int v = (random() % N);
        if (u == v) {
            v = ((u+1) % N);
        }
        GephiStreamingClient::GetInstance().PostEdge(u, v);
    }
}

int main(int argc, char* argv[])
{
    // TestGephi();
    TestMapLoadAndStore();
    return 0;
}
