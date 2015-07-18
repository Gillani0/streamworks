#include <assert.h>
#include "match_report.h"
#include <iostream>
#include <fstream>
#include <sys/stat.h>
#include "gephi_client.h"
using namespace std;

void PrintSpaces(int num_spaces, ostream& ofs)
{   
    for (int i = 0; i < num_spaces; i++) {
        ofs << " ";
    }   
    return;
}   

void ExportGephiStreaming(const MatchSet* match_set, int node)                     
{                                                                                  
    char buf[256];                                                                 
    for (int i = 0, N = match_set->NumEdges(); i < N; i++) {                       
        int u = match_set->edge_map[i].second.s;                                   
        int v = match_set->edge_map[i].second.t;                                   
        GephiStreamingClient::GetInstance().PostEdge(u, v, node);              
    }                                                                              
}      

void LoadGeoLocations(vector<string>& locations, int& num_locations)
{
    string points = "/Users/d3m432/Sites/data/points.1.csv";
    ifstream ifs(points.c_str(), ios_base::in);
    string line;
    getline(ifs, line);
    while (ifs.good()) {
        getline(ifs, line);
        if (line.size() == 0) {
            continue;
        }
        size_t pos = line.find(",");
        string location = line.substr(0, pos);
        // cout << "Adding location : " << location << endl;
        locations.push_back(location);
    }
    ifs.close();
    num_locations = locations.size();
    if (num_locations == 0) {
        cout << "No location data available from: " << points << endl;
        exit(1);
    }
    return;
}

void ExportMapView(const MatchSet* match_set, int node)                            
{                                                                                  
    static vector<string> locations__;
    static int num_locations__ = 0;
    
    if (num_locations__ == 0) {
        LoadGeoLocations(locations__, num_locations__);
    }

    char buf[256];
    struct stat sinfo;
    int flag = stat("/tmp/routes", &sinfo);
    bool write_header = false;
    if (flag == -1) {
        write_header = true;
    }
    ofstream ofs("/tmp/routes", ios_base::app);
    if (write_header) {
        ofs << "origin,destination,value" << endl;
    }
    for (int i = 0, N = match_set->NumEdges(); i < N; i++) {
        int u = match_set->edge_map[i].second.s;
        int v = match_set->edge_map[i].second.t;
        string u_loc = locations__[u % num_locations__];
        string v_loc = locations__[v % num_locations__];
        // TODO fix this
        // int level = u % 3;
        ofs << u_loc << "," << v_loc << "," << node << endl;
    }
    ofs.close();
}
