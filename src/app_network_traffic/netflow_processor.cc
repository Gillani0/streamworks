#include <sstream>
#include <time.h>
#include <boost/unordered_map.hpp>
#include <vector>
#include "graph.hpp"
#include "query_parser.hpp"
#include "netflow_parser.h"
#include "netflow_processor.h"
#include "utils.h"
#include <functional>
#include <assert.h>
#include <boost/tuple/tuple.hpp>
#include "gephi_client.h"
using namespace std;
using namespace boost;

#define UDP_TRAFFIC 0
#define TCP_TRAFFIC 1
#define ICMP_TRAFFIC 2
#define UNKNOWN_TRAFFIC 2

NetflowProcessor :: NetflowProcessor(string graph_path, 
        RECORD_FORMAT in_format,
        int num_header_lines):
        in_format__(in_format),
        graph_path__(graph_path),
        num_header_lines__(num_header_lines)
{
    parser__ = GetLineParser(in_format__);
    cout << "Allocating memory for netflow records ..." << endl;
    edges__.reserve(40000000);
    cout << "done." << endl;
    num_lines__ = 0;
    tcp_count__ = 0;
    udp_count__ = 0;
    icmp_count__ = 0;
    unknown_count__ = 0;
/*
    protocol_types__.insert(pair<string, int>("ICMP", 0));
    protocol_types__.insert(pair<string, int>("TCP", 1));
    protocol_types__.insert(pair<string, int>("UDP", 2));
    protocol_types__.insert(pair<string, int>("IPv6", 3));
    protocol_types__.insert(pair<string, int>("99", 4));
    protocol_types__.insert(pair<string, int>("AH", 5));
    protocol_types__.insert(pair<string, int>("ESP", 6));
    protocol_types__.insert(pair<string, int>("GRE", 7));
*/
    return;
}

int NetflowProcessor :: LineCount() { return num_lines__; }

void NetflowProcessor :: SetPropertyPath(string path)
{
    property_path__ = path;
    return;
}

void NetflowProcessor :: Load()
{
    Load(graph_path__, num_header_lines__);
    return;
}

void NetflowProcessor :: FilterAddress(char* addr)
{
    char* ptr = addr;
    int num_delims = 0; 
    while (*ptr != '\0') {
        if (*ptr == '.') {
            if (num_delims == 3) {
                *ptr = '\0';
                break;
            }
            else {
                num_delims++;
            }
        }
        ptr++;
    }
    return;
}

uint64_t NetflowProcessor :: GetAddressId(const char* addr)
{
    unordered_map<string, uint64_t>::iterator it = 
           ip_ids__.find(addr);
    uint64_t id;

    if (it == ip_ids__.end()) {
        id = ip_ids__.size();
        ip_ids__.insert(pair<string, uint64_t>(addr, id));
    }
    else {
        id = it->second;
    }
    return id; 
}

/*
uint64_t NetflowProcessor :: GetProtocolType(const char* protocol)
{
    map<string, int>::iterator it = 
           protocol_types__.find(protocol);
    int id;

    if (it == protocol_types__.end()) {
        cout << "Failed to find protocol = " << protocol << endl;
        exit(1);
        // id = protocol_types__.size();
        // protocol_types__.insert(pair<string, int>(protocol, id));
    }
    else {
        id = it->second;
    }
    return id; 
}
*/

void NetflowProcessor :: ParsePcap(char* line)
{
    if (isdigit(line[0]) == 0) {
        return;
    }

    int type;
    if (strstr(line, "UDP")) {
        type = UDP_TRAFFIC;
        udp_count__++;
    }
    else if (strstr(line, "ICMP")) {
        type = ICMP_TRAFFIC;
        icmp_count__++;
    }
    else if (strstr(line, " IPv6 ")) {
        type = TCP_TRAFFIC;
        tcp_count__++;
    }
    else {
        type = UNKNOWN_TRAFFIC;
        unknown_count__++;
    }

    if (type == UNKNOWN_TRAFFIC) {
        return;
    }
    // cout << "INP : " << line << endl;
    char timestamp[32];
    char src_ip[32];
    char dst_ip[32];
    stringstream strm(line);
    strm.get(timestamp, 20);
    // cout << "Timestamp = " << timestamp << endl;
    strm.ignore(11);
    strm >> src_ip;
    strm.ignore(3);
    strm >> dst_ip;
    // cout << "SRC IP = " << src_ip << endl;
    // cout << "DST IP = " << dst_ip << endl;
    FilterAddress(src_ip);
    FilterAddress(dst_ip);

    struct tm tm;
    strptime(line, netflow_timestamp_format.c_str(), &tm);
    tm.tm_isdst = -1;
    time_t t = mktime(&tm);

    DirectedEdge<Timestamp> edge;
    edge.s = GetAddressId(src_ip);
    edge.t = GetAddressId(dst_ip);
    edge.type = type;
    edge.edge_data.timestamp = t;
    edge.timestamp = t;
    // Change this if going back to vector<DirectedEdge<Timestamp>>
    // edges__.push_back(edge);
    num_lines__++;
    return;
}

/*
void NetflowProcessor :: bad_but_still_better_strptime(const char* line, struct tm& tdata)
{
    int numbers[32];
    for (int i = 0; i < 19; i++) {
        numbers[i] = line[i] - 48;
    }
    tdata.tm_year = 1000*numbers[0] + 100*numbers[1] + 10*numbers[2] + numbers[3] - 1900;
    tdata.tm_mon = 10*numbers[5] + numbers[6] - 1;
    tdata.tm_mday = 10*numbers[8] + numbers[9];
    tdata.tm_hour = 10*numbers[11] + numbers[12];
    tdata.tm_min = 10*numbers[14] + numbers[15];
    tdata.tm_sec = 10*numbers[17] + numbers[18];
    return;
}
*/

void NetflowProcessor :: ParseNetflow(char* line)
{
    struct timeval t1, t2;
    NetflowRecord netflow_record;
    // string record = line;
    // Parse(line, netflow_record);
    // ParseNCCDC(line, netflow_record);
    bool parse_success = parser__(line, netflow_record);
    if (parse_success == false) {
        return;
    } 

    DirectedEdge<NetflowAttbs> edge;
    edge.s = GetAddressId(netflow_record.src_ip);
    edge.t = GetAddressId(netflow_record.dst_ip);
    struct tm tm;
    edge.timestamp = netflow_record.timestamp;
    edge.type = netflow_record.protocol;
    GetAttributes(netflow_record, edge.edge_data);
    if (edge.type < 0) {
        cout << "bad edge type = " << edge.type << endl;
        // cout << "LINE: " << record << endl;
        exit(1);
    }
    edges__.push_back(edge);
    num_lines__++;
    if ((num_lines__ % 1000000) == 0) {
        cout << "INFO Ingested "  << num_lines__ << " netflow records" << endl;
    }
    return;
}

void NetflowProcessor :: PrintSummary()
{
    // cout << "# of protocols = " << protocol_types__.size() << endl;
    // for (map<string, int>::iterator it = protocol_types__.begin();
            // it != protocol_types__.end(); it++) {
        // cout << it->first << ":" << it->second << endl;
    // }
    // cout << "UDP = " << udp_count__ << endl;
    // cout << "TCP = " << tcp_count__ << endl;
    // cout << "ICMP = " << icmp_count__ << endl;
    // cout << "UNKNOWN = " << unknown_count__ << endl;

    cout << "# ip addresses = " << ip_ids__.size() << endl;
    cout << "# packets = " << edges__.size() << endl;
    return;
}

void NetflowProcessor :: GetVertexProperties(vector<VertexProperty<Label> >& vertex_properties)
{
    unordered_map<string, uint64_t>::iterator it;
    vertex_properties.resize(ip_ids__.size());

    for (it = ip_ids__.begin(); it != ip_ids__.end(); it++) {
        VertexProperty<Label> v_prop;
        v_prop.vertex_data.label = it->first;
        vertex_properties[it->second] = v_prop;
    }
    return;
}

void NetflowProcessor :: GetEdges(vector<DirectedEdge<NetflowAttbs> >& edge_list)
{
    edge_list.reserve(edges__.size());
    for (vector<DirectedEdge<NetflowAttbs> >::iterator e_it = edges__.begin(); 
            e_it != edges__.end(); e_it++) {
        edge_list.push_back(*e_it);
    }
    return;
}

void NetflowProcessor :: Report(vector<boost::tuple<int, long, long> >& timing_data) 
{   
    ofstream ofs("test/results/runtime_vs_edges.csv", ios_base::app);
    // ofs << "processed_edges,proc_time,num_matches" << endl;
    // for (auto record : timing_data) {
    for (int i = 0, N = timing_data.size(); i < N; i++) {
        const boost::tuple<int, long, long>& record = timing_data[i];
        ofs << get<0>(record) << "," << get<1>(record) 
             << "," << get<2>(record) << endl;
    }   
    ofs.close();
    return;
}   

/*
void NetflowProcessor :: Run(EdgeProcessor& edge_processor)
{   
    VertexProperty<Label> v_prop;
    unordered_map<string, uint64_t>::iterator v_it;    
    int REPORTING_INTERVAL = 10000;
    Graph<Label, NetflowAttbs>* gSearch = 
            new Graph<Label, NetflowAttbs>(ip_ids__.size());

    for (v_it = ip_ids__.begin(); v_it != ip_ids__.end(); v_it++) {
        v_prop.vertex_data.label = v_it->first;
        gSearch->AddVertex(v_it->second, v_prop);
    }   

    gSearch->AllocateAdjacencyLists();

    vector<DirectedEdge<NetflowAttbs> >::iterator e_it;
    int num_edges(0), num_results(0);
    vector<tuple<int, long, long> > timing_data;

    for (e_it = edges__.begin(); e_it != edges__.end(); e_it++) {
        gSearch->AddEdge(*e_it);
        gettimeofday(&t1, NULL);
        num_results += edge_processor.ProcessEdge(*e_it);
        gettimeofday(&t2, NULL);
        proc_time = get_tv_diff(t1, t2);
        num_edges++;
        if (num_edges % REPORTING_INTERVAL == 0) {
            cout << "#edges=" << num_edges  << "proc_time=" << proc_time 
                    << "(us) #results = " << num_results << endl;
            timing_data.push_back(tuple<int, long, long>(num_edges, 
                    proc_time, num_results));
            proc_time = 0;
            num_results = 0;
        }   
    }   
    Report(timing_data);
}   
*/

StreamG* NetflowProcessor :: InitGraph()
{
    VertexProperty<Label> v_prop;
    unordered_map<string, uint64_t>::iterator v_it; 

    gSearch__ = new Graph<Label, NetflowAttbs>(ip_ids__.size());

    for (v_it = ip_ids__.begin(); v_it != ip_ids__.end(); v_it++) {
        v_prop.vertex_data.label = v_it->first;
        gSearch__->AddVertex(v_it->second, v_prop);
    }   

    gSearch__->AllocateAdjacencyLists();
    cout << "Graph initialization complete." << endl;
    return gSearch__;
}

void NetflowProcessor :: Load(string src_path, 
        int num_header_lines)
{
    cout << "++++++ Loading file: " << src_path << endl;
    system("date");
    int BUFSIZE = 4096;
    char line[BUFSIZE];
    FILE* fp = fopen(src_path.c_str(), "r");

    if (fp == NULL) {
        cout << "File does not exist: [" << src_path << "]" << endl;
        return;
    }   

    int count = 0;
    int num_proc = 0;
    while (fgets(line, BUFSIZE, fp)) {
        if (count >= num_header_lines) {
            ParseNetflow(line);
            num_proc += 1;
        }
        count++;
        // if (num_proc == 1000) {
            // break;
        // } 
    }   
    
    cout << "Number of lines parsed = " << LineCount() << endl;
    fclose(fp);
    system("date");
    cout << "-------- Finished loading file" << endl;
    PrintSummary();
}
