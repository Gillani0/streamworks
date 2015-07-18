#include <iostream>
#include <fstream>
#include <sstream>
#include <set>
#include <vector>
#include <map>
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "netflow_parser.h"
#include "utils.h"
using namespace std;


LineParser GetLineParser(RECORD_FORMAT format)
{
  cout << "File format ="<< format;
    switch (format) {
      
        case NETFLOW:       cout << "LineParser -> NetflowParser" << endl; 
                            return ParseNetflow; 
        case NCCDC:         cout << "LineParser -> ParseNCCDC" << endl;
                            return ParseNCCDC;
        case SILK:         cout << "LineParser -> ParseSilk" << endl;
                            return ParseSilk;
        case VAST:          cout << "LineParser -> ParseVast" << endl;
                            return ParseVast;
        case CONNECTIONS:         cout << "LineParser -> ParseConnections" << endl;
                            return ParseConnections;
        case WIN_EVNT_LOGS: cout << "LineParser -> ParseWindowsEvents" << endl;
                            return ParseWindowsEvents;
        default:            cout << "ERROR: Unknown input file format!" << endl;
                            exit(1);
    }   
    return NULL;
}

void GetAttributes(const NetflowRecord& netflow_record,
        NetflowAttbs& netflow_attributes)
{
    netflow_attributes.num_packets = netflow_record.num_packets;
    netflow_attributes.num_bytes = netflow_record.num_bytes;
    netflow_attributes.duration = netflow_record.duration;
    netflow_attributes.src_port = 1; // atoi(netflow_record.src_port);
    netflow_attributes.dst_port = 1; // atoi(netflow_record.dst_port);
    return;
}

ostream& operator<<(ostream& os, const NetflowAttbs& record)
{
    cout << "src_port=" << record.src_port << endl;
    cout << "dst_port=" << record.dst_port << endl;
    return os;
}

ostream& operator<<(ostream& os, const NetflowRecord& record)
{
    cout << "time=" << record.timestamp << " protocol=" << record.protocol;
    cout << " duration= " << record.duration << endl;
    cout << "src=" << record.src_ip << ":" << record.src_port << endl;
    cout << "dst=" << record.dst_ip << ":" << record.dst_port << endl;
    cout << "stats(num_packets=" << record.num_packets;
    cout << ", num_bytes=" << record.num_bytes << ")" << endl;
    return os;
}

char* strcpy(char *dest, const char *src, size_t n)
{
    strncpy(dest, src, n);
    dest[n] = '\0';
    return dest;
}

bool init_protocol_types = false;
map<string, int> protocol_types;

int GetProtocolType(const char* protocol)
{
    if (init_protocol_types == false) {
        protocol_types.insert(pair<string, int>("ICMP", 0));
        protocol_types.insert(pair<string, int>("TCP", 1));
        protocol_types.insert(pair<string, int>("UDP", 2));
        protocol_types.insert(pair<string, int>("IPv6", 3));
        protocol_types.insert(pair<string, int>("99", 4));
        protocol_types.insert(pair<string, int>("AH", 5));
        protocol_types.insert(pair<string, int>("ESP", 6));
        protocol_types.insert(pair<string, int>("GRE", 7));
        init_protocol_types = true;
    }
    map<string, int>::iterator it = 
           protocol_types.find(protocol);
    int id; 

    if (it == protocol_types.end()) {
        cout << "Failed to find protocol = " << protocol << endl;
        exit(1);
        // id = protocol_types.size();
        // protocol_types.insert(pair<string, int>(protocol, id));
    }   
    else {
        id = it->second;
    }   
    assert(id >= 0);
    assert(id < 8);
    return id; 
}

time_t ParseTimestamp(const char* line)
{
    struct tm tdata;
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
    tdata.tm_isdst = -1; 
    return mktime(&tdata);
}

bool ParseNetflow(char* line, NetflowRecord& record)
{
    for (int i = 0; i < 4; i++) {
        if (!isdigit(line[i])) {
            return false;
        }
    }
    char* ptr = line;
    char* beg = NULL;
    char* dummy;
    char* sep;
    char  tmp[32];
    int field = 0;
    char timestamp[32];
    char protocol[32];
    std::fill(timestamp, timestamp + 32, ' ');

    while (*ptr != '\0') {
        if (*ptr != ' ' && !beg) {
            beg = ptr; 
        }
        else if (*ptr == ' ' && beg) {
            field++;
            switch (field) {
                case 1 :  
                    memcpy(timestamp, beg, ptr - beg + 1);
                    break;
                case 2 :
                    strcpy(timestamp + 11, beg, ptr - beg);
                    break;
                case 3 :
                    strcpy(tmp, beg, ptr-beg);
                    record.duration = strtod(tmp, &dummy);
                    break;
                case 4: 
                    strcpy(protocol, beg, ptr-beg);
                    break;
                case 5: 
                    sep = strstr(beg, ":");
                    strcpy(record.src_ip, beg, sep-beg);
                    strcpy(record.src_port, sep+1, ptr-sep-1);
                    break;
                case 7: 
                    sep = strstr(beg, ":");
                    strcpy(record.dst_ip, beg, sep-beg);
                    strcpy(record.dst_port, sep+1, ptr-sep-1);
                    break;
                case 8: 
                    strcpy(tmp, beg, ptr-beg);
                    record.num_packets = atoi(tmp);
                    break;
                case 9:
                    strcpy(tmp, beg, ptr-beg);
                    record.num_bytes = atoi(tmp);
                    break;
            }
            beg = NULL;
        }
        ptr++;
    }

    record.timestamp = ParseTimestamp(timestamp); 
    record.protocol = GetProtocolType(protocol);
    return field == 9;
}

bool ParseVast(char* line, NetflowRecord& record)
{

    char* tokens[15];
    int num_tokens = split(line, "|", tokens);

    if (num_tokens != 15) {
        cout << "num_tokens = " << num_tokens << "line =" << line << endl;
        return false;
    }
    record.timestamp= atoi(tokens[0]);
    record.protocol = atoi(tokens[1]);
    strcpy(record.src_ip, tokens[2]);
    strcpy(record.dst_ip, tokens[3]);
    strcpy(record.src_port, tokens[4]);
    strcpy(record.dst_port, tokens[5]);
    record.num_bytes = atoi(tokens[9]) + atoi(tokens[10]);
    record.num_packets= atoi(tokens[11]) + atoi(tokens[12]);
    return true; 
}
/*
bool MatchEdge(Edge& e_s, map<string, string> constraints)
{
    bool match = true;
    EdgeData edge_data = e_s.edge_data;
    for (key in keySet) {
        if (edge_data.key != constraings.get(key)) {
            match = false;
            break;
        }
    }
    return match;
}
*/

struct edge {
    string u_str, v_str;
    int u, v, cost;
    long timestamp;
};

void MetisWriter(const char* outpath, const map<string, int>& id_map,
        const vector<edge>& edges)
{
    FILE* fout = fopen(outpath, "w");
    if (!fout) {
        cout << "Failed to open: " << outpath << endl;
        exit(1);
    }

    map<int, set<int> > adj_lists;
    
    for (int i = 0, N = edges.size(); i < N; i++) {
        int u = edges[i].u;
        int v = edges[i].v;
        adj_lists[u].insert(v);
        adj_lists[v].insert(u);
    }

    int num_edges = 0;
    for (map<int, set<int> >::iterator it = adj_lists.begin();
            it != adj_lists.end(); it++) {
        num_edges += it->second.size();
    }
    num_edges >>= 1;
    fprintf(fout, "%lu %d\n", adj_lists.size(), num_edges);
   
    for (map<int, set<int> >::iterator it = adj_lists.begin();
            it != adj_lists.end(); it++) {
        set<int>& adj_list = it->second;
        vector<int> tmp_list;
        for (set<int>::iterator nbr_it = adj_list.begin();
                nbr_it != adj_list.end(); nbr_it++) {
            tmp_list.push_back(*nbr_it + 1);
        }  
        for (int i = 0, N = tmp_list.size(); i < (N-1); i++) {
            fprintf(fout, "%d ", tmp_list[i]);
        }
        fprintf(fout, "%d\n", tmp_list[adj_list.size()-1]);
    }

    fclose(fout);
}

void DimacsWriter(const char* outpath, const map<string, int>& id_map, 
            const vector<edge>& edges)
{
    FILE* fout = fopen(outpath, "w");
    if (!fout) {
        cout << "Failed to open: " << outpath << endl;
        exit(1);
    }

    char key[256];
    map<string, int> edge_table;;
    map<string, int>::iterator edge_it;
    for (int i = 0, N = edges.size(); i < N; i++) {
        sprintf(key, "a %d %d", edges[i].u + 1, edges[i].v + 1);
        edge_it = edge_table.find(key);
        if (edge_it == edge_table.end()) {
            edge_table[key] = edges[i].cost;
        }
        else {
            edge_it->second += edges[i].cost;
        }
        // fprintf(fout, "a %d %d %d\n", edges[i].u, edges[i].v, edges[i].cost);
    }

    fprintf(fout, "p sp %lu %lu\n", id_map.size(), edges.size());
    for (edge_it = edge_table.begin(); edge_it != edge_table.end(); 
            edge_it++) {
        fprintf(fout, "%s %d\n", edge_it->first.c_str(), edge_it->second);
    }
    fclose(fout);
}

void WeightWriter(const char* outpath, const map<string, int>& id_map, 
            const vector<edge>& edges)
{
    FILE* fout = fopen(outpath, "w");
    if (!fout) {
        cout << "Failed to open: " << outpath << endl;
        exit(1);
    }

    char key[256];
    map<string, int> edge_table;;
    map<string, int>::iterator edge_it;
    for (int i = 0, N = edges.size(); i < N; i++) {
        sprintf(key, "%d %d", edges[i].u+1, edges[i].v+1);
        edge_it = edge_table.find(key);
        if (edge_it == edge_table.end()) {
            edge_table[key] = edges[i].cost;
        }
        else {
            edge_it->second += edges[i].cost;
        }
    }

    for (edge_it = edge_table.begin(); edge_it != edge_table.end(); 
            edge_it++) {
        fprintf(fout, "%s %d\n", edge_it->first.c_str(), edge_it->second);
    }
    fclose(fout);
}

void TsvWriter(const char* outpath, const map<string, int>& id_map, 
            const vector<edge>& edges)
{
    FILE* fout = fopen(outpath, "w");
    if (!fout) {
        cout << "Failed to open: " << outpath << endl;
        exit(1);
    }

    char key[256];
    set<string> edge_table;;
    set<string>::iterator edge_it;
    for (int i = 0, N = edges.size(); i < N; i++) {
        sprintf(key, "%d %d", edges[i].u, edges[i].v);
        edge_table.insert(key);
    }

    // for (int i = 0, N = edges.size(); i < N; i++) {
    for (set<string>::iterator it = edge_table.begin(); it != edge_table.end();
            it++) {
        //fprintf(fout, "%d %d\n", edges[i].u, edges[i].v);
        fprintf(fout, "%s\n", it->c_str());
    }
    fclose(fout);
}

int GetId(map<string, int>& id_map, const string& str)
{
    int id;
    map<string, int>::iterator id_map_it = id_map.find(str);;
    if (id_map_it == id_map.end()) {
        id = id_map.size();
        id_map[str] = id;
    }
    else {
        id = id_map_it->second;
    }
    return id;
}

edge MakeEdge(const string& src_ip, const string& dst_ip,
        int cost, long timestamp, map<string, int>& id_map)
{
    edge e;
    e.u = GetId(id_map, src_ip);
    e.v = GetId(id_map, dst_ip);
    e.cost = cost;
    e.timestamp = timestamp;
    e.u_str = src_ip;
    e.v_str = dst_ip;
    return e;
}

void StoreIdMap(const char* dirpath, const char* prefix, 
        const map<string, int>& id_map)
{
    char path[512];
    sprintf(path, "%s/%s_id_map.csv", dirpath, prefix);
    ofstream ofs(path, ios_base::out);
    if (ofs.is_open() == false) {
        cout << "ERROR Failed to open: " << path << endl; 
        return;
    }

    for (map<string,int>::const_iterator it = id_map.begin();
            it != id_map.end(); it++) {
        ofs << it->first << "," << it->second << endl;
    }
    ofs.close();
}

class Netflow2ClusterVectorMapper {
public:
    Netflow2ClusterVectorMapper() {}
    void Map(const NetflowRecord& record, int* features)
    {
/*
        map<string, int>::iterator it = 
                protocol_map__.find(record.protocol);
        int protocol_id;
        if (it == protocol_map__.end()) {
            protocol_id = protocol_map__.size();
            protocol_map__[record.protocol] = protocol_id;
        }
        else {
            protocol_id = it->second; 
        }
*/
        features[0] = record.protocol;
        features[1] = atoi(record.src_port);
        features[2] = atoi(record.dst_port);
        features[3] = record.num_packets;
        int num_bytes = record.num_bytes;
        int num_bits = 0;
        while (num_bytes) {
            num_bytes >>= 1;
            num_bits++;
        } 
        features[4] = num_bits;
        return;
    }

private:
    map<string, int> protocol_map__;
};

void BuildClusteringInput(const char* path, RECORD_FORMAT input_format,
        const char* clustering_input_path)
{
    LineParser parser = GetLineParser(input_format);

    FILE* fp = fopen(path, "r");
    if (!fp) {
        cout << "ERROR Failed to open: " << path << endl;
        return;
    }

    ofstream cluster_fs(clustering_input_path, ios_base::out);
    if (cluster_fs.is_open() == false) {
        cout << "ERROR Failed to write: " << clustering_input_path << endl;
        return;
    }

    char buf[1024];
    int features[5];
    Netflow2ClusterVectorMapper mapper;
    
    int row_id = 0;
    while (fgets(buf, 1024, fp)) {
        NetflowRecord record;
        if (!parser(buf, record)) continue;
        mapper.Map(record, features);
        row_id++;
        cluster_fs << row_id << "," << features[0] << "," << features[1] << "," 
                << features[2] << "," << features[3] << "," 
                << features[4] << endl;
    }

    cluster_fs.close();
    return;
}

int* LoadEdgeClusterIds()
{
    string cluster_out = "/pic/projects/mnms4graphs/nccdc/2013/clustering/cluster_out.txt_1_of_1";
    ifstream ifs;
    int num_edges = 0;
    char buf[256];
    ifs.open(cluster_out.c_str(), ios_base::in);
    while (ifs.good()) {
        ifs.getline(buf, 256);
        if (buf[0] != '\0') {
            num_edges++;
        }
    } 
    ifs.close();
    int* edge_cluster_ids = new int[num_edges]; 
    char* saveptr;
    ifs.open(cluster_out.c_str(), ios_base::in);
    while (ifs.good()) {
        ifs.getline(buf, 256);
        if (buf[0] != '\0') {
            char* ptr = strtok_r(buf, "\t", &saveptr);
            int edge_id = atoi(ptr);
            ptr = strtok_r(NULL, "\t", &saveptr);
            int cluster_id = atoi(ptr);
            edge_cluster_ids[edge_id] = cluster_id;
        }
    } 
    ifs.close();
    cout  << "Returning cluster ids" << endl;
    return edge_cluster_ids;
}

void Export(const char* inpath, RECORD_FORMAT format, 
        const char* dirpath, const char* prefix,
        OUTPUT_FORMAT out_format, int window_len, bool is_sliding)
{
    LineParser parser = GetLineParser(format);
    void (*OutputWriter)(const char* outpath, const map<string, int>& id_map, 
            const vector<edge>& edges);
    string extension;
    switch(out_format) {
        case DIMACS:    OutputWriter = DimacsWriter;
                        extension = "gr";
                        cout << "Writer -> DimacsWriter" << endl;
                        break;
        case TSV:       OutputWriter = TsvWriter;
                        extension = "tsv";
                        cout << "Writer -> TsvWriter" << endl;
                        break;
        case METIS:     OutputWriter = MetisWriter;
                        extension = "graph";
                        cout << "Writer -> MetisWriter" << endl;
                        break;
        case WEIGHT:    OutputWriter = WeightWriter;
                        extension = "weight";
                        cout << "Writer -> WeightWriter" << endl;
                        break;
        default:        cout << "Unknown output file format!" << endl;
                        break;
    }
    FILE* fp = fopen(inpath, "r");
    if (!fp) {
        cout << "Failed to open: " << inpath << endl;
        exit(1);
    }
    char buf[1024];
    map<string, int> id_map;
    int file_id = 0;
    edge e;
    vector<edge> edges;
    int sliding_len = is_sliding ? (window_len >> 1) : 0;
    cout << "Window length = " << window_len << " seconds" << endl;
    cout << "Sliding window by = " << sliding_len << " seconds" << endl;
    long first = -1;
    long latest;
    mkdir(dirpath, 0774);

    while (fgets(buf, 1024, fp)) {
        NetflowRecord record;
        if (!parser(buf, record)) continue;

        // struct tm tinfo;
        // strptime(record.timestamp, "%Y-%m-%d %H:%M:%S", tinfo);
        latest = record.timestamp;
        if (first == -1) {
            first = latest;
        }

        e = MakeEdge(record.src_ip, record.dst_ip, record.num_bytes, 
                latest, id_map);
        edges.push_back(e);

        if (latest > (first + window_len)) {
                        
            char outpath[128];
            sprintf(outpath, "%s/%s_%d.%s", dirpath, prefix, 
                    file_id++, extension.c_str());
            cout << "Writing " << edges.size() << " to " << outpath << endl;
            OutputWriter(outpath, id_map, edges);
            if (!(out_format == TSV || out_format == METIS)) {
                id_map.clear();
            }
            if (sliding_len) {
                vector<edge> buffer;
                long keep_first = first + sliding_len;
                for (int i = 0, N = edges.size(); i < N; i++) {
                    const edge& e_h = edges[i];
                    if (e_h.timestamp >= keep_first) {
                        e = MakeEdge(e_h.u_str, e_h.v_str, e_h.cost,
                                e_h.timestamp, id_map);
                        buffer.push_back(e);
                    }
                }
                edges.clear();
                cout << "Retaining " << buffer.size() << " edges" << endl;
                for (int i = 0, N = buffer.size(); i < N; i++) {
                    edges.push_back(buffer[i]);
                }
            }
            else {
                edges.clear();
            }
            first = -1;
        }
    }
    fclose(fp);
    StoreIdMap(dirpath, prefix, id_map);
}

void FindClustersOverTime(const char* inpath, RECORD_FORMAT format, 
        const char* dirpath, const char* prefix,
        OUTPUT_FORMAT out_format, int window_len, bool is_sliding)
{
    bool (*LineParser)(char* line, NetflowRecord& record);
    switch (format) {
        case NETFLOW:       LineParser = ParseNetflow;
                            cout << "LineParser -> NetflowParser" << endl;
                            break; 
        case NCCDC:         LineParser = ParseNCCDC;
                            cout << "LineParser -> ParseNCCDC" << endl;
                            break; 
        case VAST:         LineParser = ParseVast;
                            cout << "LineParser -> ParseVast" << endl;
                            break; 
        case SILK:         LineParser = ParseSilk;
                            cout << "LineParser -> ParseSilk" << endl;
                            break; 
        case CONNECTIONS:         LineParser = ParseConnections;
                            cout << "LineParser -> ParseConnections" << endl;
                            break; 
        case WIN_EVNT_LOGS: LineParser = ParseWindowsEvents;
                            cout << "LineParser -> ParseWindowsEvents" << endl;
                            break; 
        default:            cout << "Unknown input file format!" << endl;
                            break;
    }
    void (*OutputWriter)(const char* outpath, const map<string, int>& id_map, 
            const vector<edge>& edges);
    string extension;
    switch(out_format) {
        case DIMACS:    OutputWriter = DimacsWriter;
                        extension = "gr";
                        cout << "Writer -> DimacsWriter" << endl;
                        break;
        case TSV:       OutputWriter = TsvWriter;
                        extension = "tsv";
                        cout << "Writer -> TsvWriter" << endl;
                        break;
        case METIS:     OutputWriter = MetisWriter;
                        extension = "graph";
                        cout << "Writer -> MetisWriter" << endl;
                        break;
        case WEIGHT:    OutputWriter = WeightWriter;
                        extension = "weight";
                        cout << "Writer -> WeightWriter" << endl;
                        break;
        default:        cout << "Unknown output file format!" << endl;
                        break;
    }
    FILE* fp = fopen(inpath, "r");
    if (!fp) {
        cout << "Failed to open: " << inpath << endl;
        exit(1);
    }
    char buf[1024];
    map<string, int> id_map;
    int file_id = 0;
    edge e;
    vector<edge> edges;
    int sliding_len = is_sliding ? (window_len >> 1) : 0;
    cout << "Window length = " << window_len << " seconds" << endl;
    cout << "Sliding window by = " << sliding_len << " seconds" << endl;
    long first = -1;
    long latest;
    mkdir(dirpath, 0774);

    char cluster_stats_path[256];
    sprintf(cluster_stats_path, "cluster_stats_win%d.csv", window_len);
    ofstream cluster_stats_fs(cluster_stats_path, ios_base::out);
    int num_clusters = 40;
    int* edge_cluster_ids = LoadEdgeClusterIds();
    int* cluster_stats = new int[num_clusters];
    for (int i = 0; i < num_clusters; i++) {
        cluster_stats[i] = 0;
    }

    int edge_id = 0;
    while (fgets(buf, 1024, fp)) {
        NetflowRecord record;
        if (!LineParser(buf, record)) continue;

        // struct tm tinfo;
        // strptime(record.timestamp, "%Y-%m-%d %H:%M:%S", tinfo);
        latest = record.timestamp;
        if (first == -1) {
            first = latest;
        }

        cluster_stats[edge_cluster_ids[edge_id++]] += 1;

        if (latest > (first + window_len)) {
                
            for (int i = 0; i < (num_clusters-1); i++) {
                cluster_stats_fs << cluster_stats[i] << ",";
            }
            cluster_stats_fs << cluster_stats[num_clusters-1] << endl;
         
            for (int i = 0; i < num_clusters; i++) {
                cluster_stats[i] = 0;
            }
            first = -1;
        }
    }
    cluster_stats_fs.close();
    fclose(fp);
    delete[] edge_cluster_ids;
    delete[] cluster_stats;
}

bool ParseWindowsEvents(char* line, NetflowRecord& record)
{
    if (line[0] != 'a') return false;

/* 
 a 'Source IP' 'Host IP' 'Timestamp' 'Event ID' 'Event Log Type' 'Hos    tname' 'NSM Type' 'User' 'Domain' 'Logon Type' 'Logon Guid' 'Source     Port' 'Authentication Package' 'Is $ User' 'Confidence'
 */
    string tokens[20];
    stringstream sstrm(line);
    string token;
    int next = 0;
    while (sstrm.good()) {
        sstrm >> token;
        tokens[next++] = token;
    }
    strcpy(record.src_ip, tokens[1].c_str());
    strcpy(record.dst_ip, tokens[2].c_str());
    char* dummy;
    record.timestamp = strtol(tokens[3].c_str(), &dummy, 10);
    // strcpy(record.timestamp, tokens[3].c_str());
    // strcpy(record.protocol, tokens[4].c_str());
    record.protocol = atoi(tokens[4].c_str());
    strcpy(record.src_port, tokens[12].c_str());
    strcpy(record.dst_port, "1234");
    record.num_packets = 1;
    record.num_bytes = 1;
    record.duration = 1;
    return true; 
}

bool ParseSilk(char* line, NetflowRecord& record)
{
    char* tokens[13];
    int num_tokens = split(line, "|", tokens);
    if (num_tokens != 13) {
 //cout << "num_tokens = " << num_tokens << endl;
        return false;
    }
    strcpy(record.src_ip, Strip(tokens[0]));
    strcpy(record.dst_ip, Strip(tokens[1]));
    // strcpy(record.timestamp, tokens[4]);
    char* dummy;
    record.timestamp = strtol(Strip(tokens[8]), &dummy, 10);
    // strcpy(record.protocol, tokens[10]);
    record.protocol = atoi(Strip(tokens[4]));
    strcpy(record.src_port, Strip(tokens[2]));
    strcpy(record.dst_port, Strip(tokens[3]));
    record.num_packets = atoi(Strip(tokens[5]));
    record.num_bytes = atoi(Strip(tokens[6]));
    time_t beg, end;
    beg = 0;
    end = 0;
    record.duration = atof(Strip(tokens[9]));
    return true;
}

bool ParseNCCDC(char* line, NetflowRecord& record)
{
    char* tokens[15];
    int num_tokens = split(line, "|", tokens);
    if (num_tokens != 14) {
        cout << "num_tokens = " << num_tokens << endl;
        return false;
    }
    strcpy(record.src_ip, tokens[6]);
    strcpy(record.dst_ip, tokens[7]);
    // strcpy(record.timestamp, tokens[4]);
    char* dummy;
    record.timestamp = strtol(tokens[4], &dummy, 10);
    // strcpy(record.protocol, tokens[10]);
    record.protocol = atoi(tokens[10]);
    strcpy(record.src_port, tokens[8]);
    strcpy(record.dst_port, tokens[9]);
    record.num_packets = atoi(tokens[2]);
    record.num_bytes = atoi(tokens[3]);
    time_t beg, end;
    beg = strtol(tokens[4], &dummy, 10);
    end = strtol(tokens[5], &dummy, 10);
    record.duration = end-beg;
    return true;
}

bool ParseConnections(char* line, NetflowRecord& record)
{
    char* tokens[3];
    int num_tokens = split(line, " ", tokens);
    if (num_tokens != 2 or tokens[0] == "#") {

        cout << "num_tokens = " << num_tokens << endl;
        return false;
    }
    char* dummy;
    record.timestamp = strtol(tokens[4], &dummy, 10);
    
    strcpy(record.src_ip, tokens[0]);
    strcpy(record.dst_ip, tokens[1]);
    
    strcpy(record.src_port, "0");
    strcpy(record.dst_port, "0");
    
    record.protocol = 0;
    record.num_packets = 0;
    record.num_bytes = 0;
    record.duration = 0.0;
    return true;
}
