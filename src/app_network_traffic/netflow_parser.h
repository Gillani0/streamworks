#ifndef __NETFLOW_PARSER_H__
#define __NETFLOW_PARSER_H__

#include <iostream>
#include <fstream>
#include <set>
#include <vector>
#include <map>
#include <time.h>
#include "schema_netflow.h"
using namespace std;

// 2013-01-01 04:18:34.912
static string netflow_timestamp_format = "%Y-%m-%d %H:%M:%S";

struct NetflowRecord {
    // char timestamp[32];
    time_t timestamp;
    char src_ip[24];
    char src_port[8];
    char dst_ip[24];
    char dst_port[8];
    // char protocol[8];
    int protocol;
    int  num_packets;
    int  num_bytes;
    float duration;
};

/*
struct NetflowAttbs { 
    time_t timestamp;
    int  num_packets;
    int  num_bytes;
    float duration;
    int src_port;
    int dst_port;

    inline bool operator<(const NetflowAttbs& other) const
    {
        if (timestamp != other.timestamp) {
            return timestamp < other.timestamp;
        }
        else {
            if (src_port != other.src_port) {
                return src_port < other.src_port;
            }
            else {
                return dst_port < other.dst_port;
            }
        }
    }

    inline bool operator==(const NetflowAttbs& other) const
    {
        return (timestamp == other.timestamp && 
                num_bytes == other.num_bytes &&
                duration == other.duration && 
                src_port == other.src_port && 
                dst_port == other.dst_port);
    }
};
*/

inline bool operator<(const NetflowAttbs& one, const NetflowAttbs& other) 
{
/*
    if (one.timestamp != other.timestamp) {
            return one.timestamp < other.timestamp;
    }
    else {
*/
        if (one.src_port != other.src_port) {
            return one.src_port < other.src_port;
        }
        else {
            return one.dst_port < other.dst_port;
        }
    // }
}

inline bool operator==(const NetflowAttbs& one, const NetflowAttbs& other)
{
    // return (one.timestamp == other.timestamp && 
    return (one.num_bytes == other.num_bytes &&
            one.duration == other.duration && 
            one.src_port == other.src_port && 
            one.dst_port == other.dst_port);
}

void GetAttributes(const NetflowRecord& netflow_record,
        NetflowAttbs& netflow_attributes);
ostream& operator<<(ostream& os, const NetflowRecord& record);
ostream& operator<<(ostream& os, const NetflowAttbs& record);
bool ParseNetflow(char* line, NetflowRecord& record);
bool ParseWindowsEvents(char* line, NetflowRecord& record);
bool ParseNCCDC(char* line, NetflowRecord& record);
bool ParseSilk(char* line, NetflowRecord& record);
bool ParseVast(char* line, NetflowRecord& record);
bool ParseConnections(char* line, NetflowRecord& record);

enum RECORD_FORMAT {
    NETFLOW,
    NCCDC,
    SILK,
    CONNECTIONS,
    VAST,
    WIN_EVNT_LOGS
};

enum OUTPUT_FORMAT {
    DIMACS,
    TSV,  
    METIS,
    WEIGHT
};

typedef bool (*LineParser)(char* line, NetflowRecord& record);
LineParser GetLineParser(RECORD_FORMAT format);

void BuildClusteringInput(const char* inpath, RECORD_FORMAT format, 
        const char* clustering_input_path);

void FindClustersOverTime(const char* inpath, RECORD_FORMAT format, 
        const char* dirpath, const char* prefix,
        OUTPUT_FORMAT out_format, int window_len, bool is_sliding);
void Export(const char* inpath, RECORD_FORMAT format, 
        const char* dirpath, const char* prefix,
        OUTPUT_FORMAT out_format, int window_len, bool is_sliding);

#endif
