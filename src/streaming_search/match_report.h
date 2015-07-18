#ifndef __MATCH_REPORT_H__
#define __MATCH_REPORT_H__
#include "match.h"
using namespace std;

void PrintSpaces(int num_spaces, ostream& ofs);
void ExportGephiStreaming(const MatchSet* match_set, int node);
void ExportMapView(const MatchSet* match_set, int node);

#endif
