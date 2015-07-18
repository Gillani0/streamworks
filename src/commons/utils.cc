/**
 * @author Sutanay Choudhury
 * @author Patrick Mackey
 */

#include "utils.h"
#include <assert.h>
#include <stdlib.h>
#include <string>
#include <map>
#include <sstream>
#include <fstream>
#include <sys/stat.h>
#include <sys/time.h>
#include <stdarg.h>
#ifdef __GXX_EXPERIMENTAL_CXX0X__
#include <regex>
#else
#include <boost/regex.hpp>
#endif
using namespace std;
// using namespace boost;

#define MAX_LEN 1024*1024

/** Routine for performing int_fetch_add that works on
    either XMT or OpenMP.  Threadsafe way to find
    the value of an integer and increase it.
    @param mem  The integer data we want to increment
    @param incr The amount to increment by
    @return The value before being incremented */
inline int int_fetch_add_mt(int* mem, int incr)
{
    #ifdef __MTA__
        return int_fetch_add(mem, incr);
    #elif _OMP
        int val = *mem; 
        #pragma omp atomic
        *mem += incr;
        return val;
    #else
        int val = *mem; 
        *mem += incr;
        return val;
    #endif
}

/** Loads the given file
    @param filename     File name of file to load
    @param len  Output variable is set to the number of bytes loaded
    @return Returns the bytes of the file */
char* load_file(const char* filename, int* len)
{
    char* buf;
    #ifdef __MTA__
    buf = snapLoad(filename, len); 
    #else
    struct stat fileStat;
    if (stat(filename, &fileStat) < 0) {
        cerr << "Failed to obtain size for : " << filename << endl;
        return NULL;
    }   
    if (!fileStat.st_size) {
        cerr << "File has zero bytes : " << filename << endl;
        return NULL;
    }   
    int sz = fileStat.st_size + 1;
    buf = new char[sz+1];
    if (buf == NULL) {
        cerr << "Failed to allocate memory for loading file : " << filename
             << " [" << sz << " bytes]" << endl;
        return NULL;
    }   

    FILE* fp = fopen(filename, "r");
    if (fp == NULL) {
        cerr << "Failed to open file : " << filename << endl;
        delete[] buf;
        return NULL;
    }   

    size_t nread = fread(buf, 1, sz-1, fp);
    fclose(fp);
    if (nread != (sz-1)) { 
        cerr << "Failed to read complete file into memory : " 
             << filename << " [" << (sz-1) << " bytes]" << endl;
        delete[] buf;
        return NULL;
    }   
    // cout << "Returning buffer with " << sz << " bytes" << endl; 
    buf[sz-1] = '\0';
    *len = sz + 1;
    #endif
    return buf;
}

tv TV() {
    tv t;   
    gettimeofday(&t, NULL);
    return t;
}

long get_tv_diff(struct timeval tv_start, struct timeval tv_finish) {
    return (long)(( 1E6*(tv_finish.tv_sec-tv_start.tv_sec)
                               + (tv_finish.tv_usec-tv_start.tv_usec) ) );
    // return (long)( 0.001*( 1E6*(tv_finish.tv_sec-tv_start.tv_sec)
                               // + (tv_finish.tv_usec-tv_start.tv_usec) ) );
}

void split(char* line, const char* delim, vector<string>& tokens)
{
    char* store;
    char* ptr = strtok_r(line, delim, &store);
    tokens.push_back(ptr);
    while ((ptr = strtok_r(NULL, delim, &store))) {
        tokens.push_back(ptr);
    }
}


int split(char* line, const char* sep, char* tokens[], int tokensReserved)
{
    // Initialize all token pointers to NULL.
    // This prevents potential seg faults later.
    for(int i=0; i<tokensReserved; i++) {
        tokens[i] = NULL;
    }   
    
    int next = 0;
    char* last;
    for (char* ptr = strtok_r(line, sep, &last); ptr;
         ptr = strtok_r(NULL, sep, &last)) {
             if(next >= tokensReserved) {
                 cerr << "Cannot finish parsing the line: " << line << endl;
                 cerr << "The number of variable exceeds the amount of tokens reserved: " 
                         << tokensReserved << endl;
                 return next;
             }
             tokens[next++] = ptr; 
         }   
    return next;
}

int split(char* line, const char* sep, char* tokens[])
{
    int next = 0;
    char* last;
    for (char* ptr = strtok_r(line, sep, &last); ptr;
         ptr = strtok_r(NULL, sep, &last)) {
             tokens[next++] = ptr; 
    }   
    return next;
}

char** split(char* buf, int len, char delim, int* numSplits)
{
    *numSplits = 0;

    if (buf[len-1] == '\0') len--;
    if (len <= 0) {
        return NULL;
    }

    int numLines = 0;
    int i;
    for (i = len -1; i >= 0; i--) {
        if (buf[i] == delim) {
            break;
        }
    }
    int boundary = i+1;
    if (boundary < len) numLines += 1;

#pragma mta assert nodep
    for (int j = 0; j < boundary; j++) {
        numLines += (buf[j] == delim) ? 1 : 0;
    }        
            
    char** strings = new char*[numLines];

    int line = 0;
    strings[line++] = buf;

#pragma mta assert nodep
    for (int j = 0, lim = len-1; j <= lim; j++) {
    if ((buf[j] == delim) && (j != lim)) { 
        buf[j] = '\0'; 
#ifdef __MTA__
            int idx = int_fetch_add(&line, 1);
#else
            int idx = line++;
#endif
            strings[idx] = buf + j + 1;
        }       
    }
    *numSplits = numLines;
    return strings;
}

int** initMatrix(int rows, int cols)
{
    int** matrix = new int*[rows];
    matrix[0] = new int[rows*cols];

    for (int i = 1; i < rows; i++) {
        matrix[i] = matrix[0] + i*cols;
    }

    for (int i = 0, n = rows*cols; i < n; i++) {
        matrix[0][i] = 0;
    }

    return matrix;
}

void freeMatrix(int** matrix)
{
    delete[] matrix[0];
    delete[] matrix;
}

/*
File :: File(const char* p, char* b, char** l, int n) :
        path(p), buf(b), lines(l), num_lines(n)
{
}

File :: File(const File& f) : 
        path(f.path), buf(f.buf), lines(f.lines), num_lines(f.num_lines)
{
}

File :: ~File() 
{
    cout << "Freeing memory for in-memory file ..." << endl;
    delete[] lines;
    delete[] buf;
}

File get_lines(const char* filepath, bool order_lines)
{
    int len = 0;
    char* buf = load_file(filepath, &len);

    // Remove an extra null character at the buffer end if put by snapshot lib
    if (buf[len-1] == '\0') len--;

    if (len <= 0) return File(filepath, NULL, NULL, 0);

    int num_lines = 1;
    struct timeval t1, t2;
    int lcount[2];
    lcount[0] = 0;
    lcount[1] = 0;
    gettimeofday(&t1, NULL);
    #pragma mta assert parallel
    for (int i = 0, N = len-1; i <= N; i++) {
        if (buf[i] == '\n' && i != N) {
            if (i & 0x01) {
                lcount[1] += 1;
            }
            else {  
                lcount[0] += 1;
            }
            // num_lines += 1;
        }
    }
    num_lines = lcount[0] + lcount[1];

    gettimeofday(&t2, NULL);
    std::pair<int, char*>* line_ptrs;
    char** lines = new char*[num_lines];
    int count = 0;

    if (order_lines) {
        #pragma mta assert par_newdelete
        std::pair<int, char*>* line_ptrs = new std::pair<int, char*>[num_lines];
        line_ptrs[count++] = std::pair<int, char*>(0, buf);
    }
    else {
        lines[count++] = buf;
    }

    #pragma mta assert parallel
    for (int i = 0, N = len-1; i <= N; i++) {
        if (buf[i] == '\n' && i != N) {
            buf[i] = '\0';
            int next;
            #ifdef __MTA__
            next = int_fetch_add(&count, 1);
            #else
            next = count++;
            #endif
            if (order_lines) {
                line_ptrs[next] = std::pair<int, char*>(i+1, buf + i + 1);
            }
            else {
                lines[next] = buf + i + 1;
            }
        }
    }

    if (order_lines) {
        #ifdef __MTA__
        mtgl_sort<std::pair<int, char*> >(line_ptrs, num_lines); 
        for (int i = 0, N = num_lines; i < N; i++) {
            lines[i] = line_ptrs[i].second;
        }
        #endif
        #pragma mta assert par_newdelete
        delete[] line_ptrs;
    }

    return File(filepath, buf, lines, num_lines);
}
*/

void flog(FILE* fp, int flush, const char* format, va_list args)
{
    time_t now;
    time(&now);
    struct tm tdata;
    localtime_r(&now, &tdata);
    char timestamp[20];
    sprintf(timestamp, "%04d/%02d/%02d %02d:%02d:%02d", tdata.tm_year+1900,
            tdata.tm_mon+1, tdata.tm_mday, tdata.tm_hour, tdata.tm_min,
            tdata.tm_sec);
  
    fprintf(fp, "%s ", timestamp);
    vfprintf(fp, format, args);

    if (flush) {
        fflush(fp);
    }
    return; 
}

void log_msg(const char* format, ...)
{
    va_list args;
    va_start(args, format);
    flog(stderr, 1, format, args);
    va_end(args);
}

void log_err(const char* format, ...)
{
    va_list args;
    va_start(args, format);
    flog(stderr, 0, format, args);
    va_end(args);
}

void dump(CompactMap& map, const char* file)
{
    FILE* fp = fopen(file, "w"); 

    int nodeCount = map.nodeCount;
    int* offsets = new int[nodeCount];
    int* numNeighbors = map.numNeighbors;
    int** neighbors = map.neighbors;

    offsets[0] = 0;
    for (int i = 1; i < nodeCount; i++) {
        offsets[i] = offsets[i-1] + numNeighbors[i-1];
    }

    fprintf(fp, "%d,%d\n", nodeCount, (offsets[nodeCount-1] + numNeighbors[nodeCount-1]));
  
    for (int i = 0; i < nodeCount; i++) {
        fprintf(fp, "%d,%d,%d", i, numNeighbors[i], offsets[i]);
        for (int j = 0, nbrCnt = numNeighbors[i]; j < nbrCnt; j++) {
            fprintf(fp, ",%d", neighbors[i][j]);
        }
        fprintf(fp, "\n");
    }
    
    fclose(fp);
}

void dump(const AdjMatGraph& graph, const char* file)
{
    FILE* fp = fopen(file, "w");
    int nodeCnt = graph.nodeCount;
    fprintf(fp, "%d\n", nodeCnt);
    int** matrix = graph.adjMatrix;

    for (int i = 0; i < nodeCnt; i++) {
        for (int j = 0; j < nodeCnt; j++) {
            if ((i != j) && matrix[i][j]) { 
                fprintf(fp, "%d,%d\n", i, j);
            }
        }
    }
    fclose(fp);
}

ostream& operator<<(ostream& os, const CompactMap& map)
{
    cerr << map.nodeCount << endl;
    int nodeCount = map.nodeCount;
    int* offsets = new int[nodeCount];
    int* numNeighbors = map.numNeighbors;
    int** neighbors = map.neighbors;

    offsets[0] = 0;
    for (int i = 1; i < nodeCount; i++) {
        offsets[i] = offsets[i-1] + numNeighbors[i-1];
    }

    os << nodeCount << "," << (offsets[nodeCount-1] + numNeighbors[nodeCount-1]) << endl;
  
    for (int i = 0; i < nodeCount; i++) {
        os << i << "," << numNeighbors[i] << "," << offsets[i]; 
        for (int j = 0, nbrCnt = numNeighbors[i]; j < nbrCnt; j++) {
            os << "," << neighbors[i][j];
        }
        os << endl;
    }

    return os;
}

/*
CompactMap* loadCompactMap(const char* filepath)
{
    int buflen = 0;
    // char* buf = load_file(filepath, &buflen);
    File file = get_lines(filepath);
    int numLines = file.num_lines;
    char** lines = file.lines;;
    int nodeCnt, totalNbrCnt;
    sscanf(lines[0], "%d,%d", &nodeCnt, &totalNbrCnt);

    int** neighbors = new int*[nodeCnt];
    neighbors[0] = new int[totalNbrCnt];
    int* numNeighbors = new int[nodeCnt];
    assert(numLines == (nodeCnt + 1));
   
    for (int i = 1; i < numLines; i++) {
        int len = strlen(lines[i]);
        if (lines[i][len-1] == '\n') lines[i][len-1] = '\0';
        vector<string> tokenV;
        split(lines[i], ",", tokenV);
        int node = atoi(tokenV[0].c_str());
        int nbrCnt = atoi(tokenV[1].c_str());
        numNeighbors[node] = nbrCnt;
        assert(tokenV.size() == (nbrCnt + 3));
        int neighborOffset = atoi(tokenV[2].c_str());
        if (node != 0) neighbors[node] = neighbors[0] + neighborOffset;
        int* nbrList = neighbors[node];

        for (int j = 0; j < nbrCnt; j++) {
            nbrList[j] = atoi(tokenV[j+3].c_str());
        }
    }
    return new CompactMap(nodeCnt, neighbors, numNeighbors);
}

AdjMatGraph* loadAdjMatGraph(const char* filepath)
{
    int buflen = 0;
    char* buf = load_file(filepath, &buflen);
    int numLines;
    char** lines = split(buf, buflen, '\n', &numLines);
    cerr << lines[0] << endl;
    int nodeCnt(0), edgeCnt(0), bidir(0), stat1(-1);
    int nread = sscanf(lines[0], "%d,%d", &nodeCnt, &stat1);

    int** matrix = new int*[nodeCnt];
    matrix[0] = new int[nodeCnt*nodeCnt];
    for (int i = 1; i < nodeCnt; i++) {
        matrix[i] = matrix[i-1] + nodeCnt;
    }
    for (int i = 0; i < nodeCnt; i++) {
        for (int j = 0; j < nodeCnt; j++) {
            matrix[i][j] = 0;
        }
    }
   
    for (int i = 1; i < numLines; i++) {
        int len = strlen(lines[i]);
        if (lines[i][len-1] == '\n') lines[i][len-1] = '\0';
        int src(0), dst(0);
        sscanf(lines[i], "%d,%d", &src, &dst);
        matrix[src][dst] = 1;
    }
    delete[] lines;
    delete[] buf;
    return new AdjMatGraph(nodeCnt, matrix, stat1);
}

bool operator== (CompactMap& map1, CompactMap& map2)
{
    if (map1.nodeCount != map2.nodeCount) return false;
    for (int i = 0, n = map1.nodeCount; i < n; i++) {
        if (map1.numNeighbors[i] != map2.numNeighbors[i]) return false;
    }
    
    for (int i = 0, n = map1.nodeCount; i < n; i++) {
        int n1 = map1.numNeighbors[i];
        int n2 = map1.numNeighbors[i];
        if (n1 != n2) return false;
        for (int j = 0; j < n1; j++) {    
            if (map1.neighbors[i][j] != map2.neighbors[i][j]) return false;
        }
    }
    return true;
}

CompactMap* AoAToCompactMap(const AoA& array_of_arrays)
{
    int total(0);
    int node_count = array_of_arrays.num_slots;
    vector<int>** arrays = array_of_arrays.arrays;
    int* num_neighbors = new int[node_count];
    for (int i = 0; i < node_count; i++) {
        int n = arrays[i]->size();
        total += n;
        num_neighbors[i] = n;
    }

    int** neighbors = new int*[node_count];
    neighbors[0] = new int[total];
    
    for (int i = 0; i < node_count; i++) {
        if (i) {
            neighbors[i] = neighbors[i-1] + num_neighbors[i-1];
        }
        copy(arrays[i]->begin(), arrays[i]->end(), neighbors[i]);
    }

    return new CompactMap(node_count, neighbors, num_neighbors);
}

CompactMap* AdjMatGraphToCompactMap(const AdjMatGraph* adjMatGraph)
{
    int iVertices = adjMatGraph->nodeCount;
    int** matrix = adjMatGraph->adjMatrix;

    int* nbrCnts = new int[iVertices];
    int** neighbors = new int*[iVertices];

    int* tmpOffsets = new int[2*iVertices];
    int* tmpCnts = tmpOffsets + iVertices;

    for (int i = 0; i < iVertices; i++) nbrCnts[i] = 0;

    for (int i = 0; i < iVertices; i++) {
        for (int j = i+1; j < iVertices; j++) {
            if (matrix[i][j] == 1 || matrix[j][i] == 1) {
                #ifdef __MTA__
                int_fetch_add(nbrCnts + i, 1);
                int_fetch_add(nbrCnts + j, 1);
                #else
                nbrCnts[i] += 1;
                nbrCnts[j] += 1;
                #endif
            }
        }
    }

    tmpOffsets[0] = 0;
    int sum = nbrCnts[0];

    for (int i = 1; i < iVertices; i++) {
        tmpOffsets[i] = tmpOffsets[i-1] + nbrCnts[i-1];
        sum += nbrCnts[i];
    }

    neighbors[0] = new int[sum];

    for (int i = 1; i < iVertices; i++) {
        neighbors[i] = neighbors[0] + tmpOffsets[i];
    } 
    
    for (int i = 0; i < iVertices; i++) tmpCnts[i] = 0;

    for (int i = 0; i < iVertices; i++) {
        for (int j = i+1; j < iVertices; j++) {
            if (matrix[i][j] == 1 || matrix[j][i] == 1) {
                #ifdef __MTA__
                int iNbrPos = int_fetch_add(tmpCnts + i, 1);
                int jNbrPos = int_fetch_add(tmpCnts + j, 1);
                #else
                int iNbrPos = tmpCnts[i];
                int jNbrPos = tmpCnts[j];
                tmpCnts[i] += 1;
                tmpCnts[j] += 1;
                #endif
                neighbors[i][iNbrPos] = j;
                neighbors[j][jNbrPos] = i;
            }
        }
    }

    delete[] tmpOffsets;

    return new CompactMap(iVertices, neighbors, nbrCnts);
}

#pragma mta expect parallel context
#pragma mta inline
int CompactMap :: getNeighborOffset(int node, int nbr)
{
    int* list = neighbors[node];
    int neighborCnt = numNeighbors[node];

    int offset = -1;

    for (int i = 0; i < neighborCnt; i++) {
        if (list[i] == nbr) {
            offset = i;
            break;  
        }       
    }
    return (neighbors[node] - neighbors[0] + offset); 
}

#pragma mta expect parallel context
#pragma mta inline
template <typename T>
void swap(T* a, T* b)
{
    T tmp = *a;
    *a = *b;
    *b = tmp;
    return;
}

#pragma mta expect parallel context
#pragma mta inline
void swap_pair(int* key_a, int* val_a, int* key_b, int* val_b)
{
    swap(key_a, key_b);
    swap(val_a, val_b);
    return;
}

void insert_sort(int* keys, int* values, int len)
{
    if (len < 2) {
        return; 
    }
    if (len == 2) { 
        if (keys[0] > keys[1]) {
            swap_pair(keys, values, keys+1, values+1);
        }       
        return; 
    }
    int key;
    int value;
    int j;  

    for (int i = 1; i < len; i++) {
        key = keys[i];
        value = values[i];
        j = i;  

        while ((j > 0) && (keys[j - 1] > key)) { 
            keys[j] = keys[j - 1];
            values[j] = values[j - 1];
            --j;    
        }       
        keys[j] = key;
        values[j] = value;
    }
    return; 
}

bool IsVertexFile(const char* path)
{
    bool stat;
    if (strstr(path, ".vertices")) {
        stat = true;
    }   
    else if (strstr(path, ".edges")) {
        stat = false;
    }   
    // Mackey 1/9/13: Added error checking
    else {
        cerr << "ERROR: " << path << " is unknown file type." << endl
             << "Vertex file names must end with *.vertices" << endl
             << "Edge file names must end with *.edges" << endl;
        exit(EXIT_FAILURE);
    }   

    return stat;
}
*/

void GetFileList(const char* input_path, vector<string>& file_list)
{
    if (!input_path) {
        return;
    }

    bool process_list = false;
    int len = strlen(input_path);

    if (len > 5) {
        if (!strcmp(input_path + len - 5, ".list")) {
            process_list = true;
        }
    }

    if (process_list == false) {
        file_list.push_back(input_path);
        return;
    }

    FILE* fp = fopen(input_path, "r");

    if (fp == NULL) {
        cerr << "GetFileList::Failed to open file containing batch file list : "
        << input_path << endl;
        return;
    }

    char line[MAX_LEN];

    while (fgets(line, MAX_LEN, fp)) {
        if (strlen(line)) {
            string filepath;
            int len = strlen(line);
            if (line[len-1] == '\n') {
                filepath = string(line, len-1);
            }
            else {
                filepath = string(line);
            }
            file_list.push_back(filepath);
        }
    }

    fclose(fp);
    return;
}

void LoadMap(const char* path, map<string, int>& property_map)
{
    ifstream ifs(path, ios_base::in);
    string line;
    while (ifs.good()) {
        getline(ifs, line);
        if (line.size() == 0) continue;
        size_t pos = line.find(",");
        string key = line.substr(0, pos);
        string val = line.substr(pos+1);
        int i_val = atoi(val.c_str());
        property_map.insert(pair<string, int>(key, i_val));
    }
    return;
}

bool FilterLabel(const string& v_s_label, const string& v_q_label)
{
    if (v_q_label.size() == 0) {
        // cout << "QUERY LABEL = [NONE]" << endl;
        return true;
    }   
    else {
        bool label_check;
        // cout << "QUERY LABEL = [" << v_q_label << "]" << endl;
        // if (!strncmp("re:", v_q_label.c_str(), 3)) {
            // label_check = regex_match(v_s_label.c_str(), 
                    // basic_regex(v_q_label.c_str() + 3));
        // }   
        // else {
            label_check = v_q_label == v_s_label;
        // }   
        return label_check;
    }   
}

bool Exists(const string& path)
{
    struct stat fstat;  
    int result = stat(path.c_str(), &fstat);
    return (result != -1);
}

void Strip(string& line)
{
    int i = 0;
    int beg, end;
    while (line[i] == ' ') {
        i++;
    }   
    beg = i;
    i = line.size()-1;
    while (line[i] == ' ') {
        i--;
    }   
    end = i;
    line = line.substr(beg, end - beg + 1); 
    return;
}

char* Strip(char* line)
{
    int i = 0;
    int beg, end;
    while (line[i] == ' ') {
        i++;
    }   
    beg = i;
    i = strlen(line) - 1;
    while (line[i] == ' ') {
        i--;
    }   
    end = i+1;
    for (int i = 0; i < (end-beg); i++) {
        line[i] = line[i + beg];
    }
    line[end-beg] = '\0';
    return line;
}
