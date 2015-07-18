/**
 * @author Sutanay Choudhury
 * @author Patrick Mackey
 */

#ifndef __XMT_BAYES_UTILS_H__
#define __XMT_BAYES_UTILS_H__

#include <iostream>
#include <iomanip>
#include <algorithm>
#include <vector>
#include <map>
#include <sstream>
#include <string>
#include <iterator>
#include <sys/time.h>
#include <math.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <exception>
#include <fstream>
using namespace std;

#ifdef __MTA__
#include <mtgl/util.hpp>
#include <mtgl/merge_sort.hpp>
using namespace mtgl;

/** Initialize snapshot, needed to load files from the XMT */
void initSnapshot();
/** Load file data for XMT.  Must call initSnapshot() first.
    @param filepath  filename of file
    @param buflen    Output variable is set with length of data in bytes
    @return Returns the bytes of the file */
char* snapLoad(const char* filepath, int* buflen);
/** Saves file data for XMT.  Must call initSnapshot() first.
    @param buf  Data to write
    @param len  Length of data in bytes
    @param filepath Filename of file to save */   
void snapDump(const char* buf, int len, const char* filepath);

#endif

// A macro to disallow the copy constructor and operator= functions
// This should be used in the private: declarations for a class
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName&);               \
  void operator=(const TypeName&)

#define percent(num, denom) (100*num*pow(denom,-1))

/** Short hand for timeval. Holds time information */
typedef struct timeval tv;
/** Returns current timeval */
tv TV();
/** Returns different between start and end time.
    NOTE: Is this in seconds or milliseconds or ...? */
long get_tv_diff(struct timeval tv_start, struct timeval tv_finish); 

/** Allocates a 2D matrix of of the given size */
int** initMatrix(int rows, int cols);
/** Frees memory of 2D matrix */
void freeMatrix(int** matrix);

/** Splits a line with the given delimiter, and stores it in tokens.
    Note: this the least efficient function for doing this. 
    @param line     Input line
    @param delim    The character to split the line by
    @param tokens   A vector of strings that is filled with the tokens from line */    
void split(char* line, const char* delim, vector<string>& tokens);
/** Splits a line with the given delimiter, and stores it in tokens.
    @param line     Input line
    @param delim    The character to split the line by
    @param tokens   Pointers to the tokens in the input line
    @param tokensReserved The size of the tokens array.  This is the maximum amount of 
            tokens we can store. If this is exceeded then a message is output to the user.
    @return Returns the number of tokens found. */
int split(char* line, const char* sep, char* tokens[], int tokensReserved);
/** Splits a line with the given delimiter, and stores it in tokens.
    @deprecated Use int split(char* line, const char* sep, char* tokens[], int tokensLength) instead 
    @param line     Input line
    @param delim    The character to split the line by
    @param tokens   Pointers to the tokens in the input line
    @return Returns the number of tokens found. */
int split(char* line, const char* sep, char* tokens[]);
/** Splits a line with the given delimiter, and returns an array of char* pointers to the tokens
    @param buf      Input line
    @param len      Length of buf in characters
    @param delim    The character to split the line by
    @param numSplits   An output variable that stores the number of tokens found.    
    @return Returns a list of pointers to the char* of each token.
            Warning: This needs to be free'd later. */
char** split(char* buf, int len, char delim, int* numSplits);

/*
struct File {
    File(const char* path = NULL, char* buf = NULL, char** lines = NULL, 
            int nlines = 0);
    File(const File& file);
    ~File();
    string path;
    char* buf;
    char** lines;
    int num_lines;
};

File get_lines(const char* filepath, bool order_lines = false);
*/

void flog(FILE* fp, int flush, const char* format, va_list args);
void log_msg(const char* format, ...);
void log_err(const char* format, ...);

struct CompactMap {
    CompactMap (int cnt, int** nbrs, int* numNbrs) :
        nodeCount(cnt), neighbors(nbrs), numNeighbors(numNbrs)  {}

    ~CompactMap () {
        delete[] neighbors[0];
        delete[] neighbors;
        delete[] numNeighbors;
    }

    int* Neighbors(int u, int* num_neighbors) const
    {
        *num_neighbors = numNeighbors[u];
        return neighbors[u];
    }

    int getNeighborOffset(int node, int nbr);
    int nodeCount;
    int** neighbors;
    int* numNeighbors;
}; 

bool operator== (CompactMap& c1, CompactMap& c2);    
ostream& operator<<(ostream& os, const CompactMap& map);
void dump(CompactMap& map, const char* file);
CompactMap* loadCompactMap(const char* filepath);

struct AdjMatGraph {
    AdjMatGraph(int cnt, int** mat, int stat = -1) : nodeCount(cnt), adjMatrix(mat), stat1(stat) {}
    AdjMatGraph(int cnt) : nodeCount(cnt) 
    {
        adjMatrix = initMatrix(cnt, cnt);
    }

    AdjMatGraph(const AdjMatGraph& other)
    {
        nodeCount = other.nodeCount;
        adjMatrix = initMatrix(nodeCount, nodeCount);
        memcpy(adjMatrix[0], other.adjMatrix[0], nodeCount*nodeCount*sizeof(int));
        for (int i = 1; i < nodeCount; i++) {
            adjMatrix[i] = adjMatrix[0] + i*nodeCount;
        }
        return;
    }

    ~AdjMatGraph() 
    {
        freeMatrix(adjMatrix);
    }

    void UpdateEdge(int s, int t, bool add) 
    {
        if (add) {
            adjMatrix[s][t] = 1;
            adjMatrix[t][s] = 1;
        }
        else {
            adjMatrix[s][t] = 0;
            adjMatrix[t][s] = 0;
        }
    }

    void RemoveVertex(int u) 
    {
        for (int i = 0; i < nodeCount; i++) {
            if (adjMatrix[i][u] || adjMatrix[u][i] == 1) {
                adjMatrix[u][i] = 0;
                adjMatrix[i][u] = 0;
            }
        }
        return;
    }

    void Neighbors(int u, int* neighbors, int* num_neighbors) const
    {
        int count = 0;
        for (int i = 0; i < nodeCount; i++) {
            if (adjMatrix[i][u] || adjMatrix[u][i]) neighbors[count++] = i;
        }
        *num_neighbors = count;
        return;
    }

    inline bool IsNeighbor(int u, int v) { return adjMatrix[u][v]; }

    int nodeCount;
    int** adjMatrix;
    int stat1;
};

CompactMap* AdjMatGraphToCompactMap(const AdjMatGraph* adjMatGraph);
void dump(const AdjMatGraph& graph, const char* file);
AdjMatGraph* loadAdjMatGraph(const char* file);

struct AoA {
    AoA(int n) : num_slots(n)
    {
        arrays = new vector<int>*[num_slots];
        for (int i = 0; i < num_slots; i++) {
            arrays[i] = new vector<int>();
        }
    }

    ~AoA()
    {
        for (int i = 0; i < num_slots; i++) {
           delete arrays[i];
        }
        delete[] arrays;
    }

    void add(int i, int val)
    {
        arrays[i]->push_back(val);
    }

    template <class ForwardIterator>
    void add(int i, ForwardIterator first, ForwardIterator last) 
    {
        vector<int>* v = arrays[i];
        for (; first != last; ++first) {
            v->push_back(*first);
        }
    }
   
    int         num_slots;
    vector<int>** arrays;
};

CompactMap* AoAToCompactMap(const AoA& arrays);

struct BipartiteGraph {
    BipartiteGraph(const CompactMap* left_to_right, const CompactMap* right_to_left) :
            left2right(left_to_right), right2left(right_to_left) {}

    int* Neighbors(bool isLeft, int n, int* num_neighbors) 
    {
        const CompactMap* target = isLeft ? left2right : right2left;
        return target->Neighbors(n, num_neighbors);
    }

    int     sz1;
    int     sz2;
    const CompactMap* left2right;
    const CompactMap* right2left;
};

template <typename T>
void sort(T* array, unsigned int size)
{
    #ifdef __MTA__
    mtgl_sort(array, size);
    #else
    sort(array, array + size);
    #endif
}

void insert_sort(int* keys, int* values, int len);

template <typename T>
void pswap(T** p1, T** p2) 
{
    void* tmp;
    tmp = (void *) *p1; 
    *p1 = *p2;
    *p2 = (T *) tmp; 
}

#ifdef __MTA__
template <typename T>
void mtgl_sort(T* array, unsigned int size)
{
    if (size < 200) {   
        insertion_sort<T>(array, size);
    }
    else {
        merge_sort<T>(array, size);
    }
}
#endif

// inline int int_fetch_add_mt(int* mem, int incr);

template <class T>
void multiply_mt(T* target, T* operand) 
{
    #ifdef __MTA__
        T old = readfe(target);
        writeef(target, old*(*operand));
    #elif _OPENMP
        #pragma omp critical 
        {
            (*target) *= (*operand);
        }
    #else
        (*target) *= (*operand);
    #endif
    return;
}

/**
 * Utility function for traversing the neighbor list associated with each
 * vertex in the junction tree.  The visitor object performs user defined operartions. */
template <class Visitor>
void VisitCliqueGraph(const CompactMap* graph, int num_vertices, Visitor& visitor)
{
    // For each clique
    for (int i = 0, N = num_vertices; i < N; i++) {
        int num_neighbors = graph->numNeighbors[i];
        int* neighbors = graph->neighbors[i];
        // For each neighboring clique
        for (int j = 0; j < num_neighbors; j++) {
            visitor(i, neighbors[j]);
        }       
    }       
    return;
}

struct EdgeCounter {
    EdgeCounter() : count(0) {}
    void operator()(int i, int j) { count++; }    
    int NumEdges() { return (count >> 1); }    
    int count;
};

template <typename T>
struct d_array {
    d_array(int alloc = 8): alloc_len(alloc), len(0), array(NULL)
    {   
        if (alloc == 0) {
            alloc_len = 8;
        }
        // if (alloc_len > 0) {
        array = new T[alloc_len];
        // }   
    }   

    d_array(const d_array<T>& other)
    {
        // assert(other.alloc_len > 0);
        int other_alloc_len = other.alloc_len;
        int other_len = other.len;
        if (other_alloc_len == 0) {
            this->alloc_len = 8;
        }
        else {
            this->alloc_len = other_alloc_len;
        }
        this->len = other_len;

        if (this->alloc_len > 0) {
            this->array = new T[this->alloc_len];
            if (other.array && other_len) {
                for (int i = 0; i < other_len; i++) {
                    array[i] = other.array[i];
                }
            }
        }
        else {
            cout << "# Hitting branch" << endl;
            array = NULL;
        }
        return;
    }    

    ~d_array() 
    {   
        if (array) {
            delete[] array;
        }   
    }   

    void operator=(const d_array<T>& other) 
    {
        alloc_len = other.alloc_len;    
        if (alloc_len == 0) {
            alloc_len = 8;
        }

        len = other.len;
        if (array) {
            delete[] array;
            array = NULL;
        }

        // if (alloc_len > 0) {
        array = new T[alloc_len];
        if (other.array && len) {
            for (int i = 0; i < len; i++) {
                array[i] = other.array[i];
            }
        }
        return;
    }

    void append(const T& v)
    {   
        if (len == alloc_len) {
            T* old = array;
            if (alloc_len == 0) {
                alloc_len = 8;
            }
            else {
                alloc_len <<= 1;
            }

            array = new T[alloc_len];

            if (!array) {
                cerr << "Failed to allocate " 
                     << alloc_len*sizeof(T) << " bytes" << endl;
                exit(1);
            }

            for (int i = 0; i < len; i++) {
                array[i] = old[i];
            }
            // memcpy(array, old, len*sizeof(T));
            delete[] old;
        }   
        array[len++] = v;
    }   

    inline const T& operator[](int pos) const
    {   
        return array[pos];
    }   

    inline T& operator[](int pos) 
    {   
        return array[pos];
    }   

    inline T* begin() 
    {   
        return array;
    }   

    inline const T* begin() const
    {   
        return array;
    }   

    inline T* end() 
    {   
        return array + len;
    }   

    inline const T* end() const
    {   
        return array + len;
    }   

    inline void MakeSet()
    {
        sort(array, array + len);
        T* last = unique(array, array + len);
        len = last - array;
    }

    inline void Clear()
    {
        len = 0;
    }

    inline void ReleaseAndClear()
    {
        for (int i = 0; i < len; i++) {
            if (array[i]) {
                delete array[i];
                array[i] = NULL;
            }
        }
        len = 0;
    }

    T* array;
    int alloc_len;
    int len;
};

template <class InputIterator>
  int count_set_intersection (InputIterator first1, InputIterator last1,
                                    InputIterator first2, InputIterator last2)
{
    int count = 0;
    while (first1!=last1 && first2!=last2)
    {
        if (*first1<*first2) ++first1;
        else if (*first2<*first1) ++first2;
        else { first1++; first2++; count++; }
    }
    return count;
}


/*
template<typename EdgeProperty>
class AdjListEdgeListGraph {
    typedef pair<int, int> VertexPair;

    AdjListEdgeListGraph(int num_vertices, int num_edges, 
            EdgeListIterator first, EdgeListIterator last) :
            neighbors__(num_vertices), num_vertices__(num_vertices),
            num_edges__(num_edges), edges__(num_edges)
{
        int edge_index = 0;

        for (EdgeListIterator it = first; it != last; it++) {
            pair<VertexPair, EdgeProperty> p = *it;
            VertexPair pair = p.first;
            int u = pair.first;
            int v = pair.second; 
            map<int, int>& neighborsU = neighbors[u];
            map<int, int>& neighborsV = neighbors[v];
            neighborsU.insert(pair<int, int>(v, edge_index));
            neighborsV.insert(pair<int, int>(u, edge_index));
            edges.push_back(p.second);
            edge_index++;
}
        return;
}

    EdgeProperty& GetEdge(int u, int v)
{
        map<int, int>& neighbor_edges = neighbors__[u];
        pair<int, int>::iterator it = neighbor_edges.find(v);
        return edges[it->second];
}
    
    private:
    vector<map<int, int> > neighbors__;
    vector<EdgeProperty> edges__;
    int num_vertices__;
    int num_edges__;
};
*/

void GetFileList(const char* input_path, vector<string>& file_list);
bool IsVertexFile(const char* path);

void LoadMap(const char* path, map<string, int>& property_map);
bool FilterLabel(const string& v_s_label, const string& v_q_label);

bool Exists(const string& path);
void Strip(string& line);
char* Strip(char* line);

template <class Key, class Value>
map<Key, Value> SimpleLoadMap(string path)
{
    map<Key, Value> m;
    Key key;
    Value value;
    ifstream ifs(path.c_str(), ios_base::out);
    while(ifs.good()) {
        ifs >> key >> value;
        m[key] = value;
    }
    ifs.close();
    return m;
}

// template <typename Key, typename Value>
// void StoreMap(const map<Key, Value>& m, string path)
template <typename Map>
void StoreMap(const Map& m, string path)
{
    ofstream ofs(path.c_str(), ios_base::out);
    // typename map<Key, Value>::const_iterator it;
    typename Map::const_iterator it;
    for (it = m.begin(); it != m.end(); it++) {
        ofs << it->first << " " << it->second << endl;
    }
    ofs.close();
    return;
}

#endif
