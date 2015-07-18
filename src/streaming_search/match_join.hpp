#ifndef __JOIN_HPP__
#define __JOIN_HPP__
#include "subgraph_search.hpp"
using namespace std;

uint64_t join_time = 0;

uint64_t JoinTimer()
{
    uint64_t tmp = join_time;
    join_time = 0;
    return tmp;
}

pair<time_t, time_t> GetTimeRange(const MatchSet* match) 
{
    d_array<pair<dir_edge_t, dir_edge_t> > edge_map = match->edge_map;
    time_t min_time(edge_map[0].second.timestamp);
    time_t max_time(edge_map[0].second.timestamp);
    time_t search_edge_timestamp;
    
    for (int i = 1; i < edge_map.len; i++) {
        search_edge_timestamp = edge_map[i].second.timestamp;
        if (search_edge_timestamp < min_time) {
            min_time = search_edge_timestamp;
        }
        else if (search_edge_timestamp > max_time) {
            max_time = search_edge_timestamp;
        }
    }
    return pair<time_t, time_t>(min_time, max_time);
}

bool CheckBijectiveMapping(const MatchSet* m1, const MatchSet* m2)
{
    int N1 = m1->NumVertices(); //m1->VertexCount();
    int N2 = m2->NumVertices(); //m2->VertexCount();
    int reserve = N1 + N2;
    /* scoped_ptr<MatchSet::VertexPair> p_q_s_map(new MatchSet::VertexPair[reserve]);
    scoped_ptr<MatchSet::VertexPair> p_s_q_map(new MatchSet::VertexPair[reserve]);
    MatchSet::VertexPair* q_s_map = p_q_s_map.get();
    MatchSet::VertexPair* s_q_map = p_s_q_map.get(); */
    MatchSet::VertexPair* q_s_map = new MatchSet::VertexPair[reserve];
    MatchSet::VertexPair* s_q_map = new MatchSet::VertexPair[reserve];
    
    int next = 0; 
    for (int i = 0; i < N1; i++) {
        q_s_map[next] = pair<int, int>(m1->vertex_map[i].first, 
                                       m1->vertex_map[i].second);
        s_q_map[next++] = pair<int, int>(m1->vertex_map[i].second, 
                                       m1->vertex_map[i].first);
    }    
    for (int i = 0; i < N2; i++) {
        q_s_map[next] = pair<int, int>(m2->vertex_map[i].first, 
                                       m2->vertex_map[i].second);
        s_q_map[next++] = pair<int, int>(m2->vertex_map[i].second, 
                                       m2->vertex_map[i].first);
    }
    
    sort(q_s_map, q_s_map + next);
    sort(s_q_map, s_q_map + next);
    
    bool bijective_mapping = true;
    
    for (int i = 1; i < next; i++) {
        if (q_s_map[i].first == q_s_map[i-1].first &&
            q_s_map[i].second != q_s_map[i-1].second) {
            bijective_mapping = false;
            break;
        }    
        if (s_q_map[i].first == s_q_map[i-1].first &&
            s_q_map[i].second != s_q_map[i-1].second) {
            bijective_mapping = false;
            break;
        }    
    }    
    delete[] q_s_map;
    delete[] s_q_map;
    return bijective_mapping;
}

MatchSet* __Join(const MatchSet* lhs, const MatchSet* rhs, 
            const Subgraph& join_subgraph)
{
#ifdef DEBUG
    LOGSRC << "LHS" << endl;
    lhs->Print();
    LOGSRC << "RHS" << endl;
    rhs->Print();
    LOGSRC << "Join::fetching time range" << endl;
#endif
    pair<time_t, time_t> lhs_time_range, rhs_time_range;

    if (!lhs->edge_map.alloc_len || !lhs->edge_map.len ||
        !rhs->edge_map.alloc_len || !rhs->edge_map.len) {
        return NULL;
    }
    if (!lhs->vertex_map.alloc_len || !lhs->vertex_map.len ||
        !rhs->vertex_map.alloc_len || !rhs->vertex_map.len) {
        return NULL;
    }
    if ((lhs->vertex_map.alloc_len > 128) || (lhs->vertex_map.len > 32) ||
        (rhs->vertex_map.alloc_len > 128) || (rhs->vertex_map.len > 32)) {
        return NULL;
    }
    if ((lhs->edge_map.alloc_len > 128) || (lhs->edge_map.len > 32) ||
        (rhs->edge_map.alloc_len > 128) || (rhs->edge_map.len > 32)) {
        return NULL;
    }

    lhs_time_range = GetTimeRange(lhs);
    rhs_time_range = GetTimeRange(rhs);
    
    if (lhs_time_range.second > rhs_time_range.first) {
#ifdef DEBUG
        LOGSRC << "Join::failed time check" << endl;
#endif
        return NULL;
    }
    
    if (CheckBijectiveMapping(lhs, rhs) == false) {
#ifdef DEBUG
        LOGSRC << "Join::failed bijective mapping check" << endl;
#endif
        return NULL;
    }
    
    int num_est_vertices = lhs->NumVertices() + rhs->NumVertices();
    int num_est_edges = lhs->NumEdges() + rhs->NumEdges();
    // MatchSet* join_result = new MatchSet(num_est_vertices, num_est_edges);
    MatchSet* join_result = new MatchSet; // (num_est_vertices, num_est_edges);

    for (int i = 0; i < lhs->edge_map.len; i++) {
        join_result->edge_map.append(lhs->edge_map[i]);
    }

    for (int i = 0; i < rhs->edge_map.len; i++) {
        join_result->edge_map.append(rhs->edge_map[i]);
    }
    
    for (int i = 0; i < lhs->vertex_map.len; i++) {
        join_result->vertex_map.append(lhs->vertex_map[i]);
    }

    for (int i = 0; i < rhs->vertex_map.len; i++) {
        join_result->vertex_map.append(rhs->vertex_map[i]);
    }
    
    join_result->vertex_map.MakeSet();
    join_result->edge_map.MakeSet();
    assert(join_result->vertex_map.alloc_len);
    assert(join_result->edge_map.alloc_len);
    assert(join_result->NumVertices() > 0);
    assert(join_result->NumEdges() > 0);
    // join_result->num_vertices = join_result->vertex_map.len;
    // join_result->num_edges = join_result->edge_map.len;
#ifdef DEBUG
    LOGSRC << "Join::successful." << endl;
    join_result->Print();
#endif
    return join_result;
}

MatchSet* Join(const MatchSet* lhs, const MatchSet* rhs, 
            const Subgraph& join_subgraph)
{
    if (lhs->NumEdges() > 20 || rhs->NumEdges() > 20) {
        return NULL;
    }
#ifdef TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL); 
#endif
    MatchSet* m_ptr = __Join(lhs, rhs, join_subgraph);
#ifdef TIMING
    // cout << "****\n\nJOIN\n\n" << endl;
    gettimeofday(&end, NULL); 
    join_time += get_tv_diff(start, end);
#endif
    return m_ptr;
}    

#endif
