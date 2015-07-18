#ifndef __QUERY_OPERATORS_HPP__
#define __QUERY_OPERATORS_HPP__

// v1 -> v2 [label == XYZ and degree > 50 and protocol = icmp]

struct Constraint {
    string property;
    string op;
    string val;
};

enum OPER { LESS, EQUAL, GREATER };

template <typename Graph, OPER>
struct VertexConstraint {
    bool operator()(const Graph* g, uint64_t u) const;
};

template <typename Graph>
struct DegreeConstraint : public VertexConstraint {
    // Allowed operators <, ==, >
    bool operator()(const Graph* g, uint64_t u) const;
};

template <typename Graph, LESS>
struct DegreeConstraint : public VertexConstraint {

    DegreeConstraint(int p) : degree_threshold(p) {}

    bool operator()(const Graph* g, uint64_t u) const
    {
        return g->VertexCount() < degree_threshold;
    }

    int degree_threshold;
};

template <typename Graph, GREATER>
struct DegreeConstraint : public VertexConstraint {

    DegreeConstraint(int p) : degree_threshold(p) {}

    bool operator()(const Graph* g, uint64_t u) const
    {
        return g->VertexCount() > degree_threshold;
    }

    int degree_threshold;
};

template <typename Graph>
struct VertexDataConstraint : public VertexConstraint {
    // Allowed operators ==
    VertexDataConstraint(string property, string op, string val);
    bool operator()(const Graph* g, uint64_t u) const;
};

template <typename Graph>
struct EdgeConstraint {
    typedef typename Graph::DirectedEdgeType dir_edge;
    bool operator()(const Graph* g, const dir_edge& e) const;
};

template <typename Graph>
struct EdgeDataConstraint : EdgeConstraint {
    EdgeDataConstraint(string property, string op, string val);
    typedef typename Graph::DirectedEdgeType dir_edge;
    bool operator()(const Graph* g, const dir_edge& e) const;
};


void ParseInput(char* line, vector<Constraint>& constraint_list);

class VertexConstraintCheckerBuilder {
    void BuildConstraint(const vector<Constraint>& constraint_list,
            vector<ConstraintVerifyFn>& function_list)
    {
        for (vector<Constraint>::iterator it = constraint_list.begin();
                it != constraint_list.end(); it++) {
            Constraint& c = *it;
            if (c.property == "degree") {
                function_list.push_back(DegreeConstraint(c.op, c.val));
            }
            else if (c.property == "label") {
                function_list.push_back(LabelConstraint(c.op, c.val));
            }
            else {
                function_list.push_back(VertexDataConstraint(c.property,
                        c.op, c.val));
            }
        }
        return;
    }
};

class EdgeConstraintCheckerBuilder {
    void BuildConstraint(const vector<Constraint>& constraint_list,
            vector<ConstraintVerifyFn>& function_list)
    {
        for (vector<Constraint>::iterator it = constraint_list.begin();
                it != constraint_list.end(); it++) {
            Constraint& c = *it;
            function_list.push_back(PropertyConstraint(c.property,
                    c.op, c.val));
        }
        return;
    }
}

#endif
