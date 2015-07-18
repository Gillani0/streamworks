#ifndef __QUERY_HPP__
#define __QUERY_HPP__
// #include <boost/regex.hpp>
using namespace std;
// using namespace boost;

template <class T>
inline bool EQ(const T& v1, const T& v2)
{
    return v1 == v2;
}

template <class T>
inline bool NEQ(const T& v1, const T& v2)
{
    return v1 != v2;
}

template <class T>
inline bool GT(const T& v1, const T& v2)
{
    return v1 > v2;
}

template <class T>
inline bool LT(const T& v1, const T& v2)
{
    return v1 < v2;
}

template <class T>
inline bool GEQ(const T& v1, const T& v2)
{
    return v1 >= v2;
}

template <class T>
inline bool LEQ(const T& v1, const T& v2)
{
    return v1 <= v2;
}

template <class T>
inline bool InRange(const T& rbeg, const T& rend, const T& v)
{
    return ((v < rend) && (v > rbeg));
}

/*
inline bool RegexMatch(const string& reg_expr, const string& val)
{
    return regex_match(val.c_str(), regex(reg_expr.c_str()));
}
*/

#endif
