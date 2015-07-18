#ifndef __COUNTER_HPP__
#define __COUNTER_HPP__

#include <fstream>
#include <algorithm>
// #include <boost/unordered_map.hpp>
#include <unordered_map>
using namespace std;
//using namespace boost;

template <class T>
bool asc_sort_pair(T& pair1, T& pair2)
{
    return pair1.first > pair2.first;
}

template <class T>
class Histogram {
public:
    void Add(const T& val)
    {
        typename unordered_map<T, int>::iterator it = 
                counts__.find(val);
        if (it == counts__.end()) {
            counts__.insert(pair<T, int>(val, 1));
        }
        else {
            it->second += 1;
        }
        return;
    }

    void GetTopK(int k, vector<pair<T, int> >& values)
    {
        if (counts__.size() <= k) {
            // for (auto t:counts__) {
            for (int i = 0, N = values.size(); i < N; i++) {
                const pair<T, int>& t = values[i];
                values.push_back(pair<T, int>(t.first, t.second));
            }
            return;
        }

        vector<pair<int, T> > scratch(counts__.size());
        // for (auto entry:counts__) {
        for (typename unordered_map<T, int>::const_iterator entry = counts__.begin();
                entry != counts__.end();
                entry++) {
            scratch.push_back(pair<int, T>(entry->second, 
                    entry->first));
        }

        partial_sort(scratch.begin(), scratch.begin() + k, 
                scratch.end(), asc_sort_pair<pair<int, T> >);

        for (int i = 0; i < k; i++) {
            pair<int, T> tmp = scratch[i];
            values.push_back(pair<T, int>(tmp.second, tmp.first));
        }
        return;
    }

    void Size()
    {
        return counts__.size();
    }
        
private:
    unordered_map<T, int> counts__;
};

/**
 * Use KeyGenerator if you need to map values to unique ids and also need 
 * to retrive the value using the id.
 * If you always need a value to id mapping, use IdGenerator.
 */
template <class T>
class KeyGenerator {
public:
    typedef typename std::map<T, int>::iterator IdMapIterator;
    KeyGenerator() : key_count__(0) {}

    bool Exists(const T& entry) 
    {
        IdMapIterator it = id_map__.find(entry);
        return (it != id_map__.end());
    }

    int GetKey(const T& entry)
    {   
        int id;         
        IdMapIterator it = id_map__.find(entry);
        if (it == id_map__.end()) {
            id = id_map__.size();
            id_map__.insert(std::pair<T, int>(entry, id));
            value_map__.insert(std::pair<int, T>(id, entry));
        }   
        else {
            id = it->second;
        }   
        return id; 
    }   

    void GetValue(int id, T& val) const
    {   
        typedef typename std::map<int, T>::const_iterator ValueMapIterator;
        ValueMapIterator it = value_map__.find(id);
        val = it->second;
        return;
    }   

    int size() const 
    {   
        return id_map__.size(); 
    }   

private:
    std::map<T, int> id_map__;
    std::map<int, T> value_map__;
    int key_count__;
};

template <class T>
class IdGenerator {
public:
    typedef typename unordered_map<T, int>::iterator iterator;
    typedef typename unordered_map<T, int>::const_iterator const_iterator;
    IdGenerator() {}

    int GetKey(const T& entry)
    {   
        int id;         
        iterator it = id_map__.find(entry);
        if (it == id_map__.end()) {
            id = id_map__.size();
            id_map__.insert(std::pair<T, int>(entry, id));
        }   
        else {
            id = it->second;
        }   
        return id; 
    }   

    int size() const 
    {   
        return id_map__.size(); 
    }   

    const_iterator begin() const
    {
        return id_map__.begin();
    }

    const_iterator end() const
    {
        return id_map__.end();
    }

    void load(string path)
    {
        ifstream ifs(path.c_str(), ios_base::in);
        if (ifs.is_open() == false) {
            cout << "Failed to open=" << path << endl;
            _exit(1);
            return;
        }   

        T key;
        int value, num_pairs(0);
        std::string line;

        while (ifs.good()) {
            getline(ifs, line);
            if (line.size() == 0) {
                continue;
            }   
            if (line[0] == '#') {
                continue;
            }   
            vector<std::string> tokens;
            std::stringstream strm(line);
            
            strm >> key >> value;
            id_map__.insert(pair<T, int>(key, value));
            num_pairs++;
        }   
        ifs.close();
        cout << "Loaded IdGenerator with " << num_pairs 
             << " entries from " << path << endl;
        return;
    }

    void save(string out_path) 
    {
        ofstream ofs(out_path.c_str(), ios_base::out);
        for (const_iterator it = id_map__.begin();
                it != id_map__.end(); it++) {
            ofs << it->first << " " << it->second << endl;
        }
        ofs.close();
        return;
    }

private:
    // map<T, int> id_map__;
    unordered_map<T, int> id_map__;
};

template <class T>
class Counter {
public:
    typedef typename unordered_map<T, int>::iterator iterator;
    typedef typename unordered_map<T, int>::const_iterator const_iterator;
    int Add(const T& entry, int count = 1)
    {
        // typedef typename map<T, int>::iterator CounterIterator;
        iterator it = count_table__.find(entry);
        if (it == count_table__.end()) {
            count_table__.insert(pair<T, int>(entry, count));
        } else {
            count = it->second + 1;
            it->second = count;
        }       
        return count;
    }

    int Count(const T& entry) const
    {
        // typedef typename map<int, T>::const_iterator CounterIterator;
        const_iterator it = count_table__.find(entry);
        return it->second;
    }

    int size() { return count_table__.size(); }

    void reset() 
    {
        count_table__.clear();
    }

    const_iterator begin() const
    {
        return count_table__.begin();
    }

    const_iterator end() const
    {
        return count_table__.end();
    }

private:
    unordered_map<T, int> count_table__;
};

#endif
