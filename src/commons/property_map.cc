#include <map>
#include <vector>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <unistd.h>
using namespace std;

void ApplyVars(const std::map<std::string, std::string>& env_vars,
        std::string& str)
{
    for (map<std::string, std::string>::const_iterator it = env_vars.begin();
            it != env_vars.end(); it++) {
        size_t pos = str.find(it->first);
        if (pos != std::string::npos) {
            str.replace(pos, it->first.size(), it->second);
        }   
    }   
    return;
}

/*
void PrintProperties(const std::map<std::string, std::string>& prop_map)
{
    for (map<std::string, std::string>::const_iterator it = prop_map.begin();
            it != prop_map.end(); it++) {
        cout << it->first << "=" << it->second << endl;
    }   
    return;
}
*/

void LoadPropertyMap(const char* property_file_path,
        std::map<std::string, std::string>& property_map)
{
    std::map<std::string, std::string> env_vars;
    ifstream ifs(property_file_path, ios_base::in);
    if (ifs.is_open() == false) {
        cout << "Failed to open=" << property_file_path << endl;
        _exit(1);
        return;
    }

    std::string token, line;
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
        while (strm.good()) {
            strm >> token;
            tokens.push_back(token);
        }
        if (tokens.size() == 2) {
            if (tokens[0][0] == '$') {
                env_vars[tokens[0]] = tokens[1];
            }
            else {
                ApplyVars(env_vars, tokens[1]);
                property_map[tokens[0]] = tokens[1];
            }
        }
    }
    ifs.close();
    // PrintProperties(property_map);
    return;
}

std::string GetProperty(const std::map<std::string, std::string>& properties, 
        const std::string& key)
{
    std::map<std::string, std::string>::const_iterator it = properties.find(key);
    if (it != properties.end()) {
        return it->second;
    }
    else {
        cout << "Missing key from properties: " << key << endl;
        _exit(1);
    }
}

/*
int main(int argc, char* argv[])
{
    std::map<std::string, std::string> property_map;
    std::string file_path = "test/nyt.properties";
    LoadPropertyMap(file_path.c_str(), property_map);
    cout << "query path=" << property_map["query_path"] << endl;
}
*/
