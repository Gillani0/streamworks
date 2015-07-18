#ifndef __PROPERTY_MAP__
#define __PROPERTY_MAP__

#include <map>
#include <string>
using namespace std;

void LoadPropertyMap(const char* property_file_path,
        std::map<std::string, std::string>& property_map);
std::string GetProperty(const std::map<std::string, std::string>& properties,
        const std::string& key);

#endif
