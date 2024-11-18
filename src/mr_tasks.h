#pragma once

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#define DEBUG 0
#define devnull "/dev/null"
#define MAX_KV_PAIR_SIZE 4096
#define DELIMITER '|'

struct BaseMapperInternal
{
    BaseMapperInternal();
    void emit(const std::string &key, const std::string &val);

    std::vector<std::pair<std::string, std::pair<std::string, std::string>>> kv_pair_vector;
    std::vector<std::string> intermediate_file_list;

    std::string internal_file_mapping(std::string key);
    void final_flush();
};

inline BaseMapperInternal::BaseMapperInternal()
{
}

inline std::string BaseMapperInternal::internal_file_mapping(std::string key)
{
    std::hash<std::string> h;
    if (BaseMapperInternal::intermediate_file_list.empty())
        return devnull;
    auto file_location = h(key) % BaseMapperInternal::intermediate_file_list.size();
    return BaseMapperInternal::intermediate_file_list[file_location];
}

inline void BaseMapperInternal::emit(const std::string &key, const std::string &val)
{
    if (BaseMapperInternal::kv_pair_vector.size() > MAX_KV_PAIR_SIZE)
    {
        for (const auto &a : BaseMapperInternal::kv_pair_vector)
        {
            std::ofstream f(a.first, std::ofstream::out | std::ofstream::app);
            f << a.second.first << DELIMITER << a.second.second << std::endl;
        }
        BaseMapperInternal::kv_pair_vector.clear();
    }
    BaseMapperInternal::kv_pair_vector.push_back({BaseMapperInternal::internal_file_mapping(key), {key, val}});
}

inline void BaseMapperInternal::final_flush()
{
    for (const auto &a : BaseMapperInternal::kv_pair_vector)
    {
        std::ofstream f(a.first, std::ofstream::out | std::ofstream::app);
        f << a.second.first << DELIMITER << a.second.second << std::endl;
        f.close();
    }
    BaseMapperInternal::kv_pair_vector.clear();
}

struct BaseReducerInternal
{
    BaseReducerInternal();
    void emit(const std::string &key, const std::string &val);

    std::string file_name;
};

inline BaseReducerInternal::BaseReducerInternal()
{
}

inline void BaseReducerInternal::emit(const std::string &key, const std::string &val)
{
    std::ofstream f(file_name, std::ofstream::out | std::ofstream::app);
    f << key << " " << val << std::endl;
    f.close();
}
