#pragma once

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#define DEBUG 0
#define devnull "/dev/null"
#define MAX_KV_PAIR_SIZE 4096
#define DELIMITER '|'

/* CS6210_TASK Implement this data structure as per your implementation.
                You will need this when your worker is running the map task*/

struct BaseMapperInternal
{

    /* DON'T change this function's signature */
    BaseMapperInternal();

    /* DON'T change this function's signature */
    void emit(const std::string &key, const std::string &val);

    /* NOW you can add below, data members and member functions as per the need of
     * your implementation*/
    /**
     * Storage vector for key value pairs
     */
    std::vector<std::pair<std::string, std::pair<std::string, std::string>>> kv_pair_vector;

    std::vector<std::string> intermediate_file_list;

    std::string internal_file_mapping(std::string key);

    void final_flush();
};

/* CS6210_TASK Implement this function */
/**
 * Constructor not required as no private variable require initialization
 */
inline BaseMapperInternal::BaseMapperInternal()
{
}

/**
 * Find intermediate file based on given key by creating hash of key and taking modulo
 * Taking hash allows normal distribution of keys for unique keys.
 * @param key
 * @return file location for given key.
 */
inline std::string BaseMapperInternal::internal_file_mapping(std::string key)
{
    std::hash<std::string> h;
    if (BaseMapperInternal::intermediate_file_list.empty())
        return devnull;
    auto file_location = h(key) % BaseMapperInternal::intermediate_file_list.size();
    return BaseMapperInternal::intermediate_file_list[file_location];
}

/**
 * Flush Key Value pair to required intermediate file. We use Caching of MAX_KV_PAIR_SIZE (2048 )
 * @param key
 * @param val
 */
inline void BaseMapperInternal::emit(const std::string &key, const std::string &val)
{
#if DEBUG > 1
//    std::cout << BaseMapperInternal::kv_pair_vector.size() << "Dummy emit by BaseMapperInternal: " << key << DELIMITER
//    << val << std::endl;
#endif
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
/**
 * Final flush to intermediate file for given map operation.
 */
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

/*-----------------------------------------------------------------------------------------------*/

/* CS6210_TASK Implement this data structureas per your implementation.
                You will need this when your worker is running the reduce task*/
struct BaseReducerInternal
{

    /* DON'T change this function's signature */
    BaseReducerInternal();

    /* DON'T change this function's signature */
    void emit(const std::string &key, const std::string &val);

    /* NOW you can add below, data members and member functions as per the need of
     * your implementation*/
    std::string file_name;
};

/**
 * Constructor not required as no private variable require initialization
 */
inline BaseReducerInternal::BaseReducerInternal()
{
}

/* CS6210_TASK Implement this function */
/**
 * Emit given key value pair to output file this->file_name.
 * @param key
 * @param val
 */
inline void BaseReducerInternal::emit(const std::string &key, const std::string &val)
{
#if DEBUG > 1
    std::cout << "Dummy emit by BaseReducerInternal: " << key << ", " << val << std::endl;
#endif
    std::ofstream f(file_name, std::ofstream::out | std::ofstream::app);
    f << key << " " << val << std::endl;
    f.close();
}