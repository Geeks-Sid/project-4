#pragma once

#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <utility>
#include <functional>

// Debugging flag, set to 1 to enable debug output
#define DEBUG 0
// Default file path for null operations
#define DEV_NULL "/dev/null"
// Maximum size for key-value pairs before flushing to file
#define MAX_KV_PAIR_SIZE 4096
// Delimiter used in file output
#define DELIMITER '|'

/**
 * @class BaseMapperInternal
 * @brief Handles the internal operations of a mapper, including emitting key-value pairs and managing intermediate files.
 */
struct BaseMapperInternal
{
    BaseMapperInternal();
    void emit(const std::string &key, const std::string &val);
    void final_flush();

    // Stores key-value pairs with their corresponding file paths
    std::vector<std::pair<std::string, std::pair<std::string, std::string>>> kv_pairs;
    // List of intermediate files for storing key-value pairs
    std::vector<std::string> intermediate_files;

private:
    std::string map_key_to_file(const std::string &key);
};

inline BaseMapperInternal::BaseMapperInternal()
{
    if (DEBUG)
        std::cout << "[DEBUG] BaseMapperInternal initialized." << std::endl;
}

/**
 * @brief Maps a key to a specific intermediate file based on a hash function.
 * @param key The key to be mapped.
 * @return The file path to which the key is mapped.
 */
inline std::string BaseMapperInternal::map_key_to_file(const std::string &key)
{
    std::hash<std::string> hash_fn;
    if (intermediate_files.empty())
    {
        if (DEBUG)
            std::cerr << "[WARNING] Intermediate file list is empty. Returning DEV_NULL." << std::endl;
        return DEV_NULL;
    }
    auto file_index = hash_fn(key) % intermediate_files.size();
    return intermediate_files[file_index];
}

/**
 * @brief Emits a key-value pair, storing it in memory until the maximum size is reached, then flushes to file.
 * @param key The key to be emitted.
 * @param val The value to be emitted.
 */
inline void BaseMapperInternal::emit(const std::string &key, const std::string &val)
{
    if (kv_pairs.size() >= MAX_KV_PAIR_SIZE)
    {
        if (DEBUG)
            std::cout << "[DEBUG] Flushing KV pairs to files as size exceeded MAX_KV_PAIR_SIZE." << std::endl;
        for (const auto &pair : kv_pairs)
        {
            std::ofstream file(pair.first, std::ofstream::out | std::ofstream::app);
            if (!file)
            {
                std::cerr << "[ERROR] Failed to open file " << pair.first << " for writing." << std::endl;
                continue;
            }
            file << pair.second.first << DELIMITER << pair.second.second << std::endl;
        }
        kv_pairs.clear();
    }
    kv_pairs.emplace_back(map_key_to_file(key), std::make_pair(key, val));
}

/**
 * @brief Flushes all remaining key-value pairs to their respective files.
 */
inline void BaseMapperInternal::final_flush()
{
    if (DEBUG)
        std::cout << "[DEBUG] Final flush of KV pairs to files." << std::endl;
    for (const auto &pair : kv_pairs)
    {
        std::ofstream file(pair.first, std::ofstream::out | std::ofstream::app);
        if (!file)
        {
            std::cerr << "[ERROR] Failed to open file " << pair.first << " for writing." << std::endl;
            continue;
        }
        file << pair.second.first << DELIMITER << pair.second.second << std::endl;
    }
    kv_pairs.clear();
}

/**
 * @class BaseReducerInternal
 * @brief Handles the internal operations of a reducer, including emitting key-value pairs to an output file.
 */
struct BaseReducerInternal
{
    BaseReducerInternal();
    void emit(const std::string &key, const std::string &val);

    // Output file for storing reduced key-value pairs
    std::string output_file;
};

inline BaseReducerInternal::BaseReducerInternal()
{
    if (DEBUG)
        std::cout << "[DEBUG] BaseReducerInternal initialized." << std::endl;
}

/**
 * @brief Emits a key-value pair to the output file.
 * @param key The key to be emitted.
 * @param val The value to be emitted.
 */
inline void BaseReducerInternal::emit(const std::string &key, const std::string &val)
{
    std::ofstream file(output_file, std::ofstream::out | std::ofstream::app);
    if (!file)
    {
        std::cerr << "[ERROR] Failed to open file " << output_file << " for writing." << std::endl;
        return;
    }
    file << key << " " << val << std::endl;
}
