#pragma once

#include <map>
#include <vector>
#include <string>
#include <fstream>
#include <sstream>
#include <iostream>
#include <functional>
#include <memory>
#include <optional>
#include <unordered_map>
#include <stdexcept>

// Include the full definition of BaseReducer
#include "mr_task_factory.h"

using std::cerr;
using std::cout;
using std::endl;
using std::hash;
using std::ifstream;
using std::istringstream;
using std::map;
using std::ofstream;
using std::out_of_range;
using std::string;
using std::vector;

/**
 * @brief Internal implementation of the BaseMapper.
 *
 * Handles the emission and storage of key-value pairs during the map phase.
 */
struct BaseMapperInternal
{
    BaseMapperInternal() = default;

    /**
     * @brief Emits a key-value pair.
     *
     * Stores the key-value pair in an internal map. If the key does not exist, it initializes a new vector.
     *
     * @param key The key to emit.
     * @param value The value associated with the key.
     */
    void emit(const string &key, const string &value);

    /**
     * @brief Writes the emitted key-value pairs to intermediate files.
     *
     * Distributes the key-value pairs across multiple output files based on a hash of the key.
     *
     * @param base_name The base name for the output files.
     * @param num_output_files The number of output files to create.
     */
    void write_data(const string &base_name, int num_output_files);

private:
    // Stores the intermediate key-value pairs.
    map<string, vector<string>> key_value_pairs_;

    // Names of the intermediate files.
    vector<string> intermediate_file_names_;
};

inline void BaseMapperInternal::emit(const string &key, const string &value)
{
    key_value_pairs_[key].emplace_back(value);
}

inline void BaseMapperInternal::write_data(const string &base_name, int num_output_files)
{
    if (num_output_files <= 0)
    {
        throw std::invalid_argument("Number of output files must be positive.");
    }

    // Initialize intermediate file names
    intermediate_file_names_.reserve(num_output_files);
    for (int i = 0; i < num_output_files; ++i)
    {
        intermediate_file_names_.emplace_back(base_name + "_R" + std::to_string(i) + ".txt");
    }

    hash<string> hash_fn;

    // Open all intermediate files once to avoid frequent opening and closing
    vector<ofstream> output_streams;
    output_streams.reserve(num_output_files);
    for (const auto &file_name : intermediate_file_names_)
    {
        output_streams.emplace_back(file_name, std::ios::app);
        if (!output_streams.back().is_open())
        {
            cerr << "[BaseMapperInternal] Error: Unable to open file " << file_name << " for writing." << endl;
            throw std::runtime_error("Failed to open intermediate file.");
        }
    }

    // Distribute key-value pairs across the intermediate files
    for (const auto &[key, values] : key_value_pairs_)
    {
        size_t file_index = hash_fn(key) % num_output_files;
        auto &ofs = output_streams[file_index];
        if (!ofs)
        {
            cerr << "[BaseMapperInternal] Error: Output stream for file " << intermediate_file_names_[file_index] << " is not valid." << endl;
            continue;
        }

        ofs << key;
        if (!values.empty())
        {
            ofs << ':' << values[0];
            for (size_t i = 1; i < values.size(); ++i)
            {
                ofs << ',' << values[i];
            }
        }
        ofs << '\n';
    }

    // Close all output streams
    for (auto &ofs : output_streams)
    {
        ofs.close();
    }

    cout << "[BaseMapperInternal] Successfully wrote data to " << num_output_files << " intermediate files." << endl;
}

/**
 * @brief Internal implementation of the BaseReducer.
 *
 * Handles the aggregation and emission of key-value pairs during the reduce phase.
 */
struct BaseReducerInternal
{
    BaseReducerInternal() = default;

    /**
     * @brief Emits a key-value pair to the final output file.
     *
     * Appends the key-value pair to the designated final output file.
     *
     * @param key The key to emit.
     * @param value The value associated with the key.
     */
    void emit(const string &key, const string &value);

    /**
     * @brief Groups keys from intermediate files.
     *
     * Reads all intermediate files, aggregates values by keys, and stores them internally.
     */
    void group_keys();

    /**
     * @brief Sets the name of the final output file.
     *
     * @param file_name The name of the final output file.
     */
    void set_final_file(const string &file_name)
    {
        final_file_ = file_name;
    }

    /**
     * @brief Adds an intermediate file to be processed.
     *
     * @param file_name The name of the intermediate file.
     */
    void add_intermediate_file(const string &file_name)
    {
        intermediate_files_.emplace_back(file_name);
    }

    // Add this public method
    void process_reduction(BaseReducer *reducer)
    {
        for (const auto &kv_pair : aggregated_pairs_)
        {
            reducer->reduce(kv_pair.first, kv_pair.second);
        }
    }

private:
    // Name of the final output file.
    string final_file_;

    // List of intermediate files to process.
    vector<string> intermediate_files_;

    // Aggregated key-value pairs.
    map<string, vector<string>> aggregated_pairs_;
};

inline void BaseReducerInternal::emit(const string &key, const string &value)
{
    if (final_file_.empty())
    {
        cerr << "[BaseReducerInternal] Error: Final output file is not set." << endl;
        throw std::runtime_error("Final output file not set.");
    }

    ofstream fout(final_file_, std::ios::app);
    if (!fout.is_open())
    {
        cerr << "[BaseReducerInternal] Error: Unable to open final output file " << final_file_ << " for writing." << endl;
        throw std::runtime_error("Failed to open final output file.");
    }

    fout << key << ", " << value << '\n';
    fout.close();

    cout << "[BaseReducerInternal] Emitted key-value pair (" << key << ", " << value << ") to " << final_file_ << endl;
}

inline void BaseReducerInternal::group_keys()
{
    if (intermediate_files_.empty())
    {
        cerr << "[BaseReducerInternal] Warning: No intermediate files to process." << endl;
        return;
    }

    for (const auto &file_name : intermediate_files_)
    {
        ifstream infile(file_name);
        if (!infile.is_open())
        {
            cerr << "[BaseReducerInternal] Error: Unable to open intermediate file " << file_name << " for reading." << endl;
            continue;
        }

        string line;
        while (std::getline(infile, line))
        {
            if (line.empty())
                continue;

            istringstream line_stream(line);
            string key;
            if (!std::getline(line_stream, key, ':'))
            {
                cerr << "[BaseReducerInternal] Warning: Malformed line in file " << file_name << ": " << line << endl;
                continue;
            }

            string value;
            while (std::getline(line_stream, value, ','))
            {
                aggregated_pairs_[key].emplace_back(value);
            }
        }

        infile.close();
        cout << "[BaseReducerInternal] Processed intermediate file: " << file_name << endl;
    }

    cout << "[BaseReducerInternal] Successfully grouped keys from all intermediate files." << endl;
}
