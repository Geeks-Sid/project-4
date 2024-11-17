#pragma once

#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#if __cplusplus >= 201703L
#if __GNUC__ > 7 || __APPLE_CC__ > 7
#include <filesystem>
#elif __GNUC__ == 7 || __APPLE_CC__ == 7
#include <experimental/filesystem>
#endif
#if __GNUC__ > 7 || __APPLE_CC__ > 7
namespace fs = std::filesystem;
#elif __GNUC__ == 7 || __APPLE_CC__ == 7
namespace fs = std::experimental::filesystem;
#endif
#endif
/* CS6210_TASK: Create your data structure here for storing spec from the config file */
struct MapReduceSpec
{
    unsigned int worker_count = 0;
    unsigned int output_files = 0;
    unsigned int map_kb = 0;
    std::string user;
    std::string output_directory;
    std::vector<std::string> worker_endpoints;
    std::vector<std::string> input_files;
};

/**
 * Splits string with given delimiter del
 * @param s --> raw string
 * @param del --> delimiter
 * @return returns vector with parsed strings.
 */
inline std::vector<std::string> splitString(const std::string &s, char del)
{
    std::vector<std::string> arr{};
    std::stringstream ss(s);
    std::string temp;
    while (std::getline(ss, temp, del))
    {
        arr.push_back(temp);
    }
    return arr;
}

/**
 * Populate MapReduceSpec data structure with the specification from the config file
 * @param config_filename
 * @param mr_spec
 * @return true or false based on success
 */
inline bool read_mr_spec_from_config_file(const std::string &config_filename, MapReduceSpec &mr_spec)
{
    std::ifstream config_file(config_filename);
    std::string config_line;
    if (!config_file.good())
    {
        std::cerr << "Error opening fie : " << config_filename << " Error No" << std::strerror(errno) << std::endl;
        return false;
    }
    while (std::getline(config_file, config_line))
    {
        std::string key, value;
        key = config_line.substr(0, config_line.find_first_of('='));
        value = config_line.substr(config_line.find_first_of('=') + 1, config_line.length());
        if (value.empty() || key.empty())
        {
            std::cerr << key << " is empty or value " << value << " is empty , please check again" << std::endl;
            return false;
        }
        if (key == "n_workers")
        {
            mr_spec.worker_count = std::stoi(value);
            continue;
        }
        if (key == "worker_ipaddr_ports")
        {
            mr_spec.worker_endpoints = splitString(value, ',');
            continue;
        }
        if (key == "input_files")
        {
            mr_spec.input_files = splitString(value, ',');

            continue;
        }
        if (key == "output_dir")
        {
            mr_spec.output_directory = value;
            continue;
        }
        if (key == "n_output_files")
        {
            mr_spec.output_files = std::stoi(value);
            continue;
        }
        if (key == "map_kilobytes")
        {
            mr_spec.map_kb = std::stoi(value);
            continue;
        }
        if (key == "user_id")
        {
            mr_spec.user = value;
            continue;
        }
    }

    return true;
}

/**
 * validate  the  specification  read  from  the  config file
 * @param mr_spec
 * @return true or false based on validation criteria.
 */
inline bool validate_mr_spec(const MapReduceSpec &mr_spec)
{

    if (mr_spec.worker_endpoints.size() != mr_spec.worker_count)
    {
        std::cerr << "Invalid Count of Workers : " << mr_spec.worker_endpoints.size() << "config - worker_count"
                  << mr_spec.worker_count << std::endl;
        return false;
    }

#if __cplusplus >= 201703L
    if (!fs::is_directory(mr_spec.output_directory))
    {
        if (fs::is_regular_file(mr_spec.output_directory))
        {
            std::cerr << mr_spec.output_directory << " is file not directory please provide correct path." << std::endl;
            return false;
        }
        else
        {
            try
            {
                fs::create_directory(mr_spec.output_directory);
            }
            catch (fs::filesystem_error &e)
            {
                std::cout << e.what() << std::endl;
            }
        }
    }
#endif
    for (const auto &f : mr_spec.input_files)
    {
        std::ifstream temp_stream(f);
        if (!temp_stream.good())
        {
            std::cerr << "Error opening fie : " << f << " Error No: " << std::strerror(errno) << std::endl;
            return false;
        }
    }
    return true;
}