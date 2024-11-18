#pragma once

#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <filesystem>
namespace fs = std::filesystem;

/**
 * @brief Represents the configuration specifications for a MapReduce job.
 *
 * This structure holds various parameters required to configure and execute
 * a MapReduce job, including the number of workers, output files, and input files.
 */
struct MapReduceSpec
{
    unsigned int num_workers = 0;              ///< Number of worker nodes
    unsigned int num_output_files = 0;         ///< Number of output files
    unsigned int map_size_kb = 0;              ///< Size of map tasks in kilobytes
    std::string username;                      ///< User executing the MapReduce job
    std::string output_dir;                    ///< Directory for storing output files
    std::vector<std::string> worker_addresses; ///< Endpoints of worker nodes
    std::vector<std::string> input_file_paths; ///< Paths to input files

    /**
     * @brief Prints the current configuration for debugging purposes.
     */
    void print_config() const
    {
        std::cout << "MapReduce Configuration:" << std::endl;
        std::cout << "Number of Workers: " << num_workers << std::endl;
        std::cout << "Number of Output Files: " << num_output_files << std::endl;
        std::cout << "Map Size (KB): " << map_size_kb << std::endl;
        std::cout << "User: " << username << std::endl;
        std::cout << "Output Directory: " << output_dir << std::endl;
        std::cout << "Worker Endpoints: ";
        for (const auto &endpoint : worker_addresses)
        {
            std::cout << endpoint << " ";
        }
        std::cout << std::endl;
        std::cout << "Input Files: ";
        for (const auto &file : input_file_paths)
        {
            std::cout << file << " ";
        }
        std::cout << std::endl;
    }
};

/**
 * @brief Splits a given string into a vector of substrings based on a specified delimiter.
 *
 * This function logs the splitting process and warns if any empty segments are encountered.
 * It also logs an error if no segments are found after splitting.
 *
 * @param input The string to be split.
 * @param delimiter The character used to split the string.
 * @return A vector of substrings obtained by splitting the input string.
 */
inline std::vector<std::string> split_string(const std::string &input, char delimiter)
{
    std::vector<std::string> segments;
    std::stringstream stream(input);
    std::string segment;

    std::cout << "Splitting string: " << input << " using delimiter: " << delimiter << std::endl;

    while (std::getline(stream, segment, delimiter))
    {
        if (!segment.empty())
        {
            segments.push_back(segment);
        }
        else
        {
            std::cerr << "Warning: Empty segment encountered while splitting." << std::endl;
        }
    }

    if (segments.empty())
    {
        std::cerr << "Error Code 201: No segments found after splitting the string." << std::endl;
    }

    return segments;
}

/**
 * @brief Reads and parses a MapReduce configuration file to populate a MapReduceSpec object.
 *
 * This function opens the specified configuration file and reads it line by line.
 * Each line is expected to be in the format "key=value". The function populates the
 * MapReduceSpec object based on recognized keys. It logs errors for invalid lines,
 * unknown keys, and invalid or out-of-range values.
 *
 * @param config_filename The path to the configuration file.
 * @param mr_spec A reference to a MapReduceSpec object to be populated.
 * @return True if the configuration file is read and parsed successfully, false otherwise.
 */
inline bool read_mr_spec_from_config_file(const std::string &config_filename, MapReduceSpec &mr_spec)
{
    std::ifstream config_file(config_filename);
    if (!config_file.is_open())
    {
        std::cerr << "Error Code 301: Error opening file: " << config_filename << " - " << std::strerror(errno) << std::endl;
        return false;
    }

    std::string line;
    while (std::getline(config_file, line))
    {
        auto delimiter_pos = line.find('=');
        if (delimiter_pos == std::string::npos)
        {
            std::cerr << "Error Code 302: Invalid line format (missing '='): " << line << std::endl;
            return false;
        }

        std::string key = line.substr(0, delimiter_pos);
        std::string value = line.substr(delimiter_pos + 1);

        if (key.empty() || value.empty())
        {
            std::cerr << "Error Code 303: Empty key or value: " << key << " = " << value << std::endl;
            return false;
        }

        try
        {
            if (key == "n_workers")
            {
                mr_spec.num_workers = std::stoi(value);
            }
            else if (key == "worker_ipaddr_ports")
            {
                mr_spec.worker_addresses = split_string(value, ',');
            }
            else if (key == "input_files")
            {
                mr_spec.input_file_paths = split_string(value, ',');
            }
            else if (key == "output_dir")
            {
                mr_spec.output_dir = value;
            }
            else if (key == "n_output_files")
            {
                mr_spec.num_output_files = std::stoi(value);
            }
            else if (key == "map_kilobytes")
            {
                mr_spec.map_size_kb = std::stoi(value);
            }
            else if (key == "user_id")
            {
                mr_spec.username = value;
            }
            else
            {
                std::cerr << "Error Code 304: Unknown configuration key: " << key << std::endl;
                return false;
            }
        }
        catch (const std::invalid_argument &e)
        {
            std::cerr << "Error Code 305: Invalid value for key: " << key << " - " << e.what() << std::endl;
            return false;
        }
        catch (const std::out_of_range &e)
        {
            std::cerr << "Error Code 306: Value out of range for key: " << key << " - " << e.what() << std::endl;
            return false;
        }
    }

    std::cout << "Configuration file " << config_filename << " read successfully." << std::endl;
    return true;
}

/**
 * @brief Validates the MapReduce specification.
 *
 * This function checks the consistency and validity of the MapReduceSpec object.
 * It ensures that the number of worker endpoints matches the worker count and
 * verifies that all input files are accessible.
 *
 * @param mr_spec The MapReduceSpec object containing configuration details.
 * @return true if the specification is valid, false otherwise.
 */
inline bool validate_mr_spec(const MapReduceSpec &mr_spec)
{
    // Check if the number of worker endpoints matches the worker count
    if (mr_spec.worker_addresses.size() != mr_spec.num_workers)
    {
        std::cerr << "Error Code 101: Worker count mismatch - Expected: "
                  << mr_spec.num_workers << ", Found: "
                  << mr_spec.worker_addresses.size() << std::endl;
        return false;
    }

    // Verify that each input file is accessible
    for (const auto &file_path : mr_spec.input_file_paths)
    {
        std::ifstream file_stream(file_path);
        if (!file_stream.good())
        {
            std::cerr << "Error Code 102: Unable to open file: "
                      << file_path << " - " << std::strerror(errno) << std::endl;
            return false;
        }
    }

    std::cout << "MapReduce specification validated successfully." << std::endl;
    return true;
}
