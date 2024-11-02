
#pragma once

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <algorithm>
#include <iterator>
#include <filesystem>

/**
 * @brief Structure to store MapReduce specifications from the configuration file.
 */
struct MapReduceSpec {
    std::vector<std::string> worker_ipaddr_ports;   ///< Workers' IP addresses and ports
    std::vector<std::string> input_files;           ///< List of input files
    std::string config_filename;                    ///< Path to the config file
    std::string user_id;                            ///< User identifier
    std::string output_dir;                         ///< Output directory path
    int num_workers = 0;                            ///< Number of workers
    int num_output_files = 0;                       ///< Number of output files to be created
    int map_kilobytes = 0;                          ///< Kilobytes per shard
};

/**
 * @brief Splits a string by a given delimiter and appends the tokens to a vector.
 *
 * @param str The string to split.
 * @param delimiter The character delimiter to split by.
 * @param tokens The vector to append the split tokens.
 */
inline void split_string(const std::string& str, char delimiter, std::vector<std::string>& tokens) {
    std::stringstream ss(str);
    std::string token;
    while (std::getline(ss, token, delimiter)) {
        tokens.emplace_back(token);
    }
}

/**
 * @brief Splits a string by a given delimiter and returns the tokens as a vector.
 *
 * @param str The string to split.
 * @param delimiter The character delimiter to split by.
 * @return std::vector<std::string> The vector containing split tokens.
 */
inline std::vector<std::string> split_string(const std::string& str, char delimiter) {
    std::vector<std::string> tokens;
    split_string(str, delimiter, tokens);
    return tokens;
}

/**
 * @brief Populates a MapReduceSpec structure with specifications from a config file.
 *
 * @param config_filename The path to the config file.
 * @param mr_spec The MapReduceSpec structure to populate.
 * @return true if the configuration was successfully read and parsed.
 * @return false otherwise.
 */
inline bool read_mapreduce_spec(const std::string& config_filename, MapReduceSpec& mr_spec) {
    std::ifstream config_file(config_filename);
    if (!config_file.is_open()) {
        std::cerr << "Error: Unable to open config file: " << config_filename << std::endl;
        return false;
    }

    mr_spec.config_filename = config_filename;
    std::string line;
    while (std::getline(config_file, line)) {
        // Skip empty lines or comments
        if (line.empty() || line[0] == '#') {
            continue;
        }

        auto key_value = split_string(line, '=');
        if (key_value.size() != 2) {
            std::cerr << "Warning: Invalid line in config file: " << line << std::endl;
            continue;
        }

        const std::string& key = key_value[0];
        const std::string& value = key_value[1];

        if (key == "n_workers") {
            try {
                mr_spec.num_workers = std::stoi(value);
            } catch (const std::invalid_argument&) {
                std::cerr << "Error: Invalid number for n_workers: " << value << std::endl;
                return false;
            }
        } else if (key == "worker_ipaddr_ports") {
            mr_spec.worker_ipaddr_ports = split_string(value, ',');
        } else if (key == "input_files") {
            mr_spec.input_files = split_string(value, ',');
        } else if (key == "output_dir") {
            mr_spec.output_dir = value;
        } else if (key == "n_output_files") {
            try {
                mr_spec.num_output_files = std::stoi(value);
            } catch (const std::invalid_argument&) {
                std::cerr << "Error: Invalid number for n_output_files: " << value << std::endl;
                return false;
            }
        } else if (key == "map_kilobytes") {
            try {
                mr_spec.map_kilobytes = std::stoi(value);
            } catch (const std::invalid_argument&) {
                std::cerr << "Error: Invalid number for map_kilobytes: " << value << std::endl;
                return false;
            }
        } else if (key == "user_id") {
            mr_spec.user_id = value;
        } else {
            std::cerr << "Warning: Unknown key in config file: " << key << std::endl;
        }
    }

    config_file.close();
    return true;
}

/**
 * @brief Validates the MapReduceSpec configuration.
 *
 * @param mr_spec The MapReduceSpec structure to validate.
 * @return true if the specification is valid.
 * @return false otherwise.
 */
inline bool validate_mapreduce_spec(const MapReduceSpec& mr_spec) {
    std::cout << "Configuration File: " << mr_spec.config_filename << std::endl;
    std::cout << "User ID: " << mr_spec.user_id << std::endl;
    std::cout << "Number of Workers: " << mr_spec.num_workers << std::endl;

    std::cout << "Worker IP Addresses and Ports:" << std::endl;
    for (const auto& ip_port : mr_spec.worker_ipaddr_ports) {
        std::cout << "\t" << ip_port << std::endl;
    }

    std::cout << "Input Files:" << std::endl;
    for (const auto& input_file : mr_spec.input_files) {
        std::cout << "\t" << input_file << std::endl;
    }

    std::cout << "Output Directory: " << mr_spec.output_dir << std::endl;
    std::cout << "Number of Output Files: " << mr_spec.num_output_files << std::endl;
    std::cout << "Kilobytes per Shard: " << mr_spec.map_kilobytes << " KB" << std::endl;

    // Validate number of workers matches number of IP addresses
    if (mr_spec.num_workers != static_cast<int>(mr_spec.worker_ipaddr_ports.size())) {
        std::cerr << "Error: Number of workers (" << mr_spec.num_workers 
                  << ") does not match number of worker IP addresses provided (" 
                  << mr_spec.worker_ipaddr_ports.size() << ")." << std::endl;
        return false;
    }

    // Validate input files exist
    for (const auto& input_file : mr_spec.input_files) {
        if (!std::filesystem::exists(input_file)) {
            std::cerr << "Error: Input file does not exist: " << input_file << std::endl;
            return false;
        }
    }

    return true;
}
