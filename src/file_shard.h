#pragma once

#include <sys/stat.h>
#include <vector>
#include <iostream>
#include <fstream>

#include "mapreduce_spec.h"
#define KB 1024
#define TEMP_DIR "intermediate"

inline std::uintmax_t get_filesize(std::string path)
{
    struct stat stat_buf;
    int rc = stat(path.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

struct splitFile
{
    std::string filename;
    std::pair<std::uintmax_t, std::uintmax_t> offsets;
};

struct FileShard
{
    int shard_id = -1;
    std::vector<splitFile> split_file_list;
};

inline std::uintmax_t approx_split(
    const std::basic_string<char> fileName,
    uintmax_t offset,
    uintmax_t optimal_shard_size)
{
    std::uintmax_t approx_size;
    std::ifstream fs(fileName);
    if (!fs.good())
    {
        std::cerr << "Error opening file: " << fileName << std::endl;
        return 0;
    }
    fs.seekg(offset + optimal_shard_size);
    std::string temp_str;
    std::getline(fs, temp_str);
    approx_size = optimal_shard_size + temp_str.length() + 1;
    return approx_size;
}

inline bool shard_files(const MapReduceSpec &mr_spec, std::vector<FileShard> &fileShards)
{
    std::uintmax_t optimal_shard_size = mr_spec.map_kb * KB;
    std::intmax_t rem_shard_size = optimal_shard_size;
    FileShard current_shard;
    current_shard.shard_id = fileShards.size();
    for (const auto &f : mr_spec.input_files)
    {
        std::uintmax_t file_size, rem_file_size;
        file_size = rem_file_size = get_filesize(f);
        std::uintmax_t offset = 0;
        splitFile current_split_file;
        while (rem_file_size > 0)
        {
            current_split_file.filename = f;
            if (rem_shard_size >= rem_file_size)
            {
                current_split_file.offsets = {offset, offset + rem_file_size};
                rem_shard_size -= rem_file_size;
                rem_file_size = 0;
                current_shard.split_file_list.push_back(current_split_file);
            }
            else
            {
                std::uintmax_t nearest_size;
                nearest_size = offset + optimal_shard_size > file_size ? file_size - offset
                                                                       : approx_split(f, offset, rem_shard_size);
                current_split_file.offsets = {offset, offset + nearest_size};
                if (offset > offset + nearest_size)
                {
                    std::cerr << "Error: Offset calculation went wrong." << std::endl;
                    exit(1);
                }
                current_shard.split_file_list.push_back(current_split_file);
                current_split_file = splitFile();
                rem_shard_size -= nearest_size;
                rem_file_size -= nearest_size;
                offset += nearest_size;
            }
            if (rem_shard_size <= 0)
            {
                fileShards.push_back(current_shard);
                current_shard = FileShard();
                current_shard.shard_id = fileShards.size();
                rem_shard_size = optimal_shard_size;
            }
        }
    }
    if (current_shard.shard_id > -1)
        fileShards.push_back(current_shard);
    return true;
}