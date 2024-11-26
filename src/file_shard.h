#pragma once

#include <sys/stat.h>
#include <vector>
#include <iostream>
#include <fstream>

#include "mapreduce_spec.h"

#define KB 1024
#define TEMP_DIR "intermediate"

/**
 * @brief Represent a portion of a file with specific offsets.
 */
struct FileSegment
{
    std::string filename;                              ///< The name of the file.
    std::pair<std::uintmax_t, std::uintmax_t> offsets; ///< The start and end offsets of the segment.
};

/**
 * @brief Represent a shard of files, containing multiple file segments.
 */
struct FileShard
{
    int shard_id = -1;                 ///< The ID of the shard.
    std::vector<FileSegment> segments; ///< List of file segments in the shard.
};

/**
 * @brief Approximate the size of a file split based on the optimal shard size.
 *
 * This function calculate an approximate size for a file split by reading
 * from the file starting at a given offset and extending to the next newline.
 *
 * @param file_name The name of the file.
 * @param offset The starting offset in the file.
 * @param optimal_shard_size The optimal size for the shard.
 * @return The approximated size of the split.
 */
inline std::uintmax_t approximateSplitSize(
    const std::string &file_name,
    std::uintmax_t offset,
    std::uintmax_t optimal_shard_size)
{

    std::ifstream file_stream(file_name);
    if (!file_stream.good())
    {
        std::cerr << "Error: Unable to open file " << file_name << std::endl;
        return 0;
    }

    file_stream.seekg(offset + optimal_shard_size);
    std::string line;
    std::getline(file_stream, line);
    return optimal_shard_size + line.length() + 1;
}

/**
 * @brief Get the size of a file.
 *
 * This function retrieve the size of a file given its path.
 *
 * @param file_path The path to the file.
 * @return The size of the file in bytes, or -1 if an error occurs.
 */
inline std::uintmax_t getFileSize(const std::string &file_path)
{
    struct stat stat_buf;
    int rc = stat(file_path.c_str(), &stat_buf);
    if (rc != 0)
    {
        std::cerr << "Error: Unable to get file size for " << file_path << std::endl;
        return -1;
    }
    return stat_buf.st_size;
}
/**
 * @brief Split files into shards based on the MapReduce specification.
 *
 * This function divide input files into shards, each containing multiple file segments.
 * The size of each shard is determined by the map_size_kb parameter in the MapReduce specification.
 *
 * @param mr_spec The MapReduce specification containing input files and shard size.
 * @param file_shards The vector to store the resulting file shards.
 * @return True if the operation is successful, false otherwise.
 */
inline bool shard_files(const MapReduceSpec &mr_spec, std::vector<FileShard> &file_shards)
{
    std::uintmax_t optimal_shard_size = mr_spec.map_size_kb * KB;
    std::intmax_t remaining_shard_size = optimal_shard_size;
    FileShard current_shard;
    current_shard.shard_id = file_shards.size();

    for (const auto &file : mr_spec.input_file_paths)
    {
        std::uintmax_t file_size = getFileSize(file);
        if (file_size == static_cast<std::uintmax_t>(-1))
        {
            std::cerr << "Error: Skipping file due to size retrieval failure: " << file << std::endl;
            continue;
        }

        std::uintmax_t remaining_file_size = file_size;
        std::uintmax_t offset = 0;
        FileSegment current_segment;

        while (remaining_file_size > 0)
        {
            current_segment.filename = file;
            if (remaining_shard_size >= remaining_file_size)
            {
                // If the remaining shard size can accomodate the remaining file size
                current_segment.offsets = {offset, offset + remaining_file_size};
                remaining_shard_size -= remaining_file_size;
                remaining_file_size = 0;
                current_shard.segments.push_back(current_segment);
            }
            else
            {
                // Calculate the nearest size for the next split
                std::uintmax_t nearest_size = (offset + optimal_shard_size > file_size)
                                                  ? file_size - offset
                                                  : approximateSplitSize(file, offset, remaining_shard_size);

                current_segment.offsets = {offset, offset + nearest_size};
                if (offset > offset + nearest_size)
                {
                    std::cerr << "Error: Offset calculation went wrong." << std::endl;
                    exit(EXIT_FAILURE);
                }
                current_shard.segments.push_back(current_segment);
                current_segment = FileSegment();
                remaining_shard_size -= nearest_size;
                remaining_file_size -= nearest_size;
                offset += nearest_size;
            }

            if (remaining_shard_size <= 0)
            {
                // If the current shard is full, add it to the list and start a new shard
                file_shards.push_back(current_shard);
                current_shard = FileShard();
                current_shard.shard_id = file_shards.size();
                remaining_shard_size = optimal_shard_size;
            }
        }
    }

    if (current_shard.shard_id > -1)
    {
        file_shards.push_back(current_shard);
    }
    return true;
}
