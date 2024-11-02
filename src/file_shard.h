/*
 * CS 6210 - Project 4
 * Haoran Li
 * GTid: 903377792
 * Date: Nov.28, 2018
 */

#pragma once

#include <cmath>
#include <vector>
#include <string>
#include <fstream>
#include <iostream>

#include "mapreduce_spec.h"

/**
 * Represents a segment of a file with start and end offsets.
 */
struct FileOffset {
    std::string filename;
    std::size_t startOffset; // Inclusive start offset within the file
    std::size_t endOffset;   // Exclusive end offset within the file
};

/**
 * Represents a shard containing multiple file offsets.
 */
struct FileShard {
    int shardId;
    std::vector<FileOffset> pieces;
};

/**
 * Splits input files into shards based on the MapReduce specification.
 *
 * This function creates file shards by dividing the input files into
 * chunks of approximately 'map_kilobytes' kilobytes. It ensures that
 * shards do not split words by adjusting shard boundaries to the next
 * newline character.
 *
 * @param mrSpec The MapReduce specification containing input files and shard size.
 * @param fileShards The vector to populate with the resulting file shards.
 * @return True if sharding is successful, false otherwise.
 */
inline bool shardFiles(const MapReduceSpec& mrSpec, std::vector<FileShard>& fileShards) {
    std::cout << "Starting file sharding procedure..." << std::endl;

    const auto& inputFilenames = mrSpec.input_files;
    const std::size_t shardSizeBytes = mrSpec.map_kilobytes * 1000;

    std::size_t totalSize = 0;
    int currentShardId = 0;
    FileShard currentShard;
    currentShard.shardId = currentShardId;
    std::size_t currentShardBytes = 0;

    for (const auto& filename : inputFilenames) {
        std::ifstream fileStream(filename, std::ifstream::binary | std::ifstream::ate);
        if (!fileStream.is_open()) {
            std::cerr << "Error opening file: " << filename << std::endl;
            return false;
        }

        std::size_t fileSize = static_cast<std::size_t>(fileStream.tellg());
        fileStream.seekg(0, std::ifstream::beg);
        totalSize += fileSize;

        std::size_t currentOffset = 0;

        while (currentOffset < fileSize) {
            std::size_t bytesLeftInShard = shardSizeBytes - currentShardBytes;
            std::size_t bytesRemainingInFile = fileSize - currentOffset;
            std::size_t bytesToRead = std::min(bytesLeftInShard, bytesRemainingInFile);

            std::size_t tentativeEndOffset = currentOffset + bytesToRead;

            // Adjust to the next newline to avoid splitting a word, if not at the end of file
            if (tentativeEndOffset < fileSize) {
                fileStream.seekg(tentativeEndOffset);
                char ch;
                while (fileStream.get(ch)) {
                    tentativeEndOffset++;
                    if (ch == '\n') {
                        break;
                    }
                }

                // If no newline found, set end offset to file size
                if (tentativeEndOffset > fileSize) {
                    tentativeEndOffset = fileSize;
                }
            } else {
                tentativeEndOffset = fileSize;
            }

            FileOffset shardPiece;
            shardPiece.filename = filename;
            shardPiece.startOffset = currentOffset;
            shardPiece.endOffset = tentativeEndOffset;

            currentShard.pieces.emplace_back(shardPiece);
            std::size_t bytesAdded = shardPiece.endOffset - shardPiece.startOffset;
            currentShardBytes += bytesAdded;
            currentOffset = shardPiece.endOffset;

            // If current shard reached or exceeded the desired size, finalize it
            if (currentShardBytes >= shardSizeBytes) {
                fileShards.emplace_back(std::move(currentShard));
                currentShardId++;
                currentShard = FileShard{currentShardId, {}};
                currentShardBytes = 0;
            }
        }

        fileStream.close();
    }

    // Add the last shard if it has any pieces
    if (!currentShard.pieces.empty()) {
        fileShards.emplace_back(std::move(currentShard));
    }

    std::size_t calculatedShardCount = static_cast<std::size_t>(std::ceil(static_cast<double>(totalSize) / shardSizeBytes));
    std::cout << "Total files size: " << totalSize << " bytes, Calculated number of shards: " 
              << calculatedShardCount << std::endl;
    std::cout << "Actual number of shards created: " << fileShards.size() << std::endl;

    return true;
}
