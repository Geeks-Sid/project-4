# Project Title: MapReduce Infrastructure

## Overview

This project implement a simplified version of the MapReduce infrastructure, a programming model designed for processing and generating large data sets. The core of this project is to simulate the MapReduce framework, which involves dividing tasks into smaller sub-tasks (map tasks) and then aggregating the results (reduce tasks). The project is designed to run on a single machine, simulating a distributed environment by using multiple process.

## Key Components

### Heartbeat Mechanism

The heartbeat mechanism is a critical component of this project, ensuring the reliability and robustness of the MapReduce framework. It involves periodic communication between the master and worker nodes to monitor the status of each worker. The master node sends heartbeat requests to each worker, and the workers respond with their status. This mechanism helps in detecting worker failures and taking corrective actions, such as reassigning tasks to other available workers.

- **Master Node**: The master node manages the heartbeat process by sending requests and processing responses. It marks workers as DEAD if they fail to respond within a specified timeout period. This is crucial for maintaining the system's stability and ensuring that tasks are reassigned promptly in case of worker failures.
  - Reference: `src/master.h` (startLine: 590, endLine: 655)

- **Worker Node**: Each worker node is responsible for responding to heartbeat requests. The worker sends back its status, indicating whether it is ALIVE or DEAD. This response helps the master node in maintaining an updated view of the system's health.
  - Reference: `src/worker.h` (startLine: 400, endLine: 435)

### Map and Reduce Tasks

The project implements the MapReduce paradigm by dividing the input data into smaller chunks (shards) and processing them using map tasks. The intermediate results are then aggregated using reduce tasks to produce the final output.

- **Map Tasks**: Each map task processes a shard of the input data, generating intermediate key-value pairs. These pairs are stored in intermediate files, which are later used by the reduce tasks.
  - Reference: `description.md` (startLine: 41, endLine: 46)

- **Reduce Tasks**: Reduce tasks aggregate the intermediate key-value pairs, sorting them by key and applying the user-defined reduce function to produce the final output.
  - Reference: `description.md` (startLine: 59, endLine: 66)

### Handling Complexities

Several complexities were addressed in this project to ensure a robust and efficient MapReduce implementation:

- **Worker Failures**: The heartbeat mechanism is used to detect and handle worker failures. If a worker is marked as DEAD, the master node reassigns its tasks to other available workers, ensuring that the MapReduce process continues without interruption.
  - Reference: `src/master.h` (startLine: 430, endLine: 441)

- **Data Partitioning**: The input data is partitioned into shards, ensuring that each shard contains complete records. This prevents issues such as splitting data in the middle of a word or record, which could lead to incorrect processing.
  - Reference: `description.md` (startLine: 20, endLine: 27)

- **Concurrency**: The project uses asynchronous gRPC calls to manage communication between the master and worker nodes, allowing for parallel processing of tasks. This approach maximizes resource utilization and reduces processing time.
  - Reference: `src/master.h` (startLine: 450, endLine: 462)

## How to Run

1. **Setup Environment**: Ensure that you have all the necessary dependencies installed, including gRPC and CMake.

2. **Build the Project**:
   - Navigate to the project root directory.
   - Run the `create_build.sh` script to clean previous builds and compile the project.

3. **Start Worker Processes**:
   - Navigate to the `bin` directory.
   - Start the worker processes using the command:
     ```bash
     ./mr_worker localhost:50051 & ./mr_worker localhost:50052 &
     ```

4. **Run the MapReduce Process**:
   - In the `bin` directory, execute the main MapReduce process:
     ```bash
     ./mrdemo config.ini
     ```

5. **Check Results**:
   - Once the process completes, check the `output` directory for the results.

6. **Cleanup**:
   - Ensure that all intermediate files are deleted after the process to prevent test failures.

This project demonstrate a robust implementation of the MapReduce framework, focusing on reliability and efficiency through the use of a heartbeat mechanism and effective task management.