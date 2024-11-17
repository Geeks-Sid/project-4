#!/bin/bash

# Exit on any error
set -e

# Go to the project root directory
cd "$(dirname "$0")"

# Step 1: Clean up previous build
echo "Cleaning previous build..."
rm -rf build
mkdir build
cd build

# Step 2: Run CMake and Make
echo "Building the project..."
cmake ..
make

# Step 3: Copy input files and config.ini
echo "Setting up input files..."
mkdir -p bin/input
cp -r ../test/input/ bin/

# Step 4: Start worker processes
echo "Starting worker processes..."
IFS=',' read -r -a WORKER_PORTS <<< "$(grep 'worker_ipaddr_ports' ../test/config.ini | cut -d '=' -f 2 | sed 's/localhost://g')"
WORKER_PIDS=()

# Change to the bin directory
cd bin

for PORT in "${WORKER_PORTS[@]}"; do
    ./mr_worker "localhost:${PORT}" &
    WORKER_PIDS+=($!)
    echo "Started mr_worker on localhost:${PORT} with PID ${!}"
done

# Step 5: Run the main MapReduce process
echo "Running the main MapReduce process..."
./mrdemo config.ini

# Step 6: Check the output directory
echo "Checking the output directory..."
OUTPUT_DIR="output"
if [ -d "$OUTPUT_DIR" ]; then
    echo "Output files generated in $OUTPUT_DIR:"
    ls -l "$OUTPUT_DIR"
else
    echo "No output directory found. Please check the logs for errors."
fi

echo "All tests completed successfully!"
