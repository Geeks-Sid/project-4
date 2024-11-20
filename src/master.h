#pragma once

#include <condition_variable>
#include <filesystem>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <numeric>
#include <thread>
#include <unistd.h>
#include <utility>

#include "file_shard.h"
#include "mapreduce_spec.h"
#include "masterworker.grpc.pb.h"

namespace fs = std::filesystem;

// Constants for server status and configuration
constexpr bool SERVER_ALIVE = true;                    ///< Indicates if the server is alive
constexpr int HEARTBEAT_TIMEOUT = 5;                   ///< Timeout in seconds for heartbeat responses
constexpr const char *TEMP_DIRECTORY = "intermediate"; ///< Directory for storing intermediate files

/**
 * @brief Enum representing the status of a worker.
 */
enum class WorkerStatus
{
    FREE, ///< Worker is available for tasks
    BUSY, ///< Worker is currently processing a task
    DEAD  ///< Worker is not responding
};

/**
 * @brief Enum representing the type of worker.
 */
enum class WorkerType
{
    MAPPER, ///< Worker is a Mapper
    REDUCER ///< Worker is a Reducer
};

/**
 * @brief Structure to hold heartbeat payload information.
 */
struct HeartbeatPayload
{
    std::string id;      ///< Unique identifier for the worker
    int64_t timestamp;   ///< Timestamp of the last heartbeat
    WorkerStatus status; ///< Current status of the worker
};

/**
 * @brief Base class for managing asynchronous gRPC calls.
 */
class AsyncCall
{
public:
    bool isMapJob = true;        ///< Flag to indicate if the job is a map job
    grpc::ClientContext context; ///< gRPC client context for the call
    grpc::Status status;         ///< Status of the gRPC call
    std::string workerAddress;   ///< Address of the worker

    virtual ~AsyncCall() = default; ///< Virtual destructor for cleanup
};

/**
 * @brief Class for managing asynchronous Map calls.
 */
class MapCall : public AsyncCall
{
public:
    masterworker::Map_Response result;                                                           ///< Result of the Map call
    std::unique_ptr<grpc::ClientAsyncResponseReader<masterworker::Map_Response>> responseReader; ///< Reader for the Map response
};

/**
 * @brief Class for managing asynchronous Reduce calls.
 */
class ReduceCall : public AsyncCall
{
public:
    masterworker::Reduce_Response result;                                                           ///< Result of the Reduce call
    std::unique_ptr<grpc::ClientAsyncResponseReader<masterworker::Reduce_Response>> responseReader; ///< Reader for the Reduce response
};

/**
 * @brief Class for managing asynchronous Heartbeat calls.
 */
class HeartbeatCall : public AsyncCall
{
public:
    masterworker::Heartbeat_Payload result;                                                           ///< Result of the Heartbeat call
    std::unique_ptr<grpc::ClientAsyncResponseReader<masterworker::Heartbeat_Payload>> responseReader; ///< Reader for the Heartbeat response
};

/**
 * @brief Client class for interacting with a worker.
 */
class WorkerClient
{
public:
    /**
     * @brief Constructs a WorkerClient object and initializes a gRPC channel.
     *
     * @param address The address of the worker to connect to.
     * @param queue Pointer to the gRPC completion queue for handling asynchronous operations.
     */
    WorkerClient(const std::string &address, grpc::CompletionQueue *queue);

    /**
     * @brief Destructor for WorkerClient.
     *
     * Cleans up resources by shutting down the heartbeat completion queue.
     */
    ~WorkerClient();

    /**
     * @brief Sends a heartbeat message to the worker.
     *
     * Prepares and sends a heartbeat message to check the worker's status.
     *
     * @param currentTime The current time used to set the deadline for the heartbeat.
     */
    void sendHeartbeat(int64_t currentTime);

    /**
     * @brief Receives a heartbeat response from the worker.
     *
     * @return True if the heartbeat was successfully received, false otherwise.
     */
    bool receiveHeartbeat();

    /**
     * @brief Schedules a Map job on the worker.
     *
     * @param spec The MapReduce specification.
     * @param shard The file shard to process.
     */
    void scheduleMapJob(const MapReduceSpec &spec, const FileShard &shard);

    /**
     * @brief Schedules a Reduce job on the worker.
     *
     * @param spec The MapReduce specification.
     * @param fileList List of files to reduce.
     * @param outputFile The output file for the reduced data.
     */
    void scheduleReduceJob(const MapReduceSpec &spec, const std::vector<std::string> &fileList, const std::string &outputFile);

private:
    /**
     * @brief Converts a FileShard to a gRPC partition.
     *
     * @param shard The FileShard to convert.
     * @param partition The gRPC partition to populate.
     */
    void convertToGrpcSpec(const FileShard &shard, masterworker::partition *partition);

    std::unique_ptr<masterworker::Map_Reduce::Stub> stub_; ///< gRPC stub for making calls
    grpc::CompletionQueue *queue_;                         ///< Completion queue for gRPC operations
    grpc::CompletionQueue heartbeatQueue_;                 ///< Completion queue for heartbeat operations
    std::string workerAddress_;                            ///< Address of the worker
};

/**
 * @brief Constructs a WorkerClient object and initializes a gRPC channel.
 *
 * This constructor sets up a gRPC channel to communicate with a worker at the specified address.
 * It also initializes the gRPC stub for making asynchronous calls.
 *
 * @param address The address of the worker to connect to.
 * @param queue Pointer to the gRPC completion queue for handling asynchronous operations.
 */
WorkerClient::WorkerClient(const std::string &address, grpc::CompletionQueue *queue)
    : queue_(queue), workerAddress_(address)
{
    std::cout << "[INFO] Initializing gRPC channel at address: " << address << std::endl;
    stub_ = masterworker::Map_Reduce::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
}

/**
 * @brief Destructor for WorkerClient.
 *
 * This destructor shuts down the heartbeat completion queue to clean up resources.
 */
WorkerClient::~WorkerClient()
{
    heartbeatQueue_.Shutdown();
}

/**
 * @brief Sends a heartbeat message to the worker.
 *
 * This function prepares and sends a heartbeat message to the worker to check its status.
 * It sets a deadline for the heartbeat response and initiates an asynchronous gRPC call.
 *
 * @param currentTime The current time used to set the deadline for the heartbeat.
 */
void WorkerClient::sendHeartbeat(int64_t currentTime)
{
    // Set the deadline for the heartbeat response
    auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(HEARTBEAT_TIMEOUT);

    // Create a new HeartbeatCall object to manage the asynchronous call
    auto call = new HeartbeatCall;
    call->workerAddress = workerAddress_;
    call->context.set_deadline(deadline);

    // Prepare the heartbeat payload with worker details
    masterworker::Heartbeat_Payload payload;
    payload.set_id(workerAddress_);
    payload.set_status(masterworker::Heartbeat_Payload::UNKNOWN);

    // Initiate the asynchronous heartbeat call
    call->responseReader = stub_->PrepareAsyncheartbeat(&call->context, payload, &heartbeatQueue_);
    call->responseReader->StartCall();
    call->responseReader->Finish(&call->result, &call->status, (void *)call);
}

/**
 * @brief Receives and processes the heartbeat response from the worker.
 *
 * This function waits for the heartbeat response and checks the status of the worker.
 * It logs an error if the worker is reported as dead or if there is a communication error.
 *
 * @return true if the worker is alive, false otherwise.
 */
bool WorkerClient::receiveHeartbeat()
{
    void *tag;
    bool ok = false;

    // Wait for the next event in the heartbeat queue
    GPR_ASSERT(heartbeatQueue_.Next(&tag, &ok));
    auto *call = static_cast<HeartbeatCall *>(tag);

    // Check the status of the heartbeat response
    if (call->status.ok())
    {
        if (call->result.status() == masterworker::Heartbeat_Payload::DEAD)
        {
            std::cerr << "[ERROR] Code 401: Worker " << call->workerAddress << " reported as dead." << std::endl;
            delete call;
            return false;
        }
        delete call;
        return true;
    }
    else
    {
        std::cerr << "[ERROR] Code 402: Heartbeat communication error with worker " << workerAddress_
                  << ": " << call->status.error_message() << std::endl;
        delete call;
        return false;
    }
}

/**
 * @brief Schedules a Map job for a worker.
 *
 * This function prepares and sends a gRPC request to schedule a Map job on a worker.
 * It converts the given FileShard into a gRPC-compatible format and initiates the asynchronous call.
 *
 * @param spec The MapReduce specification containing configuration details.
 * @param shard The FileShard to be processed in the Map job.
 */
void WorkerClient::scheduleMapJob(const MapReduceSpec &spec, const FileShard &shard)
{
    // Create a Map request and set its parameters
    masterworker::Map_Request request;
    request.set_uuid(spec.username);
    request.set_partition_count(spec.num_output_files);

    // Convert the FileShard to a gRPC partition and add it to the request
    auto *partition = request.add_shard();
    convertToGrpcSpec(shard, partition);

    // Prepare and initiate the asynchronous gRPC call
    auto call = new MapCall;
    call->workerAddress = workerAddress_;
    call->isMapJob = true;
    call->responseReader = stub_->PrepareAsyncmap(&call->context, request, queue_);
    call->responseReader->StartCall();
    call->responseReader->Finish(&call->result, &call->status, (void *)call);

    std::cout << "Scheduled Map job for worker at " << workerAddress_ << " with shard ID " << shard.shard_id << std::endl;
}

/**
 * @brief Schedules a Reduce job for a worker.
 *
 * This function prepares and sends a gRPC request to schedule a Reduce job on a worker.
 * It includes the list of intermediate files and the output file name in the request.
 *
 * @param spec The MapReduce specification containing configuration details.
 * @param fileList The list of intermediate files to be reduced.
 * @param outputFile The name of the output file for the Reduce job.
 */
void WorkerClient::scheduleReduceJob(const MapReduceSpec &spec, const std::vector<std::string> &fileList, const std::string &outputFile)
{
    // Create a Reduce request and set its parameters
    masterworker::Reduce_Request request;
    request.set_uuid(spec.username);
    request.set_output_file(outputFile);

    // Add each file in the fileList to the request
    for (const auto &file : fileList)
    {
        request.add_file_list(file);
    }

    // Prepare and initiate the asynchronous gRPC call
    auto call = new ReduceCall;
    call->workerAddress = workerAddress_;
    call->isMapJob = false;
    call->responseReader = stub_->PrepareAsyncreduce(&call->context, request, queue_);
    call->responseReader->StartCall();
    call->responseReader->Finish(&call->result, &call->status, (void *)call);

    std::cout << "Scheduled Reduce job for worker at " << workerAddress_ << " with output file " << outputFile << std::endl;
}

/**
 * @brief Converts a FileShard to a gRPC partition.
 *
 * This function translates the details of a FileShard into a format compatible with gRPC,
 * populating the provided partition object with the necessary data.
 *
 * @param shard The FileShard to be converted.
 * @param partition The gRPC partition object to be populated.
 */
void WorkerClient::convertToGrpcSpec(const FileShard &shard, masterworker::partition *partition)
{
    // Set the shard ID for the partition
    partition->set_shard_id(shard.shard_id);

    // Add each segment in the shard to the partition's file list
    for (const auto &segment : shard.segments)
    {
        auto *file = partition->add_file_list();
        file->set_filename(segment.filename);
        file->set_start_offset(segment.offsets.first);
        file->set_end_offset(segment.offsets.second);
    }
}

/**
 * @struct Worker
 * @brief Represents a worker in the MapReduce framework.
 *
 * This structure holds information about a worker, including its address, status, type,
 * current shard being processed, and associated client. It also tracks output files and
 * whether the worker's death has been handled.
 */
struct Worker
{
    std::string address;                                         ///< The network address of the worker.
    WorkerStatus status;                                         ///< The current status of the worker (e.g., FREE, BUSY, DEAD).
    WorkerType type;                                             ///< The type of worker (e.g., MAPPER, REDUCER).
    FileShard currentShard;                                      ///< The current FileShard being processed by the worker.
    std::shared_ptr<WorkerClient> client;                        ///< The client used to communicate with the worker.
    std::map<std::string, std::vector<std::string>> outputFiles; ///< Map of output files produced by the worker.
    int currentOutput;                                           ///< The index of the current output file being processed.
    bool deadHandled = false;                                    ///< Flag indicating if the worker's death has been handled.
};

/**
 * @class Master
 * @brief Manages the MapReduce process, coordinating workers and handling tasks.
 *
 * The Master class is responsible for orchestrating the MapReduce workflow. It manages
 * worker status, assigns tasks, and handles asynchronous operations using gRPC.
 */
class Master
{
public:
    /**
     * @brief Constructs a Master object with the given MapReduce specification and file shards.
     *
     * Initializes the Master with the provided configuration and prepares the environment
     * for executing the MapReduce tasks.
     *
     * @param spec The MapReduce specification containing configuration details.
     * @param shards The list of file shards to be processed.
     */
    Master(const MapReduceSpec &spec, const std::vector<FileShard> &shards);

    /**
     * @brief Destructor for the Master class.
     *
     * Cleans up resources, ensuring that all temporary files and gRPC resources are properly released.
     */
    ~Master();

    /**
     * @brief Executes the MapReduce process.
     *
     * Initiates the map and reduce phases, manages worker assignments, and ensures
     * the completion of all tasks.
     *
     * @return True if the process completes successfully, false otherwise.
     */
    bool run();

private:
    /**
     * @brief Finds a worker by its name (address).
     *
     * Searches through the list of workers to find a worker with the specified address.
     * If found, returns a pointer to the worker; otherwise, logs an error and returns nullptr.
     *
     * @param name The address of the worker to find.
     * @return Pointer to the Worker if found, nullptr otherwise.
     */
    Worker *findWorkerByName(const std::string &name);

    /**
     * @brief Finds workers by their status.
     *
     * Returns a list of indices of workers that match the specified status.
     *
     * @param status The status to filter workers by.
     * @return A vector of indices of workers with the specified status.
     */
    std::vector<int> findWorkersByStatus(WorkerStatus status);

    /**
     * @brief Monitors worker heartbeats to detect failures.
     *
     * Continuously checks the status of workers to ensure they are alive and functioning.
     */
    void heartbeat();

    /**
     * @brief Handles a worker that has been detected as dead.
     *
     * Takes necessary actions to reassign tasks and maintain system stability.
     *
     * @param workerName The name (address) of the dead worker.
     */
    void handleDeadWorker(const std::string &workerName);

    /**
     * @brief Cleans up temporary files generated during the MapReduce process.
     *
     * Ensures that all intermediate and temporary files are removed after processing.
     */
    void cleanupFiles();

    /**
     * @brief Asynchronously processes map tasks.
     *
     * Manages the distribution and completion of map tasks across available workers.
     */
    void asyncMap();

    /**
     * @brief Asynchronously processes reduce tasks.
     *
     * Manages the distribution and completion of reduce tasks across available workers.
     */
    void asyncReduce();

    /**
     * @brief Assigns files to a reducer based on the output ID.
     *
     * Determines which files should be processed by a specific reducer.
     *
     * @param outputId The ID of the output partition.
     * @return A vector of file names assigned to the reducer.
     */
    std::vector<std::string> assignFilesToReducer(int outputId);

    grpc::CompletionQueue *completionQueue_; ///< gRPC completion queue for handling async operations.
    bool serverState_ = SERVER_ALIVE;        ///< Indicates if the server is currently running.

    MapReduceSpec spec_; ///< The MapReduce specification.

    Worker dummyWorker_;                      ///< A template worker used for initialization.
    std::vector<Worker> workers_;             ///< List of workers managed by the Master.
    std::mutex workerMutex_;                  ///< Mutex for synchronizing access to worker data.
    std::condition_variable workerCondition_; ///< Condition variable for worker status changes.

    bool heartbeatInitialized_ = true;           ///< Flag indicating if heartbeat monitoring is initialized.
    std::mutex heartbeatMutex_;                  ///< Mutex for synchronizing heartbeat operations.
    std::condition_variable heartbeatCondition_; ///< Condition variable for heartbeat monitoring.

    std::mutex cleanupMutex_;                  ///< Mutex for synchronizing cleanup operations.
    std::condition_variable cleanupCondition_; ///< Condition variable for cleanup operations.

    int completionCount_;                         ///< Counter for completed tasks.
    bool operationsCompleted_ = false;            ///< Flag indicating if all operations are completed.
    std::mutex operationsMutex_;                  ///< Mutex for synchronizing operation completion.
    std::condition_variable operationsCondition_; ///< Condition variable for operation completion.

    int assignedShards_;                         ///< Number of file shards assigned to workers.
    std::vector<FileShard> fileShards_;          ///< List of file shards to be processed.
    std::vector<FileShard> missingShards_;       ///< List of file shards that are missing or unprocessed.
    std::vector<std::string> intermediateFiles_; ///< List of intermediate files generated.

    int assignedPartitions_;               ///< Number of output partitions assigned to reducers.
    std::vector<std::string> outputFiles_; ///< List of output files generated.
    std::vector<int> missingOutputs_;      ///< List of output partitions that are missing or unprocessed.
};

/**
 * @brief Constructs a Master object to manage the MapReduce process.
 *
 * Initializes the Master with the given MapReduce specification and file shards.
 * Sets up the gRPC completion queue and prepares worker clients for communication.
 *
 * @param spec The MapReduce specification containing configuration details.
 * @param shards The list of file shards to be processed.
 */
Master::Master(const MapReduceSpec &spec, const std::vector<FileShard> &shards)
    : spec_(spec), fileShards_(shards)
{
    // Initialize the gRPC completion queue for handling asynchronous operations.
    completionQueue_ = new grpc::CompletionQueue();

    // Set up worker clients based on the provided worker addresses.
    for (const auto &address : spec_.worker_addresses)
    {
        dummyWorker_.address = address;
        dummyWorker_.status = WorkerStatus::FREE; // Initially, all workers are free.
        dummyWorker_.type = WorkerType::MAPPER;   // Workers start as mappers.
        dummyWorker_.client = std::make_shared<WorkerClient>(address, completionQueue_);
        workers_.push_back(dummyWorker_);
    }
}

/**
 * @brief Destructor for the Master class.
 *
 * Cleans up resources by shutting down the gRPC completion queue and performing
 * necessary file cleanup operations.
 */
Master::~Master()
{
    // Signal the server to stop and shutdown the completion queue.
    serverState_ = !SERVER_ALIVE;
    completionQueue_->Shutdown();
    cleanupFiles(); // Perform cleanup of temporary files.
}

/**
 * @brief Finds a worker by its name (address).
 *
 * Searches through the list of workers to find a worker with the specified address.
 * If found, returns a pointer to the worker; otherwise, logs an error and returns nullptr.
 *
 * @param name The address of the worker to find.
 * @return Pointer to the Worker if found, nullptr otherwise.
 */
Worker *Master::findWorkerByName(const std::string &name)
{
    for (auto &worker : workers_)
    {
        if (worker.address == name)
        {
            return &worker;
        }
    }
    std::cerr << "Error: Worker with address '" << name << "' not found." << std::endl;
    return nullptr;
}

/**
 * @brief Finds workers by their status.
 *
 * Iterates through the list of workers and collects indices of workers that match
 * the specified status.
 *
 * @param status The status to filter workers by (e.g., FREE, BUSY, DEAD).
 * @return A vector of indices representing workers with the specified status.
 */
std::vector<int> Master::findWorkersByStatus(WorkerStatus status)
{
    std::vector<int> indices;
    for (size_t i = 0; i < workers_.size(); ++i)
    {
        if (workers_[i].status == status)
        {
            indices.push_back(static_cast<int>(i));
        }
    }
    return indices;
}

/**
 * @brief Monitors the status of workers by sending and receiving heartbeat signals.
 *
 * This function continuously checks the status of all workers by sending heartbeat
 * signals and receiving responses. If a worker fails to respond, it is marked as DEAD,
 * and appropriate cleanup and reallocation procedures are initiated. The function
 * ensures that the system remains aware of the operational status of each worker.
 */
void Master::heartbeat()
{
    while (serverState_)
    {
        // Map to store heartbeat messages for each worker
        std::map<std::string, HeartbeatPayload> heartbeatMessages;
        auto currentTime = std::chrono::system_clock::now().time_since_epoch().count();

        // Send heartbeat signals to all workers that are not marked as DEAD
        for (const auto &worker : workers_)
        {
            auto client = worker.client.get();
            HeartbeatPayload payload;
            payload.id = worker.address;
            if (worker.status != WorkerStatus::DEAD)
            {
                client->sendHeartbeat(payload.timestamp);
                heartbeatMessages[worker.address] = payload;
            }
        }

        // Receive heartbeat responses and check if workers are alive
        for (auto &worker : workers_)
        {
            auto client = worker.client.get();
            if (worker.status != WorkerStatus::DEAD)
            {
                bool alive = client->receiveHeartbeat();
                if (!alive)
                {
                    std::cerr << "Warning: Worker " << worker.address << " is unresponsive. Marking as DEAD and initiating cleanup." << std::endl;
                    worker.status = WorkerStatus::DEAD;
                    handleDeadWorker(worker.address);
                }
            }
        }

        // Notify the system if the heartbeat process has been initialized
        if (heartbeatInitialized_)
        {
            {
                std::lock_guard<std::mutex> lock(heartbeatMutex_);
                heartbeatInitialized_ = false;
                heartbeatCondition_.notify_one();
            }
        }

        // Calculate the time taken for the heartbeat process and wait if necessary
        auto endTime = std::chrono::system_clock::now().time_since_epoch().count();
        if (endTime - currentTime < 1000000)
        {
            std::unique_lock<std::mutex> lock(heartbeatMutex_);
            sleep(1); // Sleep for 1 second to prevent excessive CPU usage
            heartbeatCondition_.wait_for(lock, std::chrono::milliseconds(5000), []
                                         { return true; });
        }
    }
}

/**
 * @brief Handles the scenario when a worker is detected as dead.
 *
 * This function is responsible for managing the cleanup and reallocation
 * of tasks when a worker is marked as DEAD. It updates the status of the
 * worker, reallocates any pending tasks, and triggers necessary cleanup
 * operations. The function ensures that the system remains consistent
 * and operational by notifying relevant conditions.
 *
 * @param workerName The name of the worker that is detected as dead.
 */
void Master::handleDeadWorker(const std::string &workerName)
{
    // Log the event of handling a dead worker
    std::cerr << "Error Code 404: Handling dead worker: " << workerName << std::endl;

    // Find the worker by name
    auto worker = findWorkerByName(workerName);
    if (!worker)
    {
        // If the worker is not found, exit the function
        return;
    }

    // Lock the cleanup mutex to ensure thread safety
    std::lock_guard<std::mutex> lock(cleanupMutex_);

    // Check if the worker is a MAPPER and operations are not completed
    if (worker->type == WorkerType::MAPPER && !operationsCompleted_)
    {
        // Reallocate the shard assigned to the dead worker
        missingShards_.push_back(worker->currentShard);
        assignedShards_++;

        // Update the worker's status to DEAD
        worker->status = WorkerStatus::DEAD;

        // Perform cleanup operations
        cleanupFiles();

        // Notify one waiting thread that cleanup can proceed
        cleanupCondition_.notify_one();
    }
    // Check if the worker is a REDUCER
    else if (worker->type == WorkerType::REDUCER)
    {
        // Reallocate the output assigned to the dead worker
        missingOutputs_.push_back(worker->currentOutput);
        assignedPartitions_++;

        // Update the worker's status to DEAD
        worker->status = WorkerStatus::DEAD;

        // Perform cleanup operations
        cleanupFiles();

        // Notify one waiting thread that cleanup can proceed
        cleanupCondition_.notify_one();
    }

    // Notify all waiting threads that operations can proceed
    operationsCondition_.notify_all();
}

/**
 * @brief Cleans up temporary and output files associated with dead workers.
 *
 * This function iterates over workers marked as DEAD and removes any temporary
 * or output files associated with them. It ensures that resources are freed
 * and the system is kept clean. If the server is not alive, it removes all
 * temporary files.
 */
void Master::cleanupFiles()
{
    // Check if there are any dead workers and the server is still running
    if (!findWorkersByStatus(WorkerStatus::DEAD).empty() && serverState_ == SERVER_ALIVE)
    {
        // Iterate over each dead worker
        for (int index : findWorkersByStatus(WorkerStatus::DEAD))
        {
            auto &worker = workers_[index];

            // Handle cleanup for MAPPER type workers
            if (worker.type == WorkerType::MAPPER && !worker.deadHandled)
            {
                // Extract the port from the worker's address
                std::string workerPort = worker.address.substr(worker.address.find_first_of(':'));

                // Remove temporary files associated with the worker's port
                for (const auto &entry : fs::directory_iterator(TEMP_DIRECTORY))
                {
                    if (entry.path().string().find(workerPort) != std::string::npos)
                    {
                        fs::remove(entry);
                        std::cout << "Removed temporary file: " << entry.path() << " for worker port: " << workerPort << std::endl;
                    }
                }
            }
            // Handle cleanup for REDUCER type workers
            else if (!worker.deadHandled)
            {
                // Remove output files associated with the worker
                for (const auto &outputFile : worker.outputFiles)
                {
                    fs::remove(outputFile.first);
                    std::cout << "Removed output file: " << outputFile.first << " for worker: " << worker.address << std::endl;
                }
            }
            // Mark the worker as handled
            worker.deadHandled = true;
        }
    }

    // If the server is not alive, remove all temporary files
    if (!serverState_)
    {
        fs::remove_all(TEMP_DIRECTORY);
        std::cout << "Server is not alive. Removed all temporary files in directory: " << TEMP_DIRECTORY << std::endl;
    }
}

/**
 * @brief Executes the main workflow of the Master node, managing map and reduce tasks.
 *
 * This function orchestrates the entire MapReduce process by initializing necessary threads,
 * assigning map and reduce tasks to workers, and handling the completion of these tasks.
 * It ensures that resources are cleaned up and the system is properly shut down after execution.
 *
 * @return true if the process completes successfully, false otherwise.
 */
bool Master::run()
{
    // Start the heartbeat monitoring thread
    std::thread heartbeatThread(&Master::heartbeat, this);

    // Create a temporary directory for intermediate files
    fs::create_directory(TEMP_DIRECTORY);

    // Clear the output directory if it exists
    if (fs::is_directory(spec_.output_dir))
    {
        for (const auto &entry : fs::directory_iterator(spec_.output_dir))
        {
            fs::remove(entry.path());
        }
    }

    // Start the asynchronous map processing thread
    std::thread mapThread(&Master::asyncMap, this);

    // Wait for the heartbeat to initialize
    {
        std::unique_lock<std::mutex> lock(heartbeatMutex_);
        heartbeatInitialized_ = true;
        heartbeatCondition_.wait(lock, [this]
                                 { return !heartbeatInitialized_; });
    }

    // Initialize variables for tracking map task completion
    bool shardsCompleted = false;
    completionCount_ = assignedShards_ = static_cast<int>(fileShards_.size());

    // Assign map tasks to available workers
    while (!shardsCompleted)
    {
        for (const auto &shard : fileShards_)
        {
            int workerIndex;
            {
                std::unique_lock<std::mutex> lock(cleanupMutex_);
                cleanupCondition_.wait(lock, [this]
                                       { return !findWorkersByStatus(WorkerStatus::FREE).empty(); });
                workerIndex = findWorkersByStatus(WorkerStatus::FREE)[0];
                if (workers_[workerIndex].status == WorkerStatus::DEAD)
                    continue;
                workers_[workerIndex].currentShard = shard;
                workers_[workerIndex].status = WorkerStatus::BUSY;
                assignedShards_--;
            }
            auto client = workers_[workerIndex].client.get();
            std::cout << "Assigning map work for shard ID " << shard.shard_id << " to worker at " << workers_[workerIndex].address << std::endl;
            client->scheduleMapJob(spec_, workers_[workerIndex].currentShard);
        }

        // Wait for map operations to complete
        {
            std::unique_lock<std::mutex> lock(operationsMutex_);
            operationsCondition_.wait(lock);
            if (assignedShards_ <= 0 && operationsCompleted_)
            {
                shardsCompleted = true;
            }
        }

        // Reassign any missing shards
        {
            std::unique_lock<std::mutex> lock(cleanupMutex_);
            if (assignedShards_ > 0 && !missingShards_.empty())
            {
                std::cout << "Reassigning work for shard ID " << missingShards_[0].shard_id << std::endl;
                fileShards_.clear();
                fileShards_.assign(missingShards_.begin(), missingShards_.end());
                missingShards_.clear();
                cleanupCondition_.notify_one();
            }
        }
    }
    mapThread.join();
    std::cout << "Map phase completed successfully." << std::endl;

    // Prepare workers for the reduce phase
    for (auto &worker : workers_)
    {
        if (worker.status == WorkerStatus::DEAD)
            continue;
        worker.status = WorkerStatus::FREE;
        worker.type = WorkerType::REDUCER;
    }
    operationsCompleted_ = false;

    // Start the asynchronous reduce processing thread
    std::thread reduceThread(&Master::asyncReduce, this);

    // Initialize variables for tracking reduce task completion
    bool partitionsCompleted = false;
    completionCount_ = assignedPartitions_ = spec_.num_output_files;
    std::vector<int> outputIndices(assignedPartitions_);
    std::iota(outputIndices.begin(), outputIndices.end(), 0);

    // Assign reduce tasks to available workers
    while (!partitionsCompleted && assignedPartitions_ > 0)
    {
        for (auto &outputIndex : outputIndices)
        {
            int workerIndex;
            std::string outputFile;
            {
                std::unique_lock<std::mutex> lock(cleanupMutex_);
                cleanupCondition_.wait(lock, [this]
                                       { return !findWorkersByStatus(WorkerStatus::FREE).empty(); });
                workerIndex = findWorkersByStatus(WorkerStatus::FREE)[0];
                if (workers_[workerIndex].status == WorkerStatus::DEAD)
                    continue;
                workers_[workerIndex].type = WorkerType::REDUCER;
                outputFile = spec_.output_dir + "/output_file_" + std::to_string(outputIndex);
                workers_[workerIndex].outputFiles[outputFile] = assignFilesToReducer(outputIndex);
                workers_[workerIndex].currentOutput = outputIndex;
                workers_[workerIndex].status = WorkerStatus::BUSY;
                assignedPartitions_--;
            }
            cleanupCondition_.notify_one();
            auto client = workers_[workerIndex].client.get();
            std::cout << "Assigning reduce work for " << outputFile << " to worker at " << workers_[workerIndex].address << std::endl;
            client->scheduleReduceJob(spec_, workers_[workerIndex].outputFiles[outputFile], outputFile);
        }

        // Wait for reduce operations to complete
        {
            std::unique_lock<std::mutex> lock(operationsMutex_);
            operationsCondition_.wait(lock);
            if (assignedPartitions_ <= 0 && operationsCompleted_)
            {
                partitionsCompleted = true;
            }
        }

        // Reassign any missing outputs
        {
            std::unique_lock<std::mutex> lock(cleanupMutex_);
            if (assignedPartitions_ > 0 && !missingOutputs_.empty())
            {
                std::cout << "Reassigning work for output file " << missingOutputs_[0] << std::endl;
                outputIndices.clear();
                outputIndices.assign(missingOutputs_.begin(), missingOutputs_.end());
                missingOutputs_.clear();
                cleanupCondition_.notify_one();
            }
        }
    }
    reduceThread.join();

    // Signal the heartbeat thread to stop and clean up resources
    {
        std::unique_lock<std::mutex> lock(heartbeatMutex_);
        serverState_ = !SERVER_ALIVE;
        heartbeatCondition_.notify_all();
    }
    heartbeatThread.join();
    cleanupFiles();
    return true;
}

/**
 * @brief Asynchronously processes map tasks and manages worker status.
 *
 * This function continuously listens for completed asynchronous map tasks
 * from the completion queue. It updates the status of workers, manages the
 * completion count, and stores the intermediate files generated by the map tasks.
 * The function ensures that workers are marked as free once their tasks are
 * completed and notifies the system when all operations are done.
 */
void Master::asyncMap()
{
    void *tag;
    bool ok = false;

    // Continuously process completed asynchronous calls from the completion queue.
    while (completionQueue_->Next(&tag, &ok))
    {
        auto call = static_cast<AsyncCall *>(tag);

        // Check if the call was successful.
        if (call->status.ok())
        {
            // Ensure the worker is not marked as DEAD.
            if (findWorkerByName(call->workerAddress)->status != WorkerStatus::DEAD)
            {
                {
                    std::lock_guard<std::mutex> lock(workerMutex_);
                    // Update the status of the worker to FREE and decrement the completion count.
                    for (auto &worker : workers_)
                    {
                        if (worker.address == call->workerAddress)
                        {
                            std::cout << "Worker " << call->workerAddress << " is now free." << std::endl;
                            worker.status = WorkerStatus::FREE;
                            completionCount_--;
                            std::cout << "Response received from " << call->workerAddress
                                      << ". Current Completion Count: " << completionCount_
                                      << ", Assigned Shards: " << assignedShards_ << std::endl;
                            break;
                        }
                    }
                    // Notify one waiting thread that a worker is now free.
                    workerCondition_.notify_one();
                }

                // If the call is a map job, store the intermediate file names.
                if (call->isMapJob)
                {
                    auto mapCall = dynamic_cast<MapCall *>(call);
                    for (const auto &file : mapCall->result.file_list())
                    {
                        intermediateFiles_.push_back(file);
                    }
                }

                {
                    std::unique_lock<std::mutex> lock(operationsMutex_);
                    // Check if all operations are completed.
                    if (completionCount_ == 0)
                    {
                        operationsCompleted_ = true;
                        operationsCondition_.notify_one();
                        break;
                    }
                }
            }
            // Notify any waiting threads that cleanup can proceed.
            cleanupCondition_.notify_one();
        }
        // Clean up the call object.
        delete call;
    }
}

/**
 * @brief Asynchronously processes reduce tasks and manages worker status.
 *
 * This function continuously listens for completed asynchronous reduce tasks
 * from the completion queue. It updates the status of workers, manages the
 * completion count, and stores the output files generated by the reduce tasks.
 * The function ensures that workers are marked as free once their tasks are
 * completed and notifies the system when all operations are done.
 */
void Master::asyncReduce()
{
    void *tag;
    bool ok = false;

    // Continuously process completed asynchronous calls from the completion queue.
    while (completionQueue_->Next(&tag, &ok))
    {
        auto call = static_cast<AsyncCall *>(tag);

        // Check if the call was successful.
        if (call->status.ok())
        {
            // Ensure the worker is not marked as DEAD.
            if (findWorkerByName(call->workerAddress)->status != WorkerStatus::DEAD)
            {
                {
                    std::lock_guard<std::mutex> lock(workerMutex_);
                    // Update the status of the worker to FREE and decrement the completion count.
                    for (auto &worker : workers_)
                    {
                        if (worker.address == call->workerAddress)
                        {
                            worker.status = WorkerStatus::FREE;
                            completionCount_--;
                            std::cout << "Worker " << call->workerAddress << " has completed its task. "
                                      << "Current Completion Count: " << completionCount_
                                      << ", Assigned Partitions: " << assignedPartitions_ << std::endl;
                            break;
                        }
                    }
                    // Notify one waiting thread that a worker is now free.
                    workerCondition_.notify_one();
                }

                // If the call is a reduce job, store the output file name.
                if (!call->isMapJob)
                {
                    auto reduceCall = dynamic_cast<ReduceCall *>(call);
                    outputFiles_.push_back(reduceCall->result.file_name());
                }

                {
                    std::unique_lock<std::mutex> lock(operationsMutex_);
                    // Check if all operations are completed.
                    if (completionCount_ == 0)
                    {
                        operationsCompleted_ = true;
                        operationsCondition_.notify_one();
                        break;
                    }
                }
            }
            // Notify one waiting thread that cleanup can proceed.
            cleanupCondition_.notify_one();
        }
        // Clean up the call object.
        delete call;
    }
}

/**
 * @brief Assigns intermediate files to a reducer based on the output ID.
 *
 * This function distributes intermediate files among reducers by assigning
 * files to a specific reducer identified by the output ID. The assignment
 * is done in a round-robin fashion using the modulo operation.
 *
 * @param outputId The ID of the output file (reducer) to which files are assigned.
 * @return A vector of strings containing the file names assigned to the specified reducer.
 */
std::vector<std::string> Master::assignFilesToReducer(int outputId)
{
    std::set<std::string> fileSet; // Use a set to ensure unique file names are collected.

    // Iterate over all intermediate files.
    for (size_t i = 0; i < intermediateFiles_.size(); ++i)
    {
        // Assign file to the reducer if the index modulo the number of output files equals the output ID.
        if (i % spec_.num_output_files == outputId)
        {
            fileSet.insert(intermediateFiles_[i]); // Insert the file into the set.
        }
    }

    // Convert the set of file names to a vector and return it.
    return std::vector<std::string>(fileSet.begin(), fileSet.end());
}
