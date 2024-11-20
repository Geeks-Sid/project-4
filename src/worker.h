// worker.h
#pragma once

#include <iostream>
#include <thread>
#include <utility>
#include <filesystem>
#include <grpcpp/grpcpp.h>

#include "masterworker.grpc.pb.h"
#include "file_shard.h"
#include "mr_tasks.h"
#include "mr_task_factory.h"

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string &user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string &user_id);

/**
 * @brief Base class for handling gRPC asynchronous calls.
 */
class BaseHandler
{
public:
    /**
     * @brief Constructor for BaseHandler.
     * @param service Pointer to the gRPC service.
     * @param queue Pointer to the completion queue.
     * @param worker_address Address of the worker.
     */
    BaseHandler(masterworker::Map_Reduce::AsyncService *service,
                grpc::ServerCompletionQueue *queue,
                const std::string &worker_address)
        : service_(service),
          completion_queue_(queue),
          worker_address_(worker_address),
          status_(CallStatus::CREATE)
    {
    }

    virtual ~BaseHandler() = default;

    /**
     * @brief Proceed to the next state of the handler.
     */
    virtual void Proceed() = 0;

protected:
    masterworker::Map_Reduce::AsyncService *service_; ///< Pointer to the gRPC service.
    grpc::ServerCompletionQueue *completion_queue_;   ///< Pointer to the completion queue.
    std::string worker_address_;                      ///< Address of the worker.

    grpc::ServerContext context_; ///< Server context for the call.

    enum class CallStatus
    {
        CREATE,
        PROCESS,
        FINISH
    };
    CallStatus status_; ///< Current status of the call.
};

/**
 * @brief Handler class for Map requests.
 */
class MapperHandler final : public BaseHandler
{
public:
    /**
     * @brief Constructor for MapperHandler.
     * @param service Pointer to the gRPC service.
     * @param queue Pointer to the completion queue.
     * @param worker_address Address of the worker.
     */
    MapperHandler(masterworker::Map_Reduce::AsyncService *service,
                  grpc::ServerCompletionQueue *queue,
                  const std::string &worker_address)
        : BaseHandler(service, queue, worker_address),
          responder_(&context_)
    {
        Proceed();
    }

    void Proceed() override
    {
        if (status_ == CallStatus::CREATE)
        {
            status_ = CallStatus::PROCESS;
            service_->Requestmap(&context_, &request_, &responder_, completion_queue_, completion_queue_, this);
        }
        else if (status_ == CallStatus::PROCESS)
        {
            // Spawn a new instance to serve new clients while we process the current one.
            new MapperHandler(service_, completion_queue_, worker_address_);
            // Handle the Map request.
            response_ = HandleMapRequest(request_);
            status_ = CallStatus::FINISH;
            responder_.Finish(response_, grpc::Status::OK, this);
        }
        else
        {
            GPR_ASSERT(status_ == CallStatus::FINISH);
            delete this;
        }
    }

private:
    masterworker::Map_Request request_;                                     ///< The Map request.
    masterworker::Map_Response response_;                                   ///< The Map response.
    grpc::ServerAsyncResponseWriter<masterworker::Map_Response> responder_; ///< Asynchronous responder.

    /**
     * @brief Handles the Map request.
     * @param request The Map request.
     * @return The Map response.
     */
    masterworker::Map_Response HandleMapRequest(const masterworker::Map_Request &request);

    /**
     * @brief Converts a gRPC partition to a FileShard.
     * @param partition The gRPC partition.
     * @return The FileShard.
     */
    FileShard ConvertGrpcPartitionToFileShard(const masterworker::partition &partition);
};

/**
 * @brief Handler class for Reduce requests.
 */
class ReducerHandler final : public BaseHandler
{
public:
    /**
     * @brief Constructor for ReducerHandler.
     * @param service Pointer to the gRPC service.
     * @param queue Pointer to the completion queue.
     * @param worker_address Address of the worker.
     */
    ReducerHandler(masterworker::Map_Reduce::AsyncService *service,
                   grpc::ServerCompletionQueue *queue,
                   const std::string &worker_address)
        : BaseHandler(service, queue, worker_address),
          responder_(&context_)
    {
        Proceed();
    }

    void Proceed() override
    {
        if (status_ == CallStatus::CREATE)
        {
            status_ = CallStatus::PROCESS;
            service_->Requestreduce(&context_, &request_, &responder_, completion_queue_, completion_queue_, this);
        }
        else if (status_ == CallStatus::PROCESS)
        {
            // Spawn a new instance to serve new clients while we process the current one.
            new ReducerHandler(service_, completion_queue_, worker_address_);
            // Handle the Reduce request.
            response_ = HandleReduceRequest(request_);
            status_ = CallStatus::FINISH;
            responder_.Finish(response_, grpc::Status::OK, this);
        }
        else
        {
            GPR_ASSERT(status_ == CallStatus::FINISH);
            delete this;
        }
    }

private:
    masterworker::Reduce_Request request_;                                     ///< The Reduce request.
    masterworker::Reduce_Response response_;                                   ///< The Reduce response.
    grpc::ServerAsyncResponseWriter<masterworker::Reduce_Response> responder_; ///< Asynchronous responder.

    /**
     * @brief Handles the Reduce request.
     * @param request The Reduce request.
     * @return The Reduce response.
     */
    masterworker::Reduce_Response HandleReduceRequest(const masterworker::Reduce_Request &request);
};

/**
 * @brief Handler class for Heartbeat requests.
 */
class HeartbeatHandler final : public BaseHandler
{
public:
    /**
     * @brief Constructor for HeartbeatHandler.
     * @param service Pointer to the gRPC service.
     * @param queue Pointer to the completion queue.
     * @param worker_address Address of the worker.
     */
    HeartbeatHandler(masterworker::Map_Reduce::AsyncService *service,
                     grpc::ServerCompletionQueue *queue,
                     const std::string &worker_address)
        : BaseHandler(service, queue, worker_address),
          responder_(&context_)
    {
        Proceed();
    }

    void Proceed() override
    {
        if (status_ == CallStatus::CREATE)
        {
            status_ = CallStatus::PROCESS;
            service_->Requestheartbeat(&context_, &request_, &responder_, completion_queue_, completion_queue_, this);
        }
        else if (status_ == CallStatus::PROCESS)
        {
            // Spawn a new instance to serve new clients while we process the current one.
            new HeartbeatHandler(service_, completion_queue_, worker_address_);
            // Handle the Heartbeat request.
            response_ = HandleHeartbeatRequest(request_);
            status_ = CallStatus::FINISH;
            responder_.Finish(response_, grpc::Status::OK, this);
        }
        else
        {
            GPR_ASSERT(status_ == CallStatus::FINISH);
            delete this;
        }
    }

private:
    masterworker::Heartbeat_Payload request_;                                    ///< The Heartbeat request.
    masterworker::Heartbeat_Payload response_;                                   ///< The Heartbeat response.
    grpc::ServerAsyncResponseWriter<masterworker::Heartbeat_Payload> responder_; ///< Asynchronous responder.

    /**
     * @brief Handles the Heartbeat request.
     * @param request The Heartbeat request.
     * @return The Heartbeat response.
     */
    masterworker::Heartbeat_Payload HandleHeartbeatRequest(const masterworker::Heartbeat_Payload &request);
};

/**
 * @brief Worker class responsible for handling MapReduce tasks.
 */
class Worker
{
public:
    /**
     * @brief Constructs a Worker instance.
     * @param ip_address_port The IP address and port to listen on (e.g., "localhost:50051").
     */
    Worker(const std::string &ip_address_port);

    /**
     * @brief Runs the Worker server.
     * @return True if the server starts successfully, false otherwise.
     */
    bool run();

    ~Worker();

    // Static helper methods to get internal implementations
    static BaseReducerInternal *GetBaseReducerInternal(BaseReducer *reducer);
    static BaseMapperInternal *GetBaseMapperInternal(BaseMapper *mapper);

private:
    grpc::ServerBuilder builder_;                                  ///< Server builder.
    std::unique_ptr<grpc::ServerCompletionQueue> work_queue_;      ///< Completion queue for work.
    std::unique_ptr<grpc::ServerCompletionQueue> heartbeat_queue_; ///< Completion queue for heartbeat.
    masterworker::Map_Reduce::AsyncService service_;               ///< The gRPC service.
    std::unique_ptr<grpc::Server> server_;                         ///< The gRPC server.

    std::string worker_uuid_; ///< Unique identifier for the worker.

    std::thread heartbeat_thread_; ///< Thread for handling heartbeat messages.
    bool clean_exit_ = false;      ///< Flag indicating if a clean exit is requested.

    /**
     * @brief Handler function for heartbeat messages.
     */
    void HandleHeartbeat();
};

/**
 * @brief Constructs a Worker instance.
 *
 * This constructor initializes a Worker object, setting up the gRPC server
 * to listen on the specified IP address and port. It also prepares the
 * necessary completion queues for handling work and heartbeat messages.
 *
 * @param ip_address_port The IP address and port to listen on (e.g., "localhost:50051").
 */
Worker::Worker(const std::string &ip_address_port)
    : worker_uuid_(ip_address_port.substr(ip_address_port.find_first_of(':') + 1)) // Extracts the port as a unique identifier
{
    // Log the initialization of the worker with the specified address
    std::cout << "[INFO] Worker initialized and listening on: " << ip_address_port << std::endl;

    // Configure the server to listen on the provided address with insecure credentials
    builder_.AddListeningPort(ip_address_port, grpc::InsecureServerCredentials());

    // Register the gRPC service to handle incoming requests
    builder_.RegisterService(&service_);

    // Create completion queues for handling work and heartbeat operations
    work_queue_ = builder_.AddCompletionQueue();
    heartbeat_queue_ = builder_.AddCompletionQueue();
}

/**
 * @brief Destructor for the Worker class.
 *
 * This destructor ensures a clean shutdown of the Worker by stopping the server
 * and shutting down the completion queues. It also waits for the heartbeat thread
 * to finish if it is still running.
 */
Worker::~Worker()
{
    // Indicate that a clean exit is requested
    clean_exit_ = true;

    // Shutdown the gRPC server to stop accepting new requests
    server_->Shutdown();

    // Shutdown the completion queues to stop processing further operations
    work_queue_->Shutdown();
    heartbeat_queue_->Shutdown();

    // Wait for the heartbeat thread to complete if it is joinable
    if (heartbeat_thread_.joinable())
    {
        heartbeat_thread_.join();
    }
}

/**
 * @brief Runs the Worker server.
 *
 * This function initializes and starts the gRPC server, setting up the necessary
 * handlers for processing Map and Reduce requests. It also manages the heartbeat
 * handler thread to monitor worker status. The function enters a main processing
 * loop to handle incoming requests until the server is shut down.
 *
 * @return True if the server starts and runs successfully, false otherwise.
 */
bool Worker::run()
{
    // Attempt to build and start the gRPC server
    server_ = builder_.BuildAndStart();
    if (!server_)
    {
        std::cerr << "[ERROR] Failed to start the gRPC server." << std::endl;
        return false;
    }
    std::cout << "[INFO] gRPC server started successfully." << std::endl;

    // Launch the heartbeat handler thread to manage heartbeat messages
    heartbeat_thread_ = std::thread(&Worker::HandleHeartbeat, this);
    std::cout << "[INFO] Heartbeat handler thread started." << std::endl;

    // Instantiate handlers for Map and Reduce requests
    new MapperHandler(&service_, work_queue_.get(), worker_uuid_);
    new ReducerHandler(&service_, work_queue_.get(), worker_uuid_);
    std::cout << "[INFO] Map and Reduce handlers initialized." << std::endl;

    void *tag; // Pointer to uniquely identify a request
    bool ok;   // Status flag for request processing

    // Main processing loop to handle incoming requests
    while (true)
    {
        // Wait for the next request from the completion queue
        if (!work_queue_->Next(&tag, &ok))
        {
            std::cout << "[INFO] Completion queue is shutting down." << std::endl;
            break; // Exit loop if the queue is shutting down
        }

        if (ok)
        {
            // Proceed with the request if it was processed successfully
            static_cast<BaseHandler *>(tag)->Proceed();
        }
        else
        {
            // Log an error if the request processing failed
            std::cerr << "[ERROR] Error processing request." << std::endl;
        }
    }

    // Ensure the heartbeat thread is joined before exiting
    if (heartbeat_thread_.joinable())
    {
        heartbeat_thread_.join();
        std::cout << "[INFO] Heartbeat handler thread joined." << std::endl;
    }
    return true;
}

/**
 * @brief Handles incoming heartbeat requests from the gRPC server.
 *
 * This function continuously listens for heartbeat requests from the completion queue
 * and processes them. It ensures that the worker is alive and responsive by handling
 * each heartbeat request appropriately. The function runs in a loop until a clean exit
 * is requested, at which point it stops processing further requests.
 */
void Worker::HandleHeartbeat()
{
    // Create a unique pointer for the HeartbeatHandler to manage heartbeat requests
    std::unique_ptr<HeartbeatHandler> handler = std::make_unique<HeartbeatHandler>(&service_, heartbeat_queue_.get(), worker_uuid_);

    void *tag; // Pointer to uniquely identify a request
    bool ok;   // Status flag indicating if the request was processed successfully

    // Continuously process heartbeat requests until a clean exit is requested
    while (!clean_exit_)
    {
        // Wait for the next heartbeat request from the completion queue
        if (!heartbeat_queue_->Next(&tag, &ok))
        {
            std::cout << "[INFO] Heartbeat completion queue is shutting down." << std::endl;
            break; // Exit loop if the queue is shutting down
        }

        if (ok)
        {
            // Proceed with the request if it was processed successfully
            static_cast<BaseHandler *>(tag)->Proceed();
        }
        else
        {
            // Log an error if the request processing failed
            std::cerr << "[ERROR] Failed to process heartbeat request." << std::endl;
        }
    }
}

/**
 * @brief Static helper method to retrieve the internal implementation of a BaseReducer.
 *
 * This function provides access to the internal implementation of a BaseReducer object.
 *
 * @param reducer Pointer to the BaseReducer object.
 * @return Pointer to the internal implementation of the BaseReducer.
 */
BaseReducerInternal *Worker::GetBaseReducerInternal(BaseReducer *reducer)
{
    return reducer->impl_;
}

/**
 * @brief Static helper method to retrieve the internal implementation of a BaseMapper.
 *
 * This function provides access to the internal implementation of a BaseMapper object.
 *
 * @param mapper Pointer to the BaseMapper object.
 * @return Pointer to the internal implementation of the BaseMapper.
 */
BaseMapperInternal *Worker::GetBaseMapperInternal(BaseMapper *mapper)
{
    return mapper->impl_;
}

/**
 * @brief Handles a Map request by processing file shards and generating intermediate files.
 *
 * This function processes a Map request by retrieving the user-defined mapper function,
 * converting gRPC partitions to file shards, and processing each file segment. It generates
 * intermediate files and adds them to the response.
 *
 * @param request The Map request containing file shards and partition information.
 * @return The Map response containing the list of intermediate files.
 */
masterworker::Map_Response MapperHandler::HandleMapRequest(const masterworker::Map_Request &request)
{
    masterworker::Map_Response response;

    // Retrieve the user-defined mapper function from the task factory using the UUID.
    auto user_mapper = get_mapper_from_task_factory(request.uuid());
    if (!user_mapper)
    {
        std::cerr << "[ERROR] Mapper function not found for UUID: " << request.uuid() << std::endl;
        return response;
    }

    // Retrieve the internal implementation of BaseMapper.
    auto base_mapper_internal = Worker::GetBaseMapperInternal(user_mapper.get());
    if (!base_mapper_internal)
    {
        std::cerr << "[ERROR] BaseMapperInternal is null." << std::endl;
        return response;
    }

    // Prepare a list of intermediate files based on the partition count.
    int partition_count = request.partition_count();
    base_mapper_internal->intermediate_files.reserve(partition_count);
    for (int i = 0; i < partition_count; ++i)
    {
        std::string intermediate_file = std::string(TEMP_DIR) + "/" +
                                        std::to_string(i) + "_" +
                                        worker_address_ + ".txt";
        base_mapper_internal->intermediate_files.push_back(intermediate_file);
    }

    // Process each shard in the request.
    for (int shard_index = 0; shard_index < request.shard_size(); ++shard_index)
    {
        // Convert gRPC partition to FileShard.
        FileShard file_shard = ConvertGrpcPartitionToFileShard(request.shard(shard_index));

        // Process each file segment in the shard.
        for (const auto &segment : file_shard.segments)
        {
            std::ifstream file_stream(segment.filename, std::ios::binary);
            if (!file_stream)
            {
                std::cerr << "[ERROR] Unable to open file: " << segment.filename << std::endl;
                continue; // Skip this segment if the file cannot be opened.
            }

            // Read the file segment into a buffer.
            file_stream.seekg(segment.offsets.first);
            std::string buffer(segment.offsets.second - segment.offsets.first, '\0');
            file_stream.read(&buffer[0], segment.offsets.second - segment.offsets.first);

            if (!file_stream)
            {
                std::cerr << "[ERROR] Failed to read file segment from: " << segment.filename << std::endl;
                continue; // Skip this segment if reading fails.
            }

            // Process each line in the buffer using the user-defined mapper function.
            std::stringstream stream(buffer);
            std::string line;
            while (std::getline(stream, line))
            {
                user_mapper->map(line);
            }
        }
    }

    // Perform a final flush of the mapper to ensure all data is processed.
    base_mapper_internal->final_flush();

    // Add the list of intermediate files to the response.
    for (const auto &file : base_mapper_internal->intermediate_files)
    {
        response.add_file_list(file);
    }

    return response;
}

/**
 * @brief Converts a gRPC partition to a FileShard.
 *
 * This function takes a gRPC partition object and converts it into a FileShard
 * structure, which contains a list of file segments with their respective filenames
 * and byte offsets.
 *
 * @param partition The gRPC partition object containing file segment information.
 * @return A FileShard object populated with the file segments from the partition.
 */
FileShard MapperHandler::ConvertGrpcPartitionToFileShard(const masterworker::partition &partition)
{
    FileShard shard;
    shard.shard_id = partition.shard_id(); // Set the shard ID from the partition

    // Iterate over each file segment in the partition's file list
    for (const auto &file_segment : partition.file_list())
    {
        FileSegment segment;
        segment.filename = file_segment.filename();                                 // Set the filename for the segment
        segment.offsets = {file_segment.start_offset(), file_segment.end_offset()}; // Set the start and end offsets
        shard.segments.push_back(segment);                                          // Add the segment to the shard's list of segments
    }

    return shard; // Return the constructed FileShard
}

/**
 * @brief Handles a Reduce request by processing intermediate files and applying the user-defined reducer function.
 *
 * This function reads intermediate files specified in the request, extracts key-value pairs,
 * and applies the user-defined reducer function to each key. The results are written to the output file
 * specified in the request.
 *
 * @param request The Reduce request containing the list of intermediate files and output file name.
 * @return A Reduce_Response object containing the name of the output file.
 */
masterworker::Reduce_Response ReducerHandler::HandleReduceRequest(const masterworker::Reduce_Request &request)
{
    masterworker::Reduce_Response response;
    response.set_file_name(request.output_file());

    // Retrieve the user-defined reducer function using the UUID from the request.
    auto user_reducer = get_reducer_from_task_factory(request.uuid());
    if (!user_reducer)
    {
        std::cerr << "[ERROR] Reducer function not found for UUID: " << request.uuid() << std::endl;
        return response;
    }

    // Obtain the internal implementation of BaseReducer.
    auto base_reducer_internal = Worker::GetBaseReducerInternal(user_reducer.get());
    if (!base_reducer_internal)
    {
        std::cerr << "[ERROR] BaseReducerInternal is null." << std::endl;
        return response;
    }

    // Set the output file for the reducer.
    base_reducer_internal->output_file = request.output_file();

    // Map to store key-value pairs extracted from intermediate files.
    std::map<std::string, std::vector<std::string>> key_value_map;

    // Iterate over each intermediate file specified in the request.
    for (const auto &file_name : request.file_list())
    {
        std::ifstream file_stream(file_name);
        if (!file_stream)
        {
            std::cerr << "[ERROR] Unable to open intermediate file: " << file_name << std::endl;
            continue; // Skip this file if it cannot be opened.
        }

        std::string line;
        // Read each line from the file and extract key-value pairs.
        while (std::getline(file_stream, line))
        {
            size_t delimiter_pos = line.find_first_of(DELIMITER);
            if (delimiter_pos == std::string::npos)
            {
                std::cerr << "[WARNING] Invalid line format in file " << file_name << ": " << line << std::endl;
                continue; // Skip lines that do not contain a valid delimiter.
            }
            std::string key = line.substr(0, delimiter_pos);
            std::string value = line.substr(delimiter_pos + 1);
            key_value_map[key].push_back(value);
        }
    }

    // Apply the reduce function to each key in the map.
    for (const auto &kv : key_value_map)
    {
        user_reducer->reduce(kv.first, kv.second);
    }
    key_value_map.clear(); // Clear the map after processing.

    return response; // Return the response containing the output file name.
}

/**
 * @brief Handles a heartbeat request from a worker and generates a response.
 *
 * This method processes an incoming heartbeat request by extracting the worker's
 * unique identifier and setting the status to ALIVE in the response. The response
 * is then returned to acknowledge the worker's active status.
 *
 * @param request The incoming heartbeat request containing the worker's ID.
 * @return A Heartbeat_Payload response with the worker's ID and status set to ALIVE.
 */
masterworker::Heartbeat_Payload HeartbeatHandler::HandleHeartbeatRequest(const masterworker::Heartbeat_Payload &request)
{
    // Create a response object to send back to the worker
    masterworker::Heartbeat_Payload response;

    // Set the worker's ID in the response to match the incoming request
    response.set_id(request.id());

    // Set the worker's status to ALIVE to indicate the worker is active
    response.set_status(masterworker::Heartbeat_Payload_type_ALIVE);

    // Return the response to the worker
    return response;
}
