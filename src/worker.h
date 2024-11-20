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
        Proceed();
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
    void HeartbeatHandler();
};

// Implementation of Worker methods
Worker::Worker(const std::string &ip_address_port)
    : worker_uuid_(ip_address_port.substr(ip_address_port.find_first_of(':') + 1))
{
    std::cout << "[Worker] listening on " << ip_address_port << std::endl;
    builder_.AddListeningPort(ip_address_port, grpc::InsecureServerCredentials());
    builder_.RegisterService(&service_);
    work_queue_ = builder_.AddCompletionQueue();
    heartbeat_queue_ = builder_.AddCompletionQueue();
}

Worker::~Worker()
{
    clean_exit_ = true;
    server_->Shutdown();

    // Shutdown the completion queues
    work_queue_->Shutdown();
    heartbeat_queue_->Shutdown();

    if (heartbeat_thread_.joinable())
    {
        heartbeat_thread_.join();
    }
}

bool Worker::run()
{
    server_ = builder_.BuildAndStart();
    if (!server_)
    {
        std::cerr << "Failed to start the server." << std::endl;
        return false;
    }

    // Start the heartbeat handler thread
    heartbeat_thread_ = std::thread(&Worker::HeartbeatHandler, this);

    // Create new handlers for Map and Reduce requests
    new MapperHandler(&service_, work_queue_.get(), worker_uuid_);
    new ReducerHandler(&service_, work_queue_.get(), worker_uuid_);

    void *tag; // uniquely identifies a request.
    bool ok;

    // Main processing loop.
    while (true)
    {
        if (!work_queue_->Next(&tag, &ok))
        {
            break; // The completion queue is shutting down.
        }

        if (ok)
        {
            static_cast<BaseHandler *>(tag)->Proceed();
        }
        else
        {
            // Handle error
            std::cerr << "Error processing request." << std::endl;
        }
    }

    if (heartbeat_thread_.joinable())
    {
        heartbeat_thread_.join();
    }
    return true;
}

void Worker::HeartbeatHandler()
{
    new HeartbeatHandler(&service_, heartbeat_queue_.get(), worker_uuid_);

    void *tag;
    bool ok;

    while (true)
    {
        if (clean_exit_)
        {
            return;
        }

        if (!heartbeat_queue_->Next(&tag, &ok))
        {
            break; // The completion queue is shutting down.
        }

        if (ok)
        {
            static_cast<BaseHandler *>(tag)->Proceed();
        }
        else
        {
            // Handle error
            std::cerr << "Error processing heartbeat request." << std::endl;
        }
    }
}

// Static helper methods
BaseReducerInternal *Worker::GetBaseReducerInternal(BaseReducer *reducer)
{
    return reducer->impl_;
}

BaseMapperInternal *Worker::GetBaseMapperInternal(BaseMapper *mapper)
{
    return mapper->impl_;
}

// Implementation of MapperHandler methods
masterworker::Map_Response MapperHandler::HandleMapRequest(const masterworker::Map_Request &request)
{
    masterworker::Map_Response response;

    // Get the user-defined mapper function from the task factory.
    auto user_mapper = get_mapper_from_task_factory(request.uuid());
    if (!user_mapper)
    {
        std::cerr << "Error: Mapper function not found for UUID: " << request.uuid() << std::endl;
        return response;
    }

    // Get the internal implementation of BaseMapper
    auto base_mapper_internal = Worker::GetBaseMapperInternal(user_mapper.get());
    if (!base_mapper_internal)
    {
        std::cerr << "Error: BaseMapperInternal is null." << std::endl;
        return response;
    }

    // Prepare intermediate file list
    int partition_count = request.partition_count();
    base_mapper_internal->intermediate_file_list.reserve(partition_count);
    for (int i = 0; i < partition_count; ++i)
    {
        std::string intermediate_file = std::string(TEMP_DIR) + "/" +
                                        std::to_string(i) + "_" +
                                        worker_address_ + ".txt";
        base_mapper_internal->intermediate_file_list.push_back(intermediate_file);
    }

    // Process each shard in the request
    for (int shard_index = 0; shard_index < request.shard_size(); ++shard_index)
    {
        // Convert gRPC partition to FileShard
        FileShard file_shard = ConvertGrpcPartitionToFileShard(request.shard(shard_index));

        // Process each file segment in the shard
        for (const auto &segment : file_shard.segments)
        {
            std::ifstream file_stream(segment.filename, std::ios::binary);
            if (!file_stream)
            {
                std::cerr << "Error opening file: " << segment.filename << std::endl;
                continue; // Skip this segment
            }

            file_stream.seekg(segment.offsets.first);
            std::string buffer(segment.offsets.second - segment.offsets.first, '\0');
            file_stream.read(&buffer[0], segment.offsets.second - segment.offsets.first);

            if (!file_stream)
            {
                std::cerr << "Error reading file segment from: " << segment.filename << std::endl;
                continue; // Skip this segment
            }

            std::stringstream stream(buffer);
            std::string line;
            while (std::getline(stream, line))
            {
                user_mapper->map(line);
            }
        }
    }

    // Final flush of the mapper
    base_mapper_internal->final_flush();

    // Add intermediate files to the response
    for (const auto &file : base_mapper_internal->intermediate_file_list)
    {
        response.add_file_list(file);
    }

    return response;
}

FileShard MapperHandler::ConvertGrpcPartitionToFileShard(const masterworker::partition &partition)
{
    FileShard shard;
    shard.shard_id = partition.shard_id();
    for (const auto &file_segment : partition.file_list())
    {
        FileSegment segment;
        segment.filename = file_segment.filename();
        segment.offsets = {file_segment.start_offset(), file_segment.end_offset()};
        shard.segments.push_back(segment);
    }
    return shard;
}

// Implementation of ReducerHandler methods
masterworker::Reduce_Response ReducerHandler::HandleReduceRequest(const masterworker::Reduce_Request &request)
{
    masterworker::Reduce_Response response;
    response.set_file_name(request.output_file());

    // Get the user-defined reducer function from the task factory.
    auto user_reducer = get_reducer_from_task_factory(request.uuid());
    if (!user_reducer)
    {
        std::cerr << "Error: Reducer function not found for UUID: " << request.uuid() << std::endl;
        return response;
    }

    // Get the internal implementation of BaseReducer
    auto base_reducer_internal = Worker::GetBaseReducerInternal(user_reducer.get());
    if (!base_reducer_internal)
    {
        std::cerr << "Error: BaseReducerInternal is null." << std::endl;
        return response;
    }

    base_reducer_internal->file_name = request.output_file();

    // Map to store key-value pairs
    std::map<std::string, std::vector<std::string>> key_value_map;

    // Read intermediate files
    for (const auto &file_name : request.file_list())
    {
        std::ifstream file_stream(file_name);
        if (!file_stream)
        {
            std::cerr << "Error opening intermediate file: " << file_name << std::endl;
            continue; // Skip this file
        }

        std::string line;
        while (std::getline(file_stream, line))
        {
            size_t delimiter_pos = line.find_first_of(DELIMITER);
            if (delimiter_pos == std::string::npos)
            {
                std::cerr << "Warning: Invalid line format in file " << file_name << ": " << line << std::endl;
                continue; // Skip this line
            }
            std::string key = line.substr(0, delimiter_pos);
            std::string value = line.substr(delimiter_pos + 1);
            key_value_map[key].push_back(value);
        }
    }

    // Call the reduce function for each key
    for (const auto &kv : key_value_map)
    {
        user_reducer->reduce(kv.first, kv.second);
    }
    key_value_map.clear();
    return response;
}

// Implementation of HeartbeatHandler methods
masterworker::Heartbeat_Payload HeartbeatHandler::HandleHeartbeatRequest(const masterworker::Heartbeat_Payload &request)
{
    masterworker::Heartbeat_Payload response;
    response.set_id(request.id());
    response.set_status(masterworker::Heartbeat_Payload_type_ALIVE);
    return response;
}
