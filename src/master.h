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

constexpr bool SERVER_ALIVE = true;
constexpr int HEARTBEAT_TIMEOUT = 5;
constexpr const char *TEMP_DIRECTORY = "intermediate";

enum class WorkerStatus
{
    FREE,
    BUSY,
    DEAD
};

enum class WorkerType
{
    MAPPER,
    REDUCER
};

struct HeartbeatPayload
{
    std::string id;
    int64_t timestamp;
    WorkerStatus status;
};

class AsyncCall
{
public:
    bool isMapJob = true;
    grpc::ClientContext context;
    grpc::Status status;
    std::string workerAddress;

    virtual ~AsyncCall() = default;
};

class MapCall : public AsyncCall
{
public:
    masterworker::Map_Response result;
    std::unique_ptr<grpc::ClientAsyncResponseReader<masterworker::Map_Response>> responseReader;
};

class ReduceCall : public AsyncCall
{
public:
    masterworker::Reduce_Response result;
    std::unique_ptr<grpc::ClientAsyncResponseReader<masterworker::Reduce_Response>> responseReader;
};

class HeartbeatCall : public AsyncCall
{
public:
    masterworker::Heartbeat_Payload result;
    std::unique_ptr<grpc::ClientAsyncResponseReader<masterworker::Heartbeat_Payload>> responseReader;
};

class WorkerClient
{
public:
    WorkerClient(const std::string &address, grpc::CompletionQueue *queue);
    ~WorkerClient();

    void sendHeartbeat(int64_t currentTime);
    bool receiveHeartbeat();

    void scheduleMapJob(const MapReduceSpec &spec, const FileShard &shard);
    void scheduleReduceJob(const MapReduceSpec &spec, const std::vector<std::string> &fileList, const std::string &outputFile);

private:
    void convertToGrpcSpec(const FileShard &shard, masterworker::partition *partition);

    std::unique_ptr<masterworker::Map_Reduce::Stub> stub_;
    grpc::CompletionQueue *queue_;
    grpc::CompletionQueue heartbeatQueue_;
    std::string workerAddress_;
};

WorkerClient::WorkerClient(const std::string &address, grpc::CompletionQueue *queue)
    : queue_(queue), workerAddress_(address)
{
    std::cout << "Creating channel at " << address << std::endl;
    stub_ = masterworker::Map_Reduce::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
}

WorkerClient::~WorkerClient()
{
    heartbeatQueue_.Shutdown();
}

void WorkerClient::sendHeartbeat(int64_t currentTime)
{
    auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(HEARTBEAT_TIMEOUT);
    auto call = new HeartbeatCall;
    call->workerAddress = workerAddress_;
    call->context.set_deadline(deadline);
    masterworker::Heartbeat_Payload payload;
    payload.set_id(workerAddress_);
    payload.set_status(masterworker::Heartbeat_Payload::UNKNOWN);
    call->responseReader = stub_->PrepareAsyncheartbeat(&call->context, payload, &heartbeatQueue_);
    call->responseReader->StartCall();
    call->responseReader->Finish(&call->result, &call->status, (void *)call);
}

bool WorkerClient::receiveHeartbeat()
{
    void *tag;
    bool ok = false;
    GPR_ASSERT(heartbeatQueue_.Next(&tag, &ok));
    auto *call = static_cast<HeartbeatCall *>(tag);
    if (call->status.ok())
    {
        if (call->result.status() == masterworker::Heartbeat_Payload::DEAD)
        {
            std::cerr << "Error Code 401: Worker " << call->workerAddress << " reported as dead." << std::endl;
            delete call;
            return false;
        }
        delete call;
        return true;
    }
    else
    {
        std::cerr << "Error Code 402: Heartbeat error with worker " << workerAddress_
                  << ": " << call->status.error_message() << std::endl;
        delete call;
        return false;
    }
}

void WorkerClient::scheduleMapJob(const MapReduceSpec &spec, const FileShard &shard)
{
    masterworker::Map_Request request;
    request.set_uuid(spec.username);
    request.set_partition_count(spec.num_output_files);
    auto *partition = request.add_shard();
    convertToGrpcSpec(shard, partition);

    auto call = new MapCall;
    call->workerAddress = workerAddress_;
    call->isMapJob = true;
    call->responseReader = stub_->PrepareAsyncmap(&call->context, request, queue_);
    call->responseReader->StartCall();
    call->responseReader->Finish(&call->result, &call->status, (void *)call);
}

void WorkerClient::scheduleReduceJob(const MapReduceSpec &spec, const std::vector<std::string> &fileList, const std::string &outputFile)
{
    masterworker::Reduce_Request request;
    request.set_uuid(spec.username);
    request.set_output_file(outputFile);
    for (const auto &file : fileList)
    {
        request.add_file_list(file);
    }

    auto call = new ReduceCall;
    call->workerAddress = workerAddress_;
    call->isMapJob = false;
    call->responseReader = stub_->PrepareAsyncreduce(&call->context, request, queue_);
    call->responseReader->StartCall();
    call->responseReader->Finish(&call->result, &call->status, (void *)call);
}

void WorkerClient::convertToGrpcSpec(const FileShard &shard, masterworker::partition *partition)
{
    partition->set_shard_id(shard.shard_id);
    for (const auto &segment : shard.segments)
    {
        auto *file = partition->add_file_list();
        file->set_filename(segment.filename);
        file->set_start_offset(segment.offsets.first);
        file->set_end_offset(segment.offsets.second);
    }
}

struct Worker
{
    std::string address;
    WorkerStatus status;
    WorkerType type;
    FileShard currentShard;
    std::shared_ptr<WorkerClient> client;
    std::map<std::string, std::vector<std::string>> outputFiles;
    int currentOutput;
    bool deadHandled = false;
};

class Master
{
public:
    Master(const MapReduceSpec &spec, const std::vector<FileShard> &shards);
    ~Master();

    bool run();

private:
    Worker *findWorkerByName(const std::string &name);
    std::vector<int> findWorkersByStatus(WorkerStatus status);

    void heartbeat();
    void handleDeadWorker(const std::string &workerName);
    void cleanupFiles();

    void asyncMap();
    void asyncReduce();
    std::vector<std::string> assignFilesToReducer(int outputId);

    grpc::CompletionQueue *completionQueue_;
    bool serverState_ = SERVER_ALIVE;

    MapReduceSpec spec_;

    Worker dummyWorker_;
    std::vector<Worker> workers_;
    std::mutex workerMutex_;
    std::condition_variable workerCondition_;

    bool heartbeatInitialized_ = true;
    std::mutex heartbeatMutex_;
    std::condition_variable heartbeatCondition_;

    std::mutex cleanupMutex_;
    std::condition_variable cleanupCondition_;

    int completionCount_;
    bool operationsCompleted_ = false;
    std::mutex operationsMutex_;
    std::condition_variable operationsCondition_;

    int assignedShards_;
    std::vector<FileShard> fileShards_;
    std::vector<FileShard> missingShards_;
    std::vector<std::string> intermediateFiles_;

    int assignedPartitions_;
    std::vector<std::string> outputFiles_;
    std::vector<int> missingOutputs_;
};

Master::Master(const MapReduceSpec &spec, const std::vector<FileShard> &shards)
    : spec_(spec), fileShards_(shards)
{
    completionQueue_ = new grpc::CompletionQueue();
    for (const auto &address : spec_.worker_addresses)
    {
        dummyWorker_.address = address;
        dummyWorker_.status = WorkerStatus::FREE;
        dummyWorker_.type = WorkerType::MAPPER;
        dummyWorker_.client = std::make_shared<WorkerClient>(address, completionQueue_);
        workers_.push_back(dummyWorker_);
    }
}

Master::~Master()
{
    serverState_ = !SERVER_ALIVE;
    completionQueue_->Shutdown();
    cleanupFiles();
}

Worker *Master::findWorkerByName(const std::string &name)
{
    for (auto &worker : workers_)
    {
        if (worker.address == name)
        {
            return &worker;
        }
    }
    std::cerr << "Error Code 501: Worker " << name << " not found." << std::endl;
    return nullptr;
}

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

void Master::heartbeat()
{
    while (serverState_)
    {
        std::map<std::string, HeartbeatPayload> heartbeatMessages;
        auto currentTime = std::chrono::system_clock::now().time_since_epoch().count();

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

        for (auto &worker : workers_)
        {
            auto client = worker.client.get();
            if (worker.status != WorkerStatus::DEAD)
            {
                bool alive = client->receiveHeartbeat();
                if (!alive)
                {
                    std::cerr << "Error Code 403: Worker " << worker.address << " is dead, initiating cleanup." << std::endl;
                    worker.status = WorkerStatus::DEAD;
                    handleDeadWorker(worker.address);
                }
            }
        }

        if (heartbeatInitialized_)
        {
            {
                std::lock_guard<std::mutex> lock(heartbeatMutex_);
                heartbeatInitialized_ = false;
                heartbeatCondition_.notify_one();
            }
        }

        auto endTime = std::chrono::system_clock::now().time_since_epoch().count();
        if (endTime - currentTime < 1000000)
        {
            std::unique_lock<std::mutex> lock(heartbeatMutex_);
            sleep(1);
            heartbeatCondition_.wait_for(lock, std::chrono::milliseconds(5000), []
                                         { return true; });
        }
    }
}

void Master::handleDeadWorker(const std::string &workerName)
{
    std::cerr << "Error Code 404: Handling dead worker: " << workerName << std::endl;
    auto worker = findWorkerByName(workerName);
    if (!worker)
        return;

    std::lock_guard<std::mutex> lock(cleanupMutex_);
    if (worker->type == WorkerType::MAPPER && !operationsCompleted_)
    {
        missingShards_.push_back(worker->currentShard);
        assignedShards_++;
        worker->status = WorkerStatus::DEAD;
        cleanupFiles();
        cleanupCondition_.notify_one();
    }
    else if (worker->type == WorkerType::REDUCER)
    {
        missingOutputs_.push_back(worker->currentOutput);
        assignedPartitions_++;
        worker->status = WorkerStatus::DEAD;
        cleanupFiles();
        cleanupCondition_.notify_one();
    }
    operationsCondition_.notify_all();
}

void Master::cleanupFiles()
{
    if (!findWorkersByStatus(WorkerStatus::DEAD).empty() && serverState_ == SERVER_ALIVE)
    {
        for (int index : findWorkersByStatus(WorkerStatus::DEAD))
        {
            auto &worker = workers_[index];
            if (worker.type == WorkerType::MAPPER && !worker.deadHandled)
            {
                std::string workerPort = worker.address.substr(worker.address.find_first_of(':'));
                for (const auto &entry : fs::directory_iterator(TEMP_DIRECTORY))
                {
                    if (entry.path().string().find(workerPort) != std::string::npos)
                    {
                        fs::remove(entry);
                    }
                }
            }
            else if (!worker.deadHandled)
            {
                for (const auto &outputFile : worker.outputFiles)
                {
                    fs::remove(outputFile.first);
                }
            }
            worker.deadHandled = true;
        }
    }
    if (!serverState_)
    {
        fs::remove_all(TEMP_DIRECTORY);
    }
}

bool Master::run()
{
    std::thread heartbeatThread(&Master::heartbeat, this);
    fs::create_directory(TEMP_DIRECTORY);
    if (fs::is_directory(spec_.output_dir))
    {
        for (const auto &entry : fs::directory_iterator(spec_.output_dir))
        {
            fs::remove(entry.path());
        }
    }

    std::thread mapThread(&Master::asyncMap, this);
    {
        std::unique_lock<std::mutex> lock(heartbeatMutex_);
        heartbeatInitialized_ = true;
        heartbeatCondition_.wait(lock, [this]
                                 { return !heartbeatInitialized_; });
    }

    bool shardsCompleted = false;
    completionCount_ = assignedShards_ = static_cast<int>(fileShards_.size());
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
            std::cout << "Assigning Map Work of shard ID " << shard.shard_id << " to " << workers_[workerIndex].address << std::endl;
            client->scheduleMapJob(spec_, workers_[workerIndex].currentShard);
        }

        {
            std::unique_lock<std::mutex> lock(operationsMutex_);
            operationsCondition_.wait(lock);
            if (assignedShards_ <= 0 && operationsCompleted_)
            {
                shardsCompleted = true;
            }
        }

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
    std::cout << "Map phase completed." << std::endl;

    for (auto &worker : workers_)
    {
        if (worker.status == WorkerStatus::DEAD)
            continue;
        worker.status = WorkerStatus::FREE;
        worker.type = WorkerType::REDUCER;
    }
    operationsCompleted_ = false;

    std::thread reduceThread(&Master::asyncReduce, this);

    bool partitionsCompleted = false;
    completionCount_ = assignedPartitions_ = spec_.num_output_files;
    std::vector<int> outputIndices(assignedPartitions_);
    std::iota(outputIndices.begin(), outputIndices.end(), 0);

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
            std::cout << "Assigning Reduce Work " << outputFile << " to " << workers_[workerIndex].address << std::endl;
            client->scheduleReduceJob(spec_, workers_[workerIndex].outputFiles[outputFile], outputFile);
        }

        {
            std::unique_lock<std::mutex> lock(operationsMutex_);
            operationsCondition_.wait(lock);
            if (assignedPartitions_ <= 0 && operationsCompleted_)
            {
                partitionsCompleted = true;
            }
        }

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

    {
        std::unique_lock<std::mutex> lock(heartbeatMutex_);
        serverState_ = !SERVER_ALIVE;
        heartbeatCondition_.notify_all();
    }
    heartbeatThread.join();
    cleanupFiles();
    return true;
}

void Master::asyncMap()
{
    void *tag;
    bool ok = false;
    while (completionQueue_->Next(&tag, &ok))
    {
        auto call = static_cast<AsyncCall *>(tag);
        if (call->status.ok())
        {
            if (findWorkerByName(call->workerAddress)->status != WorkerStatus::DEAD)
            {
                {
                    std::lock_guard<std::mutex> lock(workerMutex_);
                    for (auto &worker : workers_)
                    {
                        if (worker.address == call->workerAddress)
                        {
                            std::cout << call->workerAddress << " is now free." << std::endl;
                            worker.status = WorkerStatus::FREE;
                            completionCount_--;
                            std::cout << call->workerAddress << " response received. Completion Count: " << completionCount_
                                      << ", Assigned Work: " << assignedShards_ << std::endl;
                            break;
                        }
                    }
                    workerCondition_.notify_one();
                }
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
                    if (completionCount_ == 0)
                    {
                        operationsCompleted_ = true;
                        operationsCondition_.notify_one();
                        break;
                    }
                }
            }
            cleanupCondition_.notify_one();
        }
        delete call;
    }
}

void Master::asyncReduce()
{
    void *tag;
    bool ok = false;
    while (completionQueue_->Next(&tag, &ok))
    {
        auto call = static_cast<AsyncCall *>(tag);
        if (call->status.ok())
        {
            if (findWorkerByName(call->workerAddress)->status != WorkerStatus::DEAD)
            {
                {
                    std::lock_guard<std::mutex> lock(workerMutex_);
                    for (auto &worker : workers_)
                    {
                        if (worker.address == call->workerAddress)
                        {
                            worker.status = WorkerStatus::FREE;
                            completionCount_--;
                            std::cout << call->workerAddress << " response received. Completion Count: " << completionCount_
                                      << ", Assigned Work: " << assignedPartitions_ << std::endl;
                            break;
                        }
                    }
                    workerCondition_.notify_one();
                }
                if (!call->isMapJob)
                {
                    auto reduceCall = dynamic_cast<ReduceCall *>(call);
                    outputFiles_.push_back(reduceCall->result.file_name());
                }
                {
                    std::unique_lock<std::mutex> lock(operationsMutex_);
                    if (completionCount_ == 0)
                    {
                        operationsCompleted_ = true;
                        operationsCondition_.notify_one();
                        break;
                    }
                }
            }
            cleanupCondition_.notify_one();
        }
        delete call;
    }
}

std::vector<std::string> Master::assignFilesToReducer(int outputId)
{
    std::set<std::string> fileSet;
    for (size_t i = 0; i < intermediateFiles_.size(); ++i)
    {
        if (i % spec_.num_output_files == outputId)
        {
            fileSet.insert(intermediateFiles_[i]);
        }
    }
    return std::vector<std::string>(fileSet.begin(), fileSet.end());
}
