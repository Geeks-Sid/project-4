#pragma once

#include <condition_variable>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <numeric>
#include <thread>
#include <unistd.h>
#include <utility>
#include <filesystem>

#include "file_shard.h"
#include "mapreduce_spec.h"
#include "masterworker.grpc.pb.h"

#define ALIVE true
#define TIMEOUT 5

namespace fs = std::filesystem;

enum WorkerStatus
{
    FREE,
    BUSY,
    DEAD
};

enum WorkerType
{
    MAPPER,
    REDUCER
};

struct HeartbeatPayload
{
    std::string id;
    std::int64_t timestamp;
    WorkerStatus status;
};

class AsyncClientCall
{
public:
    bool isMapJob = true;
    grpc::ClientContext context;
    grpc::Status status;
    std::string workerIpAddr;

    virtual ~AsyncClientCall() = default;
};

class MapCall : public AsyncClientCall
{
public:
    masterworker::Map_Response result;
    std::unique_ptr<grpc::ClientAsyncResponseReader<masterworker::Map_Response>> mapResponseReader;
};

class ReduceCall : public AsyncClientCall
{
public:
    masterworker::Reduce_Response result;
    std::unique_ptr<grpc::ClientAsyncResponseReader<masterworker::Reduce_Response>> reduceResponseReader;
};

class HeartbeatCall : public AsyncClientCall
{
public:
    masterworker::Heartbeat_Payload result;
    std::unique_ptr<grpc::ClientAsyncResponseReader<masterworker::Heartbeat_Payload>> heartbeat_payload_reader;
};

/**
 * Worker Client class to communicate with worker class
 */
class WorkerClient
{
public:
    WorkerClient(const std::string &address, grpc::CompletionQueue *queue);

    void send_heartbeat(int64_t current_time);

    bool recv_heartbeat();

    void schedule_reduce_job(MapReduceSpec spec, std::vector<std::string> file_list, std::string output_file_location);

    void schedule_mapper_jobs(MapReduceSpec spec, FileShard shard);
    ~WorkerClient()
    {
        heartbeat_queue->Shutdown();
    }

private:
    std::unique_ptr<masterworker::Map_Reduce::Stub> stub;
    grpc::CompletionQueue *queue;
    std::string worker_address;
    grpc::CompletionQueue *heartbeat_queue;

    void convert_grpc_spec(FileShard *shard, masterworker::partition *partition);
};
/**
 * Constructor for worker client , create communication insecure channel for each worker.
 * @param address
 * @param queue
 */
WorkerClient::WorkerClient(const std::string &address, grpc::CompletionQueue *queue)
    : queue(queue), worker_address(address)
{
    std::cout << "creating channel at " + address << std::endl;
    heartbeat_queue = new grpc::CompletionQueue();
    this->stub = masterworker::Map_Reduce::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
}
/**
 * Send Async Heartbeat Request to client to infer health. It sets deadline for TIMEOUT 5 secs if worker
 * times out in any case
 * @param current_time
 */
void WorkerClient::send_heartbeat(std::int64_t current_time)
{
    //    std::cout << "Info " << std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()) << " " +
    //    this->worker_address << " : Hbeat sent" << std::endl;

    std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::seconds(TIMEOUT);
    auto call = new HeartbeatCall;
    call->worker_ip_addr = this->worker_address;
    call->context.set_deadline(deadline);
    masterworker::Heartbeat_Payload payload;
    payload.set_id(this->worker_address);
    //    payload.set_timestamp(current_time);
    payload.set_status(masterworker::Heartbeat_Payload_type_UNKNOWN);
    call->heartbeat_payload_reader =
        WorkerClient::stub->PrepareAsyncheartbeat(&call->context, payload, WorkerClient::heartbeat_queue);
    call->heartbeat_payload_reader->StartCall();
    call->heartbeat_payload_reader->Finish(&call->result, &call->status, (void *)call);
}
/**
 * Async Heartbeat response reader ,
 * @return false if worker time out or unreachable or any other communication case.
 */
bool WorkerClient::recv_heartbeat()
{
    void *tag;
    bool ok = false;
    GPR_ASSERT(WorkerClient::heartbeat_queue->Next(&tag, &ok));
    auto *call = static_cast<HeartbeatCall *>(tag);
    if (call->status.ok())
    {
        if (call->result.status() == masterworker::Heartbeat_Payload_type_DEAD)
        {
            std::cerr << "Error " << call->worker_ip_addr << " : Dead" << std::endl;
            return false;
        }
        delete call;
        return true;
    }
    auto temp = call->status;
    std::cerr << "Error " << this->worker_address << " : " << call->status.error_message()
              << "details : " << call->status.error_details() << "status code " << call->status.error_code() << "   ok "
              << call->status.ok() << std::endl;
    return false;
}
/**
 * Schedules Reduce Jobs to worker.
 * @param spec mapreduce ini file data structure.
 * @param file_list
 * @param output_file_location
 */
void WorkerClient::schedule_reduce_job(
    MapReduceSpec spec,
    std::vector<std::string> file_list,
    std::string output_file_location)
{
    masterworker::Reduce_Request reduceRequest;
    reduceRequest.set_uuid(spec.user);
    reduceRequest.set_output_file(output_file_location);
    for (const auto &l : file_list)
    {
        auto f = reduceRequest.add_file_list();
        f->append(l);
    }
    auto call = new ReduceCall;
    call->worker_ip_addr = this->worker_address;
    call->reducer_response_reader =
        WorkerClient::stub->PrepareAsyncreduce(&call->context, reduceRequest, WorkerClient::queue);
    call->is_map_job = false;
    call->reducer_response_reader->StartCall();
    call->reducer_response_reader->Finish(&call->result, &call->status, (void *)call);
}
/**
 * Convert FileShard struct to GRPC partition.
 * @param shard
 * @param partition
 */
void WorkerClient::convert_grpc_spec(FileShard *shard, masterworker::partition *partition)
{
    partition->set_shard_id(shard->shard_id);
    for (auto f : shard->split_file_list)
    {
        auto temp = partition->add_file_list();
        temp->set_filename(f.filename);
        temp->set_start_offset(f.offsets.first);
        temp->set_end_offset(f.offsets.second);
    }
}

/**
 * Sechules mapper job with Worker Client, similar to reduce , heartbeat etc
 * @param spec
 * @param shard
 */
void WorkerClient::schedule_mapper_jobs(MapReduceSpec spec, FileShard shard)
{
    masterworker::Map_Request mapRequest;
    mapRequest.set_uuid(spec.user);
    mapRequest.set_partition_count(spec.output_files);
    auto s = mapRequest.add_shard();
    this->convert_grpc_spec(&shard, s);
    auto call = new MapCall;
    call->worker_ip_addr = WorkerClient::worker_address;
    call->map_response_reader = WorkerClient::stub->PrepareAsyncmap(&call->context, mapRequest, WorkerClient::queue);
    call->is_map_job = true;
    call->map_response_reader->StartCall();
    call->map_response_reader->Finish(&call->result, &call->status, (void *)call);
}

struct worker
{
    std::string worker_address;
    WORKER_STATUS workerStatus;
    WORKER_TYPE workerType;
    FileShard current_shard;
    std::shared_ptr<WorkerClient> client;
    std::map<std::string, std::vector<std::string>>
        output_reducer_location_map; // Which intermediate files will be used
    int current_output;
    bool dead_handled = false;
};

/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
        This is probably the biggest task for this project, will test your
   understanding of map reduce */
class Master
{

public:
    /* DON'T change the function signature of this constructor */
    Master(const MapReduceSpec &, const std::vector<FileShard> &);

    /* DON'T change this function's signature */
    bool run();
    ~Master()
    {
        Master::server_state = !ALIVE;
        Master::cq_->Shutdown();
        cleanup_files();
    }

private:
    /* NOW you can add below, data members and member functions as per the need of
     * your implementation*/

    grpc::CompletionQueue *cq_; // Map Reduce Work Assignment
    bool server_state = ALIVE;

    MapReduceSpec mr_spec;

    // Worker Queue
    worker dummy{};
    std::vector<struct worker> workers{};
    std::mutex worker_queue_mutex;
    std::condition_variable condition_worker_queue_mutex;
    worker *find_worker_by_name(std::string t);
    std::vector<int> find_worker_by_status(WORKER_STATUS t);

    // Heartbeat Stuff
    bool init_heartbeat = true;
    std::mutex heartbeat_mutex;
    std::condition_variable condition_heartbeat;
    void heartbeat();
    void handler_dead_worker(std::string worker);

    // Clean up Stuff
    std::mutex cleanup_mutex;
    std::condition_variable condition_cleanup_mutex;
    void cleanup_files();

    // Operational metadata for both map reduce
    int completion_count;
    bool ops_completed = false;
    std::mutex ops_mutex;
    std::condition_variable condition_ops_mutex;

    // Map
    int assigned_shards;
    std::vector<FileShard> file_shards;
    std::vector<FileShard> missing_shards;
    std::vector<std::string> intermidateFiles;
    void async_map();

    // Reduce
    int assigned_partition;
    std::vector<std::string> OutputFiles;
    std::vector<int> missing_output_files;
    void async_reducer();
    std::vector<std::string> assign_files_to_reducer(int output_id);
};
/**
 * Return list of Worker id with requested status,
 * `FREE OR BUSY OR DEAD`
 * @param t
 * @return list of Worker id with requested status
 */
std::vector<int> Master::find_worker_by_status(WORKER_STATUS t)
{
    std::vector<int> temp;
    for (int i = 0; i < Master::workers.size(); i++)
    {
        if (Master::workers[i].workerStatus == t)
        {
            temp.push_back(i);
        }
    }
    return temp;
}
/**
 * Return Worker id with requested Name,
 *
 * @param t
 * @return pointer for Worker with requested name or Null pointer if not found
 */
worker *Master::find_worker_by_name(std::string t)
{
    for (auto &w : Master::workers)
    {
        if (w.worker_address == t)
            return &w;
    }
    std::cerr << "worker " << t << " not found" << std::endl;
    return nullptr;
}

/**
 * Constructor for Master ,
 * Inits worker clients given spec file.
 * This is all the information your master will get from the
 * framework. You can populate your other class data members here if you want
 * @param mr_spec
 * @param file_shards
 */
Master::Master(const MapReduceSpec &mr_spec, const std::vector<FileShard> &file_shards)
    : mr_spec(mr_spec), file_shards(file_shards)
{
    cq_ = new grpc::CompletionQueue();
    for (const auto &i : Master::mr_spec.worker_endpoints)
    {
        dummy.worker_address = i;
        dummy.workerStatus = FREE;
        dummy.workerType = MAPPER;
        dummy.client = std::make_shared<WorkerClient>(i, Master::cq_);
        Master::workers.push_back(dummy);
    }
}

/* CS6210_TASK: Here you go. once this function is called you will complete
 * whole map reduce task and return true if succeeded */
/**
 * Brain of code ;)
 * Handles few things
 * 1. Creates Temp intermediate Dir
 * 2. Create Heartbeat thread for monitoring status of workers
 * 3. Assigns Workers Mapper , checks for dead worker and reassigns the work after cleanup.
 * 4. Creates mapping required for intermediate file to Output files.
 * 5. Assigns work to reducers and handle any dead reducers.
 * 6. Handles cleanup for intermediate files
 * @return true false based of worker update.
 */
bool Master::run()
{
    std::thread check_heartbeat_status(&Master::heartbeat, this);
#if __cplusplus >= 201703L
    fs::create_directory(TEMP_DIR);
    if (fs::is_directory(Master::mr_spec.output_directory))
    {
        for (const auto &fi : fs::directory_iterator(Master::mr_spec.output_directory))
        {
            fs::remove(fi.path());
        }
    }
#else
    auto dir_string = std::string("rm -rf ") + std::string(TEMP_DIR);
    system(dir_string.c_str());
    mkdir(TEMP_DIR, 0755);
#endif

    std::thread map_job(&Master::async_map, this);
    {
        std::unique_lock<std::mutex> lock_heartbeat(heartbeat_mutex);
        Master::init_heartbeat = true;
        condition_heartbeat.wait(lock_heartbeat, [this]
                                 { return !this->init_heartbeat; });
    }
    bool shards_done = false;
    Master::completion_count = Master::assigned_shards = Master::file_shards.size();
    while (!shards_done)
    {
        for (const auto &s : Master::file_shards)
        {
            int i;
            {
                std::unique_lock<std::mutex> shards(Master::cleanup_mutex);
                // wait for cleanup mutex and free worker.
                condition_cleanup_mutex.wait(shards, [this]
                                             { return !Master::find_worker_by_status(FREE).empty(); });
                i = Master::find_worker_by_status(FREE)[0];
                if (Master::workers[i].workerStatus == DEAD)
                {
                    continue;
                }
                Master::workers[i].current_shard = s;
                Master::workers[i].workerStatus = BUSY;
                Master::assigned_shards--;
            }
            auto client = Master::workers[i].client.get();
            //  Adds Mapper Job
            std::cout << "Assigning Map Work of shard id " + std::to_string(s.shard_id) + " to " +
                             Master::workers[i].worker_address
                      << std::endl;

            client->schedule_mapper_jobs(Master::mr_spec, Master::workers[i].current_shard);
        }
        {
            std::unique_lock<std::mutex> work_done(ops_mutex);
            condition_ops_mutex.wait(work_done);
            if (assigned_shards <= 0 && ops_completed)
                shards_done = true;
        }

        {
            std::unique_lock<std::mutex> shards(Master::cleanup_mutex);
            if (Master::assigned_shards > 0 && !Master::missing_shards.empty())
            {
                std::cout << "Re assigning work for " << Master::missing_shards[0].shard_id << std::endl;
                Master::file_shards.clear();
                Master::file_shards.assign(Master::missing_shards.begin(), Master::missing_shards.end());
                Master::missing_shards.clear();
                condition_cleanup_mutex.notify_one();
            }
        }
    }
    map_job.join();
    std::cout << "Map Done." << std::endl;

    for (auto &s : Master::workers)
    {
        if (s.workerStatus == DEAD)
            continue;
        s.workerStatus = FREE;
        s.workerType = REDUCER;
    }
    ops_completed = false;

    std::thread reduce_job(&Master::async_reducer, this);

    bool partition_done = false;
    Master::completion_count = Master::assigned_partition = Master::mr_spec.output_files;
    std::vector<int> output_vector(Master::assigned_partition);
    std::iota(output_vector.begin(), output_vector.end(), 0);
    while (!partition_done && Master::assigned_partition > 0)
    {
        for (auto &i : output_vector)
        {
            int j;
            std::string output_file;
            {
                std::unique_lock<std::mutex> partition(Master::cleanup_mutex);
                condition_cleanup_mutex.wait(
                    partition, [this]
                    { return !Master::find_worker_by_status(FREE).empty(); });
                j = Master::find_worker_by_status(FREE)[0];
                if (Master::workers[j].workerStatus == DEAD)
                {
                    continue;
                }
                Master::workers[j].workerType = REDUCER;
                output_file =
                    Master::mr_spec.output_directory + "/" + std::string("output_file_").append(std::to_string(i));
                Master::workers[j].output_reducer_location_map[output_file] = assign_files_to_reducer(i);
                Master::workers[j].current_output = i;
                Master::workers[j].workerStatus = BUSY;
                Master::assigned_partition--;
            }
            condition_cleanup_mutex.notify_one();
            auto client = Master::workers[j].client.get();
            //  Adds Reducer Job
            std::cout << "Assigning Reduce Work " + output_file + " to " + Master::workers[j].worker_address
                      << std::endl;

            client->schedule_reduce_job(
                Master::mr_spec, Master::workers[j].output_reducer_location_map[output_file], output_file);
        }
        {
            std::unique_lock<std::mutex> work_done(ops_mutex);
            condition_ops_mutex.wait(work_done);
            if (Master::assigned_partition <= 0 && ops_completed)
                partition_done = true;
        }

        {
            std::unique_lock<std::mutex> partition(Master::cleanup_mutex);
            if (Master::assigned_partition > 0 && !Master::missing_output_files.empty())
            {
                std::cout << "Re assigning work for " + Master::mr_spec.output_directory + "/" +
                                 std::string("output_file_")
                          << Master::missing_output_files[0] << std::endl;
                output_vector.clear();
                output_vector.assign(Master::missing_output_files.begin(), Master::missing_output_files.end());
                Master::missing_output_files.clear();
                condition_cleanup_mutex.notify_one();
            }
        }
    }

    reduce_job.join();

    {
        std::unique_lock<std::mutex> heartbeat(Master::heartbeat_mutex);
        Master::server_state = !ALIVE;
        condition_heartbeat.notify_all();
    }
    check_heartbeat_status.join();
    cleanup_files();
    return true;
}
/**
 * Handles heartbeat checks
 * 1. While server is alive, sends heartbeat messages Asyncly and waits for response.
 * 2. If Response timesout or comes as dead calls cleanup. function and re assigns the work.
 */
void Master::heartbeat()
{
    while (Master::server_state)
    {
        std::map<std::string, heartbeat_payload> message_queue;
        auto current_time = std::chrono::system_clock::now().time_since_epoch().count();
        for (const auto &w : Master::workers)
        {
            auto c = w.client.get();
            heartbeat_payload temp_payload{};
            temp_payload.id = w.worker_address;
            //            temp_payload.timestamp = current_time;
            if (w.workerStatus != DEAD)
            {
                c->send_heartbeat(temp_payload.timestamp);
                message_queue[w.worker_address] = temp_payload;
            }
        }

        for (auto &w : this->workers)
        {
            auto c = w.client.get();
            if (!c)
                continue;
            if (w.workerStatus != DEAD)
            {
                bool status = c->recv_heartbeat();
                if (!status)
                {
                    std::cerr << "Error " << w.worker_address << " : Dead , cleaning up" << std::endl;
                    w.workerStatus = DEAD;
                    Master::handler_dead_worker(message_queue[w.worker_address].id);
                }
            }
        }

        if (init_heartbeat)
        {
            {
                std::unique_lock<std::mutex> heartbeat_lock(Master::heartbeat_mutex);
                init_heartbeat = false;
                condition_heartbeat.notify_one();
            }
        }

        auto end_time = std::chrono::system_clock::now().time_since_epoch().count();

        if (end_time - current_time < 1000 * 1000)
        {
            std::unique_lock<std::mutex> heartbeat_lock(Master::heartbeat_mutex);
            sleep(1);
            condition_heartbeat.wait_for(heartbeat_lock, std::chrono::milliseconds(5 * 1000), [this]
                                         { return true; });
        }
    }
}

/**
 * Given worker ,Cleans up half ass work for the worker and moves the file shards or ouput files back to pool for
 * assigning to other workers
 * @param worker address/id
 */
void Master::handler_dead_worker(std::string worker)
{
    std::cerr << "HANDLING DEAD WORKER......" + worker << std::endl;
    auto w = Master::find_worker_by_name(worker);
    // Handle Mapper
    if (w->workerType == MAPPER and !ops_completed)
    {
        std::lock_guard<std::mutex> lockGuard(Master::cleanup_mutex);
        auto c = w->client.get();
        //        unintialized....
        if (!c)
        {
            condition_ops_mutex.notify_all();
            return;
        }
        Master::missing_shards.push_back(w->current_shard);
        Master::assigned_shards++;
        w->workerStatus = DEAD;
        Master::cleanup_files();
        condition_cleanup_mutex.notify_one();
    }
    else
    {
        std::lock_guard<std::mutex> lockGuard(Master::cleanup_mutex);
        auto c = w->client.get();
        if (!c)
        {
            condition_ops_mutex.notify_all();
            return;
        }
        Master::missing_output_files.push_back(w->current_output);
        Master::assigned_partition++;
        w->workerStatus = DEAD;
        Master::cleanup_files();
        condition_cleanup_mutex.notify_one();
    }
    condition_ops_mutex.notify_all();
}
/**
 * Cleans up file if Server is alive and if there is any Dead worker in the list.
 * and also handles intermediate files cleanup during master's exit.
 */
void Master::cleanup_files()
{
    if (!Master::find_worker_by_status(DEAD).empty() && Master::server_state == ALIVE)
    {
        for (auto i : Master::find_worker_by_status(DEAD))
        {
            if (Master::workers[i].workerType == MAPPER && !Master::workers[i].dead_handled)
            {
                // std::string(TEMP_DIR) + "/" + std::to_string(i) + "_" + MapperHandler::worker_address + ".txt"))

                auto worker_port =
                    Master::workers[i].worker_address.substr(Master::workers[i].worker_address.find_first_of(':'));
#if __cplusplus >= 201703L
                for (auto f : fs::directory_iterator(TEMP_DIR))
                {
                    if (f.path().string().find(worker_port) != std::string::npos)
                        fs::remove(f);
                }
#else
                auto dir_string = std::string("rm -rf ") + TEMP_DIR + "/*_" + worker_port + ".txt";
                system(dir_string.c_str());
#endif
            }
            else
            {
                for (const auto &worker_location : Master::workers[i].output_reducer_location_map)
                {
                    if (!Master::workers[i].dead_handled)
#if __cplusplus >= 201703L

                        fs::remove(worker_location.first);
#else
                        remove(worker_location.first.c_str());
#endif
                }
            }
            Master::workers[i].dead_handled = true;
        }
    }
    if (!Master::server_state)
    {
#if __cplusplus >= 201703L
        fs::remove_all(TEMP_DIR);
#else
        auto dir_string = std::string("rm -rf ") + TEMP_DIR;
        system(dir_string.c_str());
#endif
    }
}
/**
 * Handles Async Responses for Mapper Requests and frees worker for other work and puts data back in in the list
 */
void Master::async_map()
{
    void *tag;
    bool ok = false;
    while (Master::cq_->Next(&tag, &ok))
    {
        auto call = static_cast<AsyncClientCall *>(tag);
        if (call->status.ok())
        {
            if (Master::find_worker_by_name(call->worker_ip_addr)->workerStatus != DEAD)
            {
                {
                    std::lock_guard<std::mutex> worker_queue(this->worker_queue_mutex);
                    for (auto &worker : Master::workers)
                    {
                        if (worker.worker_address == call->worker_ip_addr)
                        {
                            std::cout << call->worker_ip_addr + " back to free." << std::endl;

                            worker.workerStatus = FREE;
                            Master::completion_count--;
                            std::cout << call->worker_ip_addr + " response recieved. Completion Count : " +
                                             std::to_string(Master::completion_count) +
                                             " Assigned Work: " + std::to_string(Master::assigned_shards)
                                      << std::endl;
                            break;
                        }
                    }
                    condition_worker_queue_mutex.notify_one();
                }
                if (call->is_map_job)
                {
                    auto mcall = dynamic_cast<MapCall *>(call);
                    for (const auto &m : mcall->result.file_list())
                    {
                        Master::intermidateFiles.push_back(m);
                    }
                }
                {
                    std::unique_lock<std::mutex> work_done(ops_mutex);
                    if (Master::completion_count == 0)
                    {
                        ops_completed = true;
                        condition_ops_mutex.notify_one();
                        break;
                    }
                }
            }
            condition_cleanup_mutex.notify_one();
        }
        delete call;
    }
}
/**
 * Simillar  Async_map ,  handles Reducer Responses.
 */
void Master::async_reducer()
{
    void *tag;
    bool ok = false;
    while (Master::cq_->Next(&tag, &ok))
    {
        auto call = static_cast<AsyncClientCall *>(tag);
        if (call->status.ok())
        {
            if (Master::find_worker_by_name(call->worker_ip_addr)->workerStatus != DEAD)
            {
                {
                    std::lock_guard<std::mutex> worker_queue(this->worker_queue_mutex);
                    for (auto &worker : Master::workers)
                    {
                        if (worker.worker_address == call->worker_ip_addr)
                        {
                            worker.workerStatus = FREE;
                            Master::completion_count--;
                            std::cout << call->worker_ip_addr + " response received. Completion Count : " +
                                             std::to_string(Master::completion_count) +
                                             " Assigned Work: " + std::to_string(Master::assigned_partition)
                                      << std::endl;
                            break;
                        }
                    }
                    condition_worker_queue_mutex.notify_one();
                }
                if (!call->is_map_job)
                {
                    auto mcall = dynamic_cast<ReduceCall *>(call);
                    Master::OutputFiles.push_back(mcall->result.file_name());
                }
                {
                    std::unique_lock<std::mutex> work_done(ops_mutex);
                    if (Master::completion_count == 0)
                    {
                        ops_completed = true;
                        condition_ops_mutex.notify_one();
                        break;
                    }
                }
            }
            condition_cleanup_mutex.notify_one();
        }
        delete call;
    }
}
/**
 * Assigns list of intermidiate files to output files for reducing
 * @param output_id
 * @return list of intermediate files for given output id /file
 */
std::vector<std::string> Master::assign_files_to_reducer(int output_id)
{
    std::set<std::string> file_list;
    for (int i = 0; i < Master::intermidateFiles.size(); i++)
    {
        auto f = Master::intermidateFiles[i];
        if (i % Master::mr_spec.output_files == output_id)
        {
            file_list.insert(f);
        }
    }
    std::vector<std::string> convert;
    convert.assign(file_list.begin(), file_list.end());
    return convert;
}