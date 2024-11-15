#pragma once

#include <thread>
#include <chrono>
#include <grpc++/grpc++.h>
#include <mr_task_factory.h>
#include <unordered_set>

#include "file_shard.h"
#include "mapreduce_spec.h"
#include "masterworker.grpc.pb.h"

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;

using masterworker::AssignTask;
using masterworker::MapRequest;
using masterworker::PingRequest;
using masterworker::ReduceRequest;
using masterworker::ShardPiece;
using masterworker::TaskReply;

using std::cout;
using std::endl;
using std::string;
using std::unique_ptr;
using std::vector;

enum work_status
{
    MAP = 0,
    REDUCE,
    IDLE,
    DONE,
    ACTIVE
};

struct WorkerInfo
{
    FileShard file_shard;    // File shard currently assigned to the worker
    work_status status;      // Worker's current status
    string ip_address;       // Worker's IP address
    string job_id;           // ID of the current job
    string output_file_name; // Name of the output file
    int current_work_time;   // Time the worker has been working on the current task
};

struct AsyncWorkerCall
{
    ClientContext context;
    unique_ptr<ClientAsyncResponseReader<TaskReply>> response_reader;
    WorkerInfo *cur_worker;
    Status rpc_status;
    TaskReply reply;
};

/* Handle all the bookkeeping that Master is supposed to do */
class Master
{
public:
    /* DON'T change the function signature of this constructor */
    Master(const MapReduceSpec &, const vector<FileShard> &);

    /* DON'T change this function's signature */
    bool run();

private:
    MapReduceSpec m_spec;
    CompletionQueue master_cq;
    vector<FileShard> shards;
    vector<WorkerInfo> workers;
    vector<string> output_files;
    std::unordered_set<string> jobs;    // Track all completed jobs
    vector<string> map_output_files;    // Mapper output files list
    vector<string> reduce_output_files; // Reducer output files list
    const int max_work_time = 150;      // Max work time before reassigning task

    void assignMapTask(WorkerInfo &worker);
    void assignReduceTask(WorkerInfo &worker, int section);
    work_status pingWorkerProcess(const WorkerInfo &worker);
    TaskReply getSingleResponse(const string &task_type);
    bool isNewJob(const string &job_id);
    bool areAllWorkersDone();
};

/* Initialize the master with the map-reduce specification and file shards */
Master::Master(const MapReduceSpec &mr_spec, const vector<FileShard> &file_shards)
    : m_spec(mr_spec),
      shards(file_shards)
{

    for (const auto &worker_ip : mr_spec.worker_ipaddr_ports)
    {
        WorkerInfo worker;
        worker.ip_address = worker_ip;
        worker.status = IDLE;
        worker.current_work_time = 0;
        workers.push_back(worker);
    }

    for (size_t i = 0; i < m_spec.num_output_files; ++i)
    {
        string file_name = m_spec.output_dir + "/output_" + std::to_string(i);
        output_files.push_back(file_name);
    }
}

/* Run the map-reduce process */
bool Master::run()
{
    cout << "[Master] Starting map phase." << endl;

    bool map_complete_flag = false;
    int num_shards_completed = 0;
    const int num_map_tasks_to_do = shards.size();
    const int kRunningTime = 200;

    // Loop until all mapping tasks are completed
    while (!map_complete_flag)
    {
        // Ping workers and update their status
        for (auto &worker : workers)
        {
            work_status state = pingWorkerProcess(worker);

            if (state == ACTIVE)
            {
                worker.current_work_time++;

                // Reassign file_shard if worker exceeds max_work_time
                if (worker.current_work_time > max_work_time)
                {
                    shards.push_back(worker.file_shard);
                    worker.status = IDLE;
                    worker.current_work_time = 0;
                    cout << "[Master] Worker " << worker.ip_address << " timed out. Reassigning shard." << endl;
                }
            }
            else
            {
                if (worker.status == MAP)
                {
                    shards.push_back(worker.file_shard);
                    cout << "[Master] Worker " << worker.ip_address << " failed during MAP. Reassigning shard." << endl;
                }
                worker.status = DONE;
            }
        }

        if (areAllWorkersDone())
        {
            cout << "[Master] All worker processes have already been done!" << endl;
            return false;
        }

        // Assign shards to idle workers
        for (auto &worker : workers)
        {
            if (worker.status == IDLE && !shards.empty())
            {
                FileShard assign_shard = shards.back();
                shards.pop_back();
                worker.file_shard = assign_shard;
                worker.status = MAP;
                assignMapTask(worker);
                cout << "[Master] Assigned shard " << worker.file_shard.shard_id << " to worker " << worker.ip_address << endl;
            }
        }

        TaskReply reply = getSingleResponse("MAP");
        if (reply.task_type() == "MAP")
        { // If successful
            if (isNewJob(reply.job_id()))
            {
                map_output_files.push_back(reply.out_file());
                num_shards_completed++;
                cout << "[Master] Received MAP result from job " << reply.job_id() << endl;
            }
        }

        // If all shards have been assigned and completed
        if (num_shards_completed == num_map_tasks_to_do)
        {
            map_complete_flag = true;
            cout << "[Master] Map phase complete!" << endl;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(kRunningTime));
    }

    jobs.clear();

    // Start reduce phase
    cout << "[Master] Starting reduce phase." << endl;

    bool reduce_complete_flag = false;
    int num_reduces_completed = 0;
    const int num_reduce_tasks_to_do = output_files.size();

    // Loop until all reducing tasks are completed
    while (!reduce_complete_flag)
    {
        // Ping workers and update their status
        for (auto &worker : workers)
        {
            work_status state = pingWorkerProcess(worker);

            if (state == ACTIVE)
            {
                worker.current_work_time++;
                if (worker.current_work_time > max_work_time)
                {
                    output_files.push_back(worker.output_file_name);
                    worker.status = IDLE;
                    worker.current_work_time = 0;
                    cout << "[Master] Worker " << worker.ip_address << " timed out. Reassigning reduce task." << endl;
                }
            }
            else
            {
                if (worker.status == REDUCE)
                {
                    output_files.push_back(worker.output_file_name);
                    cout << "[Master] Worker " << worker.ip_address << " failed during REDUCE. Reassigning reduce task." << endl;
                }
                worker.status = DONE;
            }
        }

        if (areAllWorkersDone())
        {
            cout << "[Master] All worker processes have already been done!" << endl;
            return false;
        }

        // Assign reduce tasks to idle workers
        for (auto &worker : workers)
        {
            if (worker.status == IDLE && !output_files.empty())
            {
                string assign_file = output_files.back();
                output_files.pop_back();
                worker.output_file_name = assign_file;
                worker.status = REDUCE;
                assignReduceTask(worker, num_reduces_completed);
                cout << "[Master] Assigned reduce task for file " << assign_file << " to worker " << worker.ip_address << endl;
            }
        }

        TaskReply reply = getSingleResponse("REDUCE");
        if (reply.task_type() == "REDUCE")
        { // If successful
            if (isNewJob(reply.job_id()))
            {
                reduce_output_files.push_back(reply.out_file());
                num_reduces_completed++;
                cout << "[Master] Received REDUCE result from job " << reply.job_id() << endl;
            }
        }

        // If all reduce tasks have been assigned and processed
        if (num_reduces_completed == num_reduce_tasks_to_do)
        {
            reduce_complete_flag = true;
            cout << "[Master] Reduce phase complete!" << endl;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(kRunningTime));
    }

    return true;
}

/* Ping a worker to check if it's active */
work_status Master::pingWorkerProcess(const WorkerInfo &worker)
{
    const string &worker_ip_addr = worker.ip_address;
    work_status res = ACTIVE;
    PingRequest request;
    auto stub = AssignTask::NewStub(grpc::CreateChannel(worker_ip_addr, grpc::InsecureChannelCredentials()));
    ClientContext context;
    TaskReply reply;
    Status status;

    status = stub->Ping(&context, request, &reply);

    if (!status.ok())
    {
        res = DONE;
        cout << "[Master] Notice! Worker " << worker_ip_addr << " is not available currently." << endl;
    }

    return res;
}

/* Assign a map task to a worker */
void Master::assignMapTask(WorkerInfo &worker)
{
    MapRequest request;
    auto stub = AssignTask::NewStub(grpc::CreateChannel(worker.ip_address, grpc::InsecureChannelCredentials()));
    request.set_user_id(m_spec.user_id);
    FileShard &file_shard = worker.file_shard;

    for (const auto &kv_pair : file_shard.pieces)
    {
        ShardPiece *piece = request.add_shard();
        piece->set_file_name(kv_pair->filename);
        piece->set_start_index(kv_pair->start_offset);
        piece->set_end_index(kv_pair->end_offset);
    }

    string jobID = "map_" + std::to_string(worker.file_shard.shard_id);
    request.set_job_id(jobID);
    request.set_num_reducers(m_spec.num_output_files);
    worker.job_id = jobID;

    AsyncWorkerCall *call = new AsyncWorkerCall;
    call->cur_worker = &worker;
    call->response_reader = stub->AsyncMap(&call->context, request, &master_cq);
    call->response_reader->Finish(&call->reply, &call->rpc_status, (void *)call);
}

/* Assign a reduce task to a worker */
void Master::assignReduceTask(WorkerInfo &worker, int section)
{
    ReduceRequest request;
    auto stub = AssignTask::NewStub(grpc::CreateChannel(worker.ip_address, grpc::InsecureChannelCredentials()));
    request.set_user_id(m_spec.user_id);
    request.set_output_file(worker.output_file_name);
    request.set_job_id(worker.output_file_name);
    request.set_section(std::to_string(section));

    for (const auto &input_file : map_output_files)
    {
        request.add_input_files(input_file);
    }

    worker.job_id = worker.output_file_name;

    AsyncWorkerCall *call = new AsyncWorkerCall;
    call->cur_worker = &worker;
    call->response_reader = stub->AsyncReduce(&call->context, request, &master_cq);
    call->response_reader->Finish(&call->reply, &call->rpc_status, (void *)call);
}

/* Check if all workers have completed their tasks */
bool Master::areAllWorkersDone()
{
    for (const auto &worker : workers)
    {
        if (worker.status != DONE)
        {
            return false;
        }
    }
    return true;
}

/* Check if the job is new */
bool Master::isNewJob(const string &job_id)
{
    if (jobs.find(job_id) != jobs.end())
    {
        return false;
    }
    jobs.insert(job_id);
    return true;
}

/* Get a single response from a worker */
TaskReply Master::getSingleResponse(const string &task_type)
{
    TaskReply reply;
    reply.set_task_type("FAIL");
    void *got_tag;
    bool ok = false;
    std::chrono::system_clock::time_point due_timestamp = std::chrono::system_clock::now() + std::chrono::milliseconds(600);
    grpc::CompletionQueue::NextStatus cq_status = master_cq.AsyncNext(&got_tag, &ok, due_timestamp);
    if (cq_status == grpc::CompletionQueue::TIMEOUT)
    {
        return reply;
    }
    AsyncWorkerCall *call = static_cast<AsyncWorkerCall *>(got_tag);
    WorkerInfo *worker = call->cur_worker;
    GPR_ASSERT(ok);

    if (call->rpc_status.ok())
    {
        worker->status = IDLE;
        worker->current_work_time = 0;
        reply = call->reply;
        cout << "[Master] Task " << worker->job_id << " completed successfully by worker " << worker->ip_address << endl;
    }
    else
    {
        cout << "[Master] Error occurred in " << task_type << " with worker " << worker->ip_address << endl;
        cout << "[Master] Error code: " << call->rpc_status.error_code() << endl;
        cout << "[Master] Error message: " << call->rpc_status.error_message() << endl;
        worker->status = DONE;
    }
    delete call;
    return reply;
}
