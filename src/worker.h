#pragma once

#include <deque>
#include <fstream>
#include <memory>
#include <string>
#include <iostream>

#include <grpcpp/grpcpp.h>

#include "mr_task_factory.h"
#include "mr_tasks.h"
#include "masterworker.grpc.pb.h"

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;

using masterworker::AssignTask;
using masterworker::ShardPiece;
using masterworker::PingRequest;
using masterworker::MapRequest;
using masterworker::ReduceRequest;
using masterworker::TaskReply;

using std::cout;
using std::cerr;
using std::endl;
using std::string;
using std::ifstream;
using std::unique_ptr;
using std::shared_ptr;

// External functions to get mapper and reducer from task factory
extern shared_ptr<BaseMapper> get_mapper_from_task_factory(const string& user_id);
extern shared_ptr<BaseReducer> get_reducer_from_task_factory(const string& user_id);

/* CS6210_TASK: Handle all the tasks a Worker is supposed to do.
   This is a big task for this project, will test your understanding of map reduce */
class Worker {
public:
    // Constructor and Destructor
    Worker(const string& ip_addr_port);
    ~Worker();

    // Run the worker
    bool run();

    // Worker status
    enum WorkerStatus { IDLE, MAPPING, REDUCING };

    WorkerStatus get_status() const {
        return work_status_;
    }

    void set_status(WorkerStatus status) {
        work_status_ = status;
    }

private:
    // Private member variables
    WorkerStatus work_status_;

    enum JobType { PING = 1, MAP = 2, REDUCE = 3 };

    AssignTask::AsyncService task_service_;
    unique_ptr<ServerCompletionQueue> task_cq_;
    unique_ptr<Server> task_server_;
    string port_;

    // Class to handle incoming RPCs
    class CallData {
    public:
        CallData(AssignTask::AsyncService* service, ServerCompletionQueue* cq, JobType job_type, const string& worker_id)
            : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE), job_type_(job_type), worker_id_(worker_id) {
            Proceed();
        }

        void Proceed() {
            if (status_ == CREATE) {
                status_ = PROCESS;
                switch (job_type_) {
                    case PING:
                        service_->RequestPing(&ctx_, &ping_request_, &responder_, cq_, cq_, this);
                        break;
                    case MAP:
                        service_->RequestMap(&ctx_, &map_request_, &responder_, cq_, cq_, this);
                        break;
                    case REDUCE:
                        service_->RequestReduce(&ctx_, &reduce_request_, &responder_, cq_, cq_, this);
                        break;
                    default:
                        cerr << "Unknown job type in Proceed CREATE state" << endl;
                        break;
                }
            } else if (status_ == PROCESS) {
                // Spawn a new CallData instance to serve new clients while we process the current request.
                new CallData(service_, cq_, job_type_, worker_id_);

                // Process the request based on job type
                switch (job_type_) {
                    case PING:
                        HandlePing();
                        break;
                    case MAP:
                        HandleMap();
                        break;
                    case REDUCE:
                        HandleReduce();
                        break;
                    default:
                        cerr << "Unknown job type in Proceed PROCESS state" << endl;
                        break;
                }
                status_ = FINISH;
                responder_.Finish(task_reply_, Status::OK, this);
            } else {
                GPR_ASSERT(status_ == FINISH);
                delete this;
            }
        }

    private:
        // Handle Ping request
        void HandlePing() {
            cout << "Worker [" << worker_id_ << "] received Ping request" << endl;
            task_reply_.set_task_type("PING");
        }

        // Handle Map request
        void HandleMap() {
            cout << "Worker [" << worker_id_ << "] received Map request" << endl;

            // Get mapper from task factory
            auto mapper = get_mapper_from_task_factory(map_request_.user_id());
            if (!mapper) {
                cerr << "Failed to get mapper for user_id: " << map_request_.user_id() << endl;
                return;
            }

            // Process each shard piece
            const auto& shard_pieces = map_request_.shard();
            for (const auto& shard_piece : shard_pieces) {
                ifstream input_file(shard_piece.file_name());
                if (!input_file.is_open()) {
                    cerr << "Failed to open file: " << shard_piece.file_name() << endl;
                    continue;
                }

                input_file.seekg(shard_piece.start_index());
                if (input_file.fail()) {
                    cerr << "Failed to seek to position " << shard_piece.start_index() << " in file " << shard_piece.file_name() << endl;
                    input_file.close();
                    continue;
                }

                string line;
                while (input_file.good() && input_file.tellg() < shard_piece.end_index()) {
                    getline(input_file, line);
                    if (!line.empty()) {
                        mapper->map(line);
                    }
                }
                input_file.close();
            }

            // Write the mapper's intermediate data to disk
            string output_filename = worker_id_ + '_' + map_request_.job_id();
            mapper->impl_->write_data(output_filename, map_request_.num_reducers());

            // Prepare the reply
            task_reply_.set_task_type("MAP");
            task_reply_.set_job_id(map_request_.job_id());
            task_reply_.set_out_file(output_filename);
            cout << "Worker [" << worker_id_ << "] completed Map task: " << output_filename << endl;
        }

        // Handle Reduce request
        void HandleReduce() {
            cout << "Worker [" << worker_id_ << "] received Reduce request" << endl;

            // Get reducer from task factory
            auto reducer = get_reducer_from_task_factory(reduce_request_.user_id());
            if (!reducer) {
                cerr << "Failed to get reducer for user_id: " << reduce_request_.user_id() << endl;
                return;
            }

            // Set the final output file
            reducer->impl_->final_file = reduce_request_.output_file();

            // Collect intermediate files
            for (const auto& input_file : reduce_request_.input_files()) {
                string combined_name = input_file + "_R" + reduce_request_.section();
                reducer->impl_->interm_files.push_back(combined_name);
            }

            // Group keys from intermediate files
            reducer->impl_->group_keys();

            // Reduce the grouped keys
            for (const auto& key_value_pair : reducer->impl_->pairs) {
                reducer->reduce(key_value_pair.first, key_value_pair.second);
            }

            // Prepare the reply
            task_reply_.set_task_type("REDUCE");
            task_reply_.set_job_id(reduce_request_.job_id());
            task_reply_.set_out_file(reduce_request_.output_file());
            cout << "Worker [" << worker_id_ << "] completed Reduce task: " << reduce_request_.output_file() << endl;
        }

        // Private member variables
        AssignTask::AsyncService* service_;
        ServerCompletionQueue* cq_;
        ServerContext ctx_;

        // The means to get back to the client.
        ServerAsyncResponseWriter<TaskReply> responder_;

        // Request and reply messages
        PingRequest ping_request_;
        MapRequest map_request_;
        ReduceRequest reduce_request_;
        TaskReply task_reply_;

        // Job and worker information
        JobType job_type_;
        string worker_id_;

        enum CallStatus { CREATE, PROCESS, FINISH };
        CallStatus status_;
    };
};

// Destructor
Worker::~Worker() {
    if (task_server_) {
        task_server_->Shutdown();
    }
    if (task_cq_) {
        task_cq_->Shutdown();
    }
    cout << "Worker at port " << port_ << " shut down." << endl;
}

// Constructor
Worker::Worker(const string& ip_addr_port) : work_status_(IDLE) {
    cout << "Starting Worker at " << ip_addr_port << endl;

    // Extract port from ip_addr_port
    auto pos = ip_addr_port.find(':');
    if (pos != string::npos && pos + 1 < ip_addr_port.size()) {
        port_ = ip_addr_port.substr(pos + 1);
    } else {
        cerr << "Invalid ip_addr_port format: " << ip_addr_port << endl;
        port_ = "";
    }

    // Build and start the server
    ServerBuilder builder;
    builder.AddListeningPort(ip_addr_port, grpc::InsecureServerCredentials());
    builder.RegisterService(&task_service_);
    task_cq_ = builder.AddCompletionQueue();
    task_server_ = builder.BuildAndStart();

    if (task_server_) {
        cout << "Worker listening on " << ip_addr_port << endl;
    } else {
        cerr << "Failed to start server at " << ip_addr_port << endl;
    }
}

// Run method
bool Worker::run() {
    // Start CallData instances for each job type
    new CallData(&task_service_, task_cq_.get(), PING, port_);
    new CallData(&task_service_, task_cq_.get(), MAP, port_);
    new CallData(&task_service_, task_cq_.get(), REDUCE, port_);

    void* tag;
    bool ok;

    // Main loop to handle incoming requests
    while (task_cq_->Next(&tag, &ok)) {
        if (!ok) {
            // Server is shutting down
            break;
        }
        static_cast<CallData*>(tag)->Proceed();
    }

    cout << "Worker run loop exiting." << endl;
    return true;
}
