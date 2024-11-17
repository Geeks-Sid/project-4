#pragma once

#include <grpcpp/grpcpp.h>
#include <mr_task_factory.h>
#include <thread>
#include <utility>

#include "file_shard.h"
#include "masterworker.grpc.pb.h"
#include "mr_tasks.h"
#if __cplusplus >= 201703L

#if __GNUC__ > 7 || __APPLE_CC__ > 7
#include <filesystem>
#elif __GNUC__ == 7 || __APPLE_CC__ == 7
#include <experimental/filesystem>
#endif
#endif
extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string &user_id);

extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string &user_id);
/**
 * Base Class for Three task , map , reduce and heartbeat
 */
class BaseHandler
{
public:
    BaseHandler(
        masterworker::Map_Reduce::AsyncService *service,
        grpc::ServerCompletionQueue *queue,
        std::string worker_address)
        : service(service), s_queue(queue), worker_address(std::move(worker_address)), status_(CREATE)
    {
        Proceed();
    }

    virtual void Proceed()
    {
    }

    ~BaseHandler() = default;

protected:
    masterworker::Map_Reduce::AsyncService *service;
    grpc::ServerCompletionQueue *s_queue;
    std::string worker_address;
    // State engine used from GRPC default example for Async Server.
    grpc::ServerContext ctx_;
    enum CallStatus
    {
        CREATE,
        PROCESS,
        FINISH
    };
    CallStatus status_;
};
/**
 * Mapper Class
 */
class MapperHandler final : BaseHandler
{
public:
    /**
     * Constructor for Mapper Class which inits class
     * @param service
     * @param pQueue
     * @param basicString
     */
    MapperHandler(
        masterworker::Map_Reduce::AsyncService *service,
        grpc::ServerCompletionQueue *pQueue,
        std::string basicString)
        : BaseHandler(service, pQueue, basicString), m_writer(&ctx_)
    {
        Proceed();
    }

    void Proceed()
    {
        if (status_ == CREATE)
        {
            status_ = PROCESS;
            service->Requestmap(&ctx_, &mapRequest, &m_writer, s_queue, s_queue, this);
        }
        else if (status_ == PROCESS)
        {
            new MapperHandler(service, s_queue, worker_address);
            mapResponse = handle_mapper_job(mapRequest);
            status_ = FINISH;
            m_writer.Finish(mapResponse, grpc::Status::OK, this);
        }
        else
        {
            GPR_ASSERT(status_ == FINISH);
            delete this;
        }
    }

private:
    masterworker::Map_Request mapRequest;
    masterworker::Map_Response mapResponse;
    grpc::ServerAsyncResponseWriter<masterworker::Map_Response> m_writer;

    masterworker::Map_Response handle_mapper_job(masterworker::Map_Request request);

    BaseMapperInternal *get_basemapper_internal(BaseMapper *mapper);
    FileShard convert_grpc_spec(masterworker::partition partition);
};
/**
 * Reducer Class
 */
class ReducerHandler final : BaseHandler
{
public:
    /**
     * Constructor for Reducer
     * @param service
     * @param pQueue
     * @param basicString
     */
    ReducerHandler(
        masterworker::Map_Reduce::AsyncService *service,
        grpc::ServerCompletionQueue *pQueue,
        std::string basicString)
        : BaseHandler(service, pQueue, basicString), r_writer(&ctx_)
    {
        ReducerHandler::Proceed();
    }

    void Proceed()
    {
        if (status_ == CREATE)
        {
            status_ = PROCESS;
            service->Requestreduce(&ctx_, &ReduceRequest, &r_writer, s_queue, s_queue, this);
        }
        else if (status_ == PROCESS)
        {
            new ReducerHandler(service, s_queue, worker_address);
            ReduceResponse = handle_reducer_job(ReduceRequest);
            status_ = FINISH;
            r_writer.Finish(ReduceResponse, grpc::Status::OK, this);
        }
        else
        {
            GPR_ASSERT(status_ == FINISH);
            delete this;
        }
    }

private:
    masterworker::Reduce_Request ReduceRequest;
    masterworker::Reduce_Response ReduceResponse;
    grpc::ServerAsyncResponseWriter<masterworker::Reduce_Response> r_writer;

    masterworker::Reduce_Response handle_reducer_job(masterworker::Reduce_Request request);

    BaseReducerInternal *get_basereducer_internal(BaseReducer *reducer);
};
/**
 * Heartbeat class
 */
class HeartbeatHandler final : BaseHandler
{
public:
    /**
     * Constructor for Heartbeat class
     * @param service
     * @param pQueue
     * @param basicString
     */
    HeartbeatHandler(
        masterworker::Map_Reduce::AsyncService *service,
        grpc::ServerCompletionQueue *pQueue,
        std::string basicString)
        : BaseHandler(service, pQueue, basicString), h_writer(&ctx_)
    {
        HeartbeatHandler::Proceed();
    }

    void Proceed()
    {
        if (status_ == CREATE)
        {
            status_ = PROCESS;
            service->Requestheartbeat(&ctx_, &request, &h_writer, s_queue, s_queue, this);
        }
        else if (status_ == PROCESS)
        {
            new HeartbeatHandler(service, s_queue, worker_address);
            response = handle_heartbeat_job(request);
            status_ = FINISH;
            h_writer.Finish(response, grpc::Status::OK, this);
        }
        else
        {
            GPR_ASSERT(status_ == FINISH);
            delete this;
        }
    }

private:
    masterworker::Heartbeat_Payload request, response;
    grpc::ServerAsyncResponseWriter<masterworker::Heartbeat_Payload> h_writer;

    masterworker::Heartbeat_Payload handle_heartbeat_job(masterworker::Heartbeat_Payload request);
};

/**
 * CS6210_TASK: Handle all the task a Worker is supposed to do.
 * This is a big task for this project, will test your understanding of mapreduce
 * */
class Worker
{

public:
    /* DON'T change the function signature of this constructor */
    Worker(std::string ip_addr_port);

    /* DON'T change this function's signature */
    bool run();

    ~Worker()
    {
        Worker::clean_exit = true;
        this->server->Shutdown();
    }

    static BaseReducerInternal *get_basereducer_internal(BaseReducer *reducer)
    {
        return reducer->impl_;
    }

    static BaseMapperInternal *get_basemapper_internal(BaseMapper *mapper)
    {
        return mapper->impl_;
    }

private:
    /* NOW you can add below, data members and member functions as per the need of
     * your implementation*/
    grpc::ServerBuilder builder;
    std::unique_ptr<grpc::ServerCompletionQueue> work_queue;
    std::unique_ptr<grpc::ServerCompletionQueue> heartbeat_queue;
    masterworker::Map_Reduce::AsyncService mapreduce_service;
    std::unique_ptr<grpc::Server> server;
    std::string worker_uuid;

    void heartbeat_handler();

    bool clean_exit = false;
};

/**
 * ip_addr_port is the only information you get when started.
 * You can populate your other class data members here if you want
 * @param ip_addr_port
 */
inline Worker::Worker(std::string ip_addr_port)
{
    std::cout << "listening on " << ip_addr_port << std::endl;
    Worker::builder.AddListeningPort(ip_addr_port, grpc::InsecureServerCredentials());
    Worker::builder.RegisterService(&this->mapreduce_service);
    Worker::work_queue = Worker::builder.AddCompletionQueue();
    Worker::heartbeat_queue = Worker::builder.AddCompletionQueue();
    Worker::worker_uuid = ip_addr_port.substr(ip_addr_port.find_first_of(':') + 1);
}

/**
 * Here you go. once this function is called your woker's job is to
 * keep looking for new tasks from Master, complete when given one and again
 * keep looking for the next one. Note that you have the access to BaseMapper's
 * member BaseMapperInternal impl_ and BaseReduer's member BaseReducerInternal
 * impl_ directly, so you can manipulate them however you want when running
 * map/reduce tasks
 * @return
 */
inline bool Worker::run()
{
    void *tag;
    bool ok;
    Worker::server = Worker::builder.BuildAndStart();
    std::thread heartbeat_job(&Worker::heartbeat_handler, this);
    new MapperHandler(&(Worker::mapreduce_service), work_queue.get(), worker_uuid);
    new ReducerHandler(&(Worker::mapreduce_service), work_queue.get(), worker_uuid);

    while (true)
    {
        GPR_ASSERT(work_queue->Next(&tag, &ok));
        static_cast<BaseHandler *>(tag)->Proceed();
    }
    return true;
}
/**
 * Heartbeat handler , recives heartbeat request and send them back with same values and ALIVE state,
 * unless clean_exit is marked true.
 */
inline void Worker::heartbeat_handler()
{
    void *tag;
    bool ok;

    new HeartbeatHandler(&(Worker::mapreduce_service), heartbeat_queue.get(), worker_uuid);

    while (true)
    {
        if (Worker::clean_exit)
            return;
        GPR_ASSERT(heartbeat_queue->Next(&tag, &ok));

        static_cast<BaseHandler *>(tag)->Proceed();
    }
}
/**
 * Conver grpc payload to FileShard.
 * @param partition
 * @return FileShard struct equivalent  of gprc partition payload.
 */
FileShard MapperHandler::convert_grpc_spec(masterworker::partition partition)
{
    FileShard shard{};
    shard.shard_id = partition.shard_id();
    for (auto f : partition.file_list())
    {
        splitFile temp{};
        temp.filename = f.filename();
        temp.offsets = {f.start_offset(), f.end_offset()};
        shard.split_file_list.push_back(temp);
    }
    return shard;
}
/**
 * Given GRPC Map request , Select intermedidate file. usually TEMP_DIR/<partition_count>_<worker_port>.txt
 * Reads shard files and apply user defined map function on it.
 * @param request grpc Map Request , check .proto
 * @return grpc Map Response payload , check .proto
 */
inline masterworker::Map_Response MapperHandler::handle_mapper_job(masterworker::Map_Request request)
{
    masterworker::Map_Response payload;
    auto user_mapper_func = get_mapper_from_task_factory(request.uuid());
    auto base_mapper = get_basemapper_internal(user_mapper_func.get());
    auto partition_count = request.partition_count();
    base_mapper->intermediate_file_list.reserve(partition_count);
    for (int i = 0; i < partition_count; i++)
    {
        base_mapper->intermediate_file_list.push_back(std::string(
            std::string(TEMP_DIR) + "/" + std::to_string(i) + "_" + MapperHandler::worker_address + ".txt"));
    }
    FileShard local_shard;
    for (int shard_count = 0; shard_count < request.shard_size(); shard_count++)
    {
        local_shard = MapperHandler::convert_grpc_spec(request.shard(shard_count));

        for (const auto &i : local_shard.split_file_list)
        {
            std::string mapper_line;
            std::ifstream f(i.filename, std::ios::binary);
            if (!f.good())
            {
                std::cerr << i.filename << " not open...." << std::endl;
            }

            f.seekg(i.offsets.first);
            std::string dummy(i.offsets.second - i.offsets.first, ' ');
            f.read(&dummy[0], i.offsets.second - i.offsets.first);
            std::stringstream stream(dummy);
            while (std::getline(stream, mapper_line))
            {
                user_mapper_func->map(mapper_line);
            }
        }
    }
    base_mapper->final_flush();
    for (const auto &i : base_mapper->intermediate_file_list)
    {
        payload.add_file_list(i);
    }

    return payload;
}
/**
 * Given GRPC Reduce request , Given output file , runs user defined reduce function and emits output data.
 * @param request grpc Reduce Request , check .proto
 * @return grpc Reduce Response payload , check .proto
 */
inline masterworker::Reduce_Response ReducerHandler::handle_reducer_job(masterworker::Reduce_Request request)
{
    masterworker::Reduce_Response payload;
    auto user_reducer_func = get_reducer_from_task_factory(request.uuid());
    auto base_reducer = get_basereducer_internal(user_reducer_func.get());
    base_reducer->file_name = request.output_file();
    std::map<std::string, std::vector<std::string>> key_value_map;
    payload.set_file_name(request.output_file());
    auto d = request.file_list();
    for (const auto &f : d)
    {
        std::ifstream fs(f);
        std::string dummy;
        try
        {
            if (fs.good() && fs.is_open())
            {
                while (std::getline(fs, dummy))
                {
                    key_value_map[dummy.substr(0, dummy.find_first_of(DELIMITER))].push_back(
                        dummy.substr(dummy.find_first_of(DELIMITER) + 1));
                }
            }
        }
        catch (std::ifstream::failure &e)
        {
            std::cerr << f + "  Error: " + e.what() << std::endl;
        }
    }
    for (const auto &k : key_value_map)
    {
        user_reducer_func->reduce(k.first, k.second);
    }
    key_value_map.clear();
    return payload;
}
/**
 * Handles Heartbeat request and return heartbeat payload
 * @param request Heartbeat payload check .proto
 * @return Heartbeat payload
 */
inline masterworker::Heartbeat_Payload HeartbeatHandler::handle_heartbeat_job(masterworker::Heartbeat_Payload request)
{
    masterworker::Heartbeat_Payload payload;
    payload.set_id(request.id());
    if (true)
        payload.set_status(masterworker::Heartbeat_Payload_type_ALIVE);
    return payload;
}
/**
 *
 * @param reducer
 * @return BaseReducerinternal Class
 */
inline BaseReducerInternal *ReducerHandler::get_basereducer_internal(BaseReducer *reducer)
{
    return Worker::get_basereducer_internal(reducer);
}
/**
 *
 * @param mapper
 * @return BaseMapperinternal Class
 */
inline BaseMapperInternal *MapperHandler::get_basemapper_internal(BaseMapper *mapper)
{
    return Worker::get_basemapper_internal(mapper);
}