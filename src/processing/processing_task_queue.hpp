/**
* Header file for the distrubuted processing Room
* @author creativeid00
*/

#ifndef GRPC_FOR_SUPERGENIUS_PROCESSING_TASK_QUEUE_HPP
#define GRPC_FOR_SUPERGENIUS_PROCESSING_TASK_QUEUE_HPP

#include <processing/proto/SGProcessing.pb.h>

#include <optional>

class ProcessingTaskQueue
{
public:
    virtual ~ProcessingTaskQueue() = default;

    virtual void EnqueueTask(
        const SGProcessing::Task& task,
        const std::list<SGProcessing::SubTask>& subTasks) = 0;

    virtual void GetSubTasks(
        const std::optional<std::string>& taskId,
        const std::set<SGProcessing::SubTaskState::Type>& states,
        const std::set<std::string>& excludeSubTaskIds,
        std::list<SGProcessing::SubTask>& subTasks) = 0;

    virtual bool GrabTask(std::string& taskKey, SGProcessing::Task& task) = 0;
    virtual bool CompleteTask(const std::string& taskKey, const SGProcessing::TaskResult& task) = 0;
};

#endif // GRPC_FOR_SUPERGENIUS_PROCESSING_TASK_QUEUE_HPP
