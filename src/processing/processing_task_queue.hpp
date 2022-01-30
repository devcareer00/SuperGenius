/**
* Header file for the distrubuted task queue
* @author creativeid00
*/

#ifndef GRPC_FOR_SUPERGENIUS_PROCESSING_TASK_QUEUE_HPP
#define GRPC_FOR_SUPERGENIUS_PROCESSING_TASK_QUEUE_HPP

#include <processing/proto/SGProcessing.pb.h>

#include <optional>

class ProcessingTaskQueue
{
/** Distributed task queue interface
*/
public:
    virtual ~ProcessingTaskQueue() = default;

    /** Enqueues a task with subtasks that the task has been split to
    * @param task - task to enqueue
    * @param subTasks - list of subtasks that the task has been split to
    */
    virtual void EnqueueTask(
        const SGProcessing::Task& task,
        const std::list<SGProcessing::SubTask>& subTasks) = 0;

    /** Returns a list of subtasks that satifsy to passed filter
    * @param taskId - task id. Can be an empty option. It is ignored for the case
    * @param states - a set of states. No states checked when the set is empty
    * @param excludeSubTaskIds - subtask ids to exclude from the result
    * @param subTasks - list of found subtasks
    */
    virtual void GetSubTasks(
        const std::optional<std::string>& taskId,
        const std::set<SGProcessing::SubTaskState::Type>& states,
        const std::set<std::string>& excludeSubTaskIds,
        std::list<SGProcessing::SubTask>& subTasks) = 0;

    /** Grabs a task from task queue
    * @return taskId - task id
    * @return task
    */
    virtual bool GrabTask(std::string& taskId, SGProcessing::Task& task) = 0;

    /** Handles task completion
    * @param taskId - task id
    * @param task result
    */
    virtual bool CompleteTask(const std::string& taskId, const SGProcessing::TaskResult& result) = 0;
};

#endif // GRPC_FOR_SUPERGENIUS_PROCESSING_TASK_QUEUE_HPP
