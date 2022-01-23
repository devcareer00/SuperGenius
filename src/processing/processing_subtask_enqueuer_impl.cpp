#include "processing_subtask_enqueuer_impl.hpp"

namespace sgns::processing
{
SubTaskEnqueuerImpl::SubTaskEnqueuerImpl(
    std::shared_ptr<ProcessingTaskQueue> taskQueue,
    TaskSplitter taskSplitter)
    : m_taskQueue(taskQueue)
    , m_taskSplitter(taskSplitter)
{
}

bool SubTaskEnqueuerImpl::EnqueueSubTasks(
    std::string& subTaskQueueId, 
    std::list<SGProcessing::SubTask>& subTasks)
{
    SGProcessing::Task task;
    std::string taskKey;
    if (m_taskQueue->GrabTask(taskKey, task))
    {
        subTaskQueueId = taskKey;
        m_taskSplitter(task, subTasks);
        return true;
    }
    return false;
}

}