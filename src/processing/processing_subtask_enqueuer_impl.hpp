#ifndef SUPERGENIUS_PROCESSING_SUBTASK_ENQUEUER_IMPL_HPP
#define SUPERGENIUS_PROCESSING_SUBTASK_ENQUEUER_IMPL_HPP

#include <processing/processing_subtask_enqueuer.hpp>
#include <processing/processing_task_queue.hpp>

namespace sgns::processing
{
// Encapsulates subtask queue construction algorithm
class SubTaskEnqueuerImpl : public SubTaskEnqueuer
{
public:
    typedef std::function<void(const SGProcessing::Task&, std::list<SGProcessing::SubTask>&)> TaskSplitter;

    SubTaskEnqueuerImpl(
        std::shared_ptr<ProcessingTaskQueue> taskQueue,
        TaskSplitter taskSplitter
    );

    bool EnqueueSubTasks(
        std::string& subTaskQueueId, 
        std::list<SGProcessing::SubTask>& subTasks) override;

private:
    std::shared_ptr<ProcessingTaskQueue> m_taskQueue;
    TaskSplitter m_taskSplitter;

};
}

#endif // SUPERGENIUS_PROCESSING_SUBTASK_ENQUEUER_IMPL_HPP
