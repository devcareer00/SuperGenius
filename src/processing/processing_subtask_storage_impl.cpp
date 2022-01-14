#include "processing_subtask_storage_impl.hpp"

namespace sgns::processing
{
SubTaskStorageImpl::SubTaskStorageImpl(
    std::shared_ptr<ProcessingSubTaskQueueManager> subTaskQueueManager,
    std::function<void(const SGProcessing::TaskResult&)> taskResultProcessingSink)
    : m_subTaskQueueManager(subTaskQueueManager)
    , m_taskResultProcessingSink(taskResultProcessingSink)
{
}

void SubTaskStorageImpl::GrabSubTask(SubTaskGrabbedCallback onSubTaskGrabbedCallback)
{
    m_subTaskQueueManager->GrabSubTask(onSubTaskGrabbedCallback);
}

void SubTaskStorageImpl::CompleteSubTask(const std::string& subTaskId, const SGProcessing::SubTaskResult& subTaskResult)
{
    m_subTaskQueueManager->AddSubTaskResult(subTaskId, subTaskResult);

    // Task processing finished
    if (m_subTaskQueueManager->IsProcessed()) 
    {
        bool valid = m_subTaskQueueManager->ValidateResults();
        m_logger->debug("RESULTS_VALIDATED: {}", valid ? "VALID" : "INVALID");
        if (valid)
        {
            // @todo Add a test where the owner disconnected, but the last valid result is received by slave nodes
            // @todo Request the ownership instead of just checking
            if (m_subTaskQueueManager->HasOwnership())
            {
                SGProcessing::TaskResult taskResult;
                auto results = taskResult.mutable_subtask_results();
                for (const auto& r : m_results)
                {
                    auto result = results->Add();
                    result->CopyFrom(*r.second);
                }
                m_taskResultProcessingSink(taskResult);
                // @todo Notify other nodes that the task is finalized
            }
            else
            {
                // @todo Process task finalization expiration
            }
        }
    }
    // @todo Check that the queue processing is continued when subtasks invalidated
}
}
