/**
* Header file for subtask storage implementation
* @author creativeid00
*/

#ifndef SUPERGENIUS_PROCESSING_SUBTASK_STORAGE_IMPL_HPP
#define SUPERGENIUS_PROCESSING_SUBTASK_STORAGE_IMPL_HPP

#include <processing/processing_subtask_storage.hpp>
#include <processing/processing_subtask_queue_manager.hpp>

namespace sgns::processing
{
/** Subtask storage implementation
*/
class SubTaskStorageImpl: public SubTaskStorage
{
public:
    SubTaskStorageImpl(
        std::shared_ptr<ProcessingSubTaskQueueManager> subTaskQueueManager,
        std::function<void(const SGProcessing::TaskResult&)> taskResultProcessingSink);

    void GrabSubTask(SubTaskGrabbedCallback onSubTaskGrabbedCallback) override;
    void CompleteSubTask(const std::string& subTaskId, const SGProcessing::SubTaskResult& subTaskResult) override;

private:
    std::shared_ptr<ProcessingSubTaskQueueManager> m_subTaskQueueManager;
    std::function<void(const SGProcessing::TaskResult&)> m_taskResultProcessingSink;

    std::map<std::string, std::unique_ptr<SGProcessing::SubTaskResult>> m_results;

    base::Logger m_logger = base::createLogger("ProcessingSubTaskStorageImpl");
};
}

#endif // SUPERGENIUS_PROCESSING_SUBTASK_STORAGE_IMPL_HPP
