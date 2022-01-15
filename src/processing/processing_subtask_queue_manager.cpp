#include "processing_subtask_queue_manager.hpp"

namespace sgns::processing
{
////////////////////////////////////////////////////////////////////////////////
ProcessingSubTaskQueueManager::ProcessingSubTaskQueueManager(
    std::shared_ptr<ProcessingSubTaskQueueChannel> queueChannel,
    std::shared_ptr<boost::asio::io_context> context,
    const std::string& localNodeId)
    : m_queueChannel(queueChannel)
    , m_context(std::move(context))
    , m_localNodeId(localNodeId)
    , m_dltQueueResponseTimeout(*m_context.get())
    , m_queueResponseTimeout(boost::posix_time::seconds(5))
    , m_dltGrabSubTaskTimeout(*m_context.get())
    , m_processingQueue(localNodeId)
    , m_processingTimeout(std::chrono::seconds(10))
{
}

bool ProcessingSubTaskQueueManager::CreateQueue(ProcessingCore::SubTaskList& subTasks)
{
    auto timestamp = std::chrono::system_clock::now();

    bool hasChunksDuplicates = false;

    auto queue = std::make_shared<SGProcessing::SubTaskQueue>();
    auto queueSubTasks = queue->mutable_subtasks();
    auto processingQueue = queue->mutable_processing_queue();
    for (auto itSubTask = subTasks.begin(); itSubTask != subTasks.end(); ++itSubTask)
    {
        queueSubTasks->AddAllocated(itSubTask->release());
        processingQueue->add_items();
    }

    std::lock_guard<std::mutex> guard(m_queueMutex);
    m_queue = std::move(queue);
    m_processingQueue.CreateQueue(processingQueue);

    PublishSubTaskQueue();

    return true;
}

bool ProcessingSubTaskQueueManager::UpdateQueue(SGProcessing::SubTaskQueue* pQueue)
{
    if (pQueue)
    {
        std::shared_ptr<SGProcessing::SubTaskQueue> queue(pQueue);
        if (m_processingQueue.UpdateQueue(queue->mutable_processing_queue()))
        {
            m_queue.swap(queue);
            std::vector<int> validItemIndices;
            for (int subTaskIdx = 0; subTaskIdx < m_queue->subtasks_size(); ++subTaskIdx)
            {
                const auto& subTask = m_queue->subtasks(subTaskIdx);
                if (m_results.find(subTask.subtaskid()) == m_results.end())
                {
                    validItemIndices.push_back(subTaskIdx);
                }
            }
            m_processingQueue.SetValidItemIndices(std::move(validItemIndices));
            LogQueue();
            return true;
        }
    }
    return false;
}

void ProcessingSubTaskQueueManager::ProcessPendingSubTaskGrabbing()
{
    // The method has to be called in scoped lock of queue mutex
    m_dltGrabSubTaskTimeout.expires_at(boost::posix_time::pos_infin);
    while (!m_onSubTaskGrabbedCallbacks.empty())
    {
        size_t itemIdx;
        if (m_processingQueue.GrabItem(itemIdx))
        {
            LogQueue();
            PublishSubTaskQueue();

            m_onSubTaskGrabbedCallbacks.front()({ m_queue->subtasks(itemIdx) });
            m_onSubTaskGrabbedCallbacks.pop_front();
        }
        else
        {
            // No available subtasks found
            auto unlocked = m_processingQueue.UnlockExpiredItems(m_processingTimeout);
            if (!unlocked)
            {
                break;
            }
        }
    }

    if (!m_onSubTaskGrabbedCallbacks.empty())
    {
        if (m_results.size() <(size_t)m_queue->processing_queue().items_size())
        {
            // Wait for subtasks are processed
            auto timestamp = std::chrono::system_clock::now();
            auto lastLockTimestamp = m_processingQueue.GetLastLockTimestamp();
            auto lastExpirationTime = lastLockTimestamp + m_processingTimeout;

            std::chrono::milliseconds grabSubTaskTimeout =
                (lastExpirationTime > timestamp)
                ? std::chrono::duration_cast<std::chrono::milliseconds>(lastExpirationTime - timestamp)
                : std::chrono::milliseconds(1);

            m_logger->debug("GRAB_TIMEOUT {}ms", grabSubTaskTimeout.count());

            m_dltGrabSubTaskTimeout.expires_from_now(
                boost::posix_time::milliseconds(grabSubTaskTimeout.count()));

            m_dltGrabSubTaskTimeout.async_wait(std::bind(
                &ProcessingSubTaskQueueManager::HandleGrabSubTaskTimeout, this, std::placeholders::_1));
        }
        else
        {
            while (!m_onSubTaskGrabbedCallbacks.empty())
            {
                // Let the requester know that there are no available subtasks
                m_onSubTaskGrabbedCallbacks.front()({});
                // Reset the callback
                m_onSubTaskGrabbedCallbacks.pop_front();
            }
        }
    }
}

void ProcessingSubTaskQueueManager::HandleGrabSubTaskTimeout(const boost::system::error_code& ec)
{
    if (ec != boost::asio::error::operation_aborted)
    {
        std::lock_guard<std::mutex> guard(m_queueMutex);
        m_dltGrabSubTaskTimeout.expires_at(boost::posix_time::pos_infin);
        m_logger->debug("HANDLE_GRAB_TIMEOUT");
        if (!m_onSubTaskGrabbedCallbacks.empty()
            && (m_results.size() < (size_t)m_queue->processing_queue().items_size()))
        {
            GrabSubTasks();
        }
    }
}

void ProcessingSubTaskQueueManager::GrabSubTask(SubTaskGrabbedCallback onSubTaskGrabbedCallback)
{
    std::lock_guard<std::mutex> guard(m_queueMutex);
    m_onSubTaskGrabbedCallbacks.push_back(std::move(onSubTaskGrabbedCallback));
    GrabSubTasks();
}

void ProcessingSubTaskQueueManager::GrabSubTasks()
{
    if (m_processingQueue.HasOwnership())
    {
        ProcessPendingSubTaskGrabbing();
    }
    else
    {
        // Send a request to grab a subtask queue
        m_queueChannel->RequestQueueOwnership(m_localNodeId);
    }
}

bool ProcessingSubTaskQueueManager::MoveOwnershipTo(const std::string& nodeId)
{
    std::lock_guard<std::mutex> guard(m_queueMutex);
    if (m_processingQueue.MoveOwnershipTo(nodeId))
    {
        LogQueue();
        PublishSubTaskQueue();
        return true;
    }
    return false;
}

bool ProcessingSubTaskQueueManager::HasOwnership() const
{
    std::lock_guard<std::mutex> guard(m_queueMutex);
    return m_processingQueue.HasOwnership();
}

void ProcessingSubTaskQueueManager::PublishSubTaskQueue() const
{
    m_queueChannel->PublishQueue(m_queue);
    m_logger->debug("QUEUE_PUBLISHED");
}

bool ProcessingSubTaskQueueManager::ProcessSubTaskQueueMessage(SGProcessing::SubTaskQueue* queue)
{
    std::lock_guard<std::mutex> guard(m_queueMutex);
    m_dltQueueResponseTimeout.expires_at(boost::posix_time::pos_infin);

    bool queueChanged = UpdateQueue(queue);
    if (queueChanged && m_processingQueue.HasOwnership())
    {
        ProcessPendingSubTaskGrabbing();
    }
    return queueChanged;
}

bool ProcessingSubTaskQueueManager::ProcessSubTaskQueueRequestMessage(
    const SGProcessing::SubTaskQueueRequest& request)
{
    std::lock_guard<std::mutex> guard(m_queueMutex);
    if (m_processingQueue.MoveOwnershipTo(request.node_id()))
    {
        LogQueue();
        PublishSubTaskQueue();
        return true;
    }

    m_logger->debug("QUEUE_REQUEST_RECEIVED");
    m_dltQueueResponseTimeout.expires_from_now(m_queueResponseTimeout);
    m_dltQueueResponseTimeout.async_wait(std::bind(
        &ProcessingSubTaskQueueManager::HandleQueueRequestTimeout, this, std::placeholders::_1));

    return false;
}

void ProcessingSubTaskQueueManager::HandleQueueRequestTimeout(const boost::system::error_code& ec)
{
    if (ec != boost::asio::error::operation_aborted)
    {
        std::lock_guard<std::mutex> guard(m_queueMutex);
        m_logger->debug("QUEUE_REQUEST_TIMEOUT");
        m_dltQueueResponseTimeout.expires_at(boost::posix_time::pos_infin);
        if (m_processingQueue.RollbackOwnership())
        {
            LogQueue();
            PublishSubTaskQueue();

            if (m_processingQueue.HasOwnership())
            {
                ProcessPendingSubTaskGrabbing();
            }
        }
    }
}

std::unique_ptr<SGProcessing::SubTaskQueue> ProcessingSubTaskQueueManager::GetQueueSnapshot() const
{
    auto queue = std::make_unique<SGProcessing::SubTaskQueue>();

    std::lock_guard<std::mutex> guard(m_queueMutex);
    if (m_queue)
    {
        queue->CopyFrom(*m_queue.get());
    }
    return std::move(queue);
}

bool ProcessingSubTaskQueueManager::AddSubTaskResult(
    const std::string& subTaskId, const SGProcessing::SubTaskResult& subTaskResult)
{
    std::lock_guard<std::mutex> guard(m_queueMutex);
    bool channelFound = false;
    for (int subTaskIdx = 0; subTaskIdx < m_queue->subtasks_size(); ++subTaskIdx)
    {
        const auto& subTask = m_queue->subtasks(subTaskIdx);
        if (subTask.subtaskid() == subTaskId)
        {
            channelFound = true;
            break;
        }
    }

    if (!channelFound)
    {
        m_logger->error("UNEXPECTED_SUBTASK_ID {}", subTaskId);
        return false;
    }
    
    SGProcessing::SubTaskResult result;
    result.CopyFrom(subTaskResult);
    m_results.insert(std::make_pair(subTaskId, std::move(result)));

    std::vector<int> validItemIndices;
    for (int subTaskIdx = 0; subTaskIdx < m_queue->subtasks_size(); ++subTaskIdx)
    {
        const auto& subTask = m_queue->subtasks(subTaskIdx);
        if (m_results.find(subTask.subtaskid()) == m_results.end())
        {
            validItemIndices.push_back(subTaskIdx);
        }
    }
    m_processingQueue.SetValidItemIndices(std::move(validItemIndices));

    return true;
}

bool ProcessingSubTaskQueueManager::IsProcessed() const
{
    std::lock_guard<std::mutex> guard(m_queueMutex);
    // The queue can contain only valid results
    return (m_results.size() == (size_t)m_queue->subtasks_size());
}

bool ProcessingSubTaskQueueManager::ValidateResults()
{
    std::lock_guard<std::mutex> guard(m_queueMutex);
    if ((m_results.size() != (size_t)m_queue->subtasks_size()))
    {
        return false;
    }

    bool areResultsValid = true;
    // Compare result hashes for each chunk
    // If a chunk hashes didn't match each other add the all subtasks with invalid hashes to VALID ITEMS LIST
    std::map<std::string, std::vector<uint32_t>> chunks;
    std::set<int> invalidSubtasksIndices;
    for (int subTaskIdx = 0; subTaskIdx < m_queue->subtasks_size(); ++subTaskIdx)
    {
        const auto& subTask = m_queue->subtasks(subTaskIdx);
        auto itResult = m_results.find(subTask.subtaskid());
        if (itResult != m_results.end())
        {
            if (itResult->second.chunk_hashes_size() != subTask.chunkstoprocess_size())
            {
                m_logger->error("WRONG_RESULT_HASHES_LENGTH {}", subTask.subtaskid());
                invalidSubtasksIndices.insert(subTaskIdx);
            }
            else
            {
                for (int chunkIdx = 0; chunkIdx < subTask.chunkstoprocess_size(); ++chunkIdx)
                {
                    auto it = chunks.insert(std::make_pair(
                        subTask.chunkstoprocess(chunkIdx).SerializeAsString(), std::vector<uint32_t>()));

                    it.first->second.push_back(itResult->second.chunk_hashes(chunkIdx));
                }
            }
        }
        else
        {
            // Since all subtasks are processed a result should be found for all of them
            m_logger->error("NO_RESULTS_FOUND {}", subTask.subtaskid());
            invalidSubtasksIndices.insert(subTaskIdx);
        }
    }

    for (int subTaskIdx = 0; subTaskIdx < m_queue->subtasks_size(); ++subTaskIdx)
    {
        const auto& subTask = m_queue->subtasks(subTaskIdx);
        if ((invalidSubtasksIndices.find(subTaskIdx) == invalidSubtasksIndices.end())
            && !CheckSubTaskResultHashes(subTask, chunks))
        {
            invalidSubtasksIndices.insert(subTaskIdx);
        }
    }

    if (!invalidSubtasksIndices.empty())
    {
        areResultsValid = false;

        std::vector<int> validItemIndices;
        for (auto invalidSubTaskIndex : invalidSubtasksIndices)
        {
            m_results.erase(m_queue->subtasks(invalidSubTaskIndex).subtaskid());
            validItemIndices.push_back(invalidSubTaskIndex);
        }

        m_processingQueue.SetValidItemIndices(std::move(validItemIndices));
    }
    return areResultsValid;
}

void ProcessingSubTaskQueueManager::LogQueue() const
{
    if (m_logger->level() <= spdlog::level::trace)
    {
        std::stringstream ss;
        ss << "{";
        ss << "\"owner_node_id\":\"" << m_queue->processing_queue().owner_node_id() << "\"";
        ss << "," << "\"last_update_timestamp\":" << m_queue->processing_queue().last_update_timestamp();
        ss << "," << "\"items\":[";
        for (int itemIdx = 0; itemIdx < m_queue->processing_queue().items_size(); ++itemIdx)
        {
            auto item = m_queue->processing_queue().items(itemIdx);
            ss << "{\"lock_node_id\":\"" << item.lock_node_id() << "\"";
            ss << ",\"lock_timestamp\":" << item.lock_timestamp() << "},";
        }
        ss << "]}";

        m_logger->trace(ss.str());
    }
}

bool ProcessingSubTaskQueueManager::CheckSubTaskResultHashes(
    const SGProcessing::SubTask& subTask,
    const std::map<std::string, std::vector<uint32_t>>& chunks) const
{
    for (int chunkIdx = 0; chunkIdx < subTask.chunkstoprocess_size(); ++chunkIdx)
    {
        const auto& chunk = subTask.chunkstoprocess(chunkIdx);
        auto it = chunks.find(chunk.SerializeAsString());
        if (it != chunks.end())
        {
            // Check duplicated chunks only
            if ((it->second.size() >= 2) 
                && !std::equal(it->second.begin() + 1, it->second.end(), it->second.begin()))
            {
                m_logger->debug("INVALID_CHUNK_RESULT_HASH [{}, {}]", subTask.subtaskid(), chunk.chunkid());
                return false;
            }
        }
        else
        {
            m_logger->debug("NO_CHUNK_RESULT_FOUND [{}, {}]", subTask.subtaskid(), chunk.chunkid());
            return false;
        }
    }
    return true;
}

}

////////////////////////////////////////////////////////////////////////////////
