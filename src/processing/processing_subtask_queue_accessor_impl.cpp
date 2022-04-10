#include "processing_subtask_queue_accessor_impl.hpp"

namespace sgns::processing
{
SubTaskQueueAccessorImpl::SubTaskQueueAccessorImpl(
    std::shared_ptr<sgns::ipfs_pubsub::GossipPubSub> gossipPubSub,
    std::shared_ptr<ProcessingSubTaskQueueManager> subTaskQueueManager,
    std::shared_ptr<SubTaskStateStorage> subTaskStateStorage,
    std::shared_ptr<SubTaskResultStorage> subTaskResultStorage,
    std::function<void(const SGProcessing::TaskResult&)> taskResultProcessingSink)
    : m_gossipPubSub(gossipPubSub)
    , m_subTaskQueueManager(subTaskQueueManager)
    , m_subTaskStateStorage(subTaskStateStorage)
    , m_subTaskResultStorage(subTaskResultStorage)
    , m_taskResultProcessingSink(taskResultProcessingSink)
{
    // @todo replace hardcoded channel identified with an input value
    m_resultChannel = std::make_shared<ipfs_pubsub::GossipPubSubTopic>(m_gossipPubSub, "RESULT_CHANNEL_ID");
}

SubTaskQueueAccessorImpl::~SubTaskQueueAccessorImpl()
{
    m_logger->debug("[RELEASED] this: {}", reinterpret_cast<size_t>(this));
}

void SubTaskQueueAccessorImpl::ConnectToResultChannel()
{
    // It cannot be called in class constructor because shared_from_this doesn't work for the case
    // The shared_from_this() is required to prevent a case when the message processing callback
    // is called using an invalid 'this' pointer to destroyed object
    m_resultChannel->Subscribe(
        std::bind(
            &SubTaskQueueAccessorImpl::OnResultChannelMessage,
            weak_from_this(), std::placeholders::_1));
}

void SubTaskQueueAccessorImpl::AssignSubTasks(std::list<SGProcessing::SubTask>& subTasks)
{
    for (const auto& subTask : subTasks)
    {
        m_subTaskStateStorage->ChangeSubTaskState(
            subTask.subtaskid(), SGProcessing::SubTaskState::ENQUEUED);
    }

    std::vector<std::string> subTaskIds;
    for (auto& subTask : subTasks)
    {
        subTaskIds.push_back(subTask.subtaskid());
    }

    std::vector<SGProcessing::SubTaskResult> results;
    m_subTaskResultStorage->GetSubTaskResults(subTaskIds, results);

    std::set<std::string> processedSubTaskIds;
    for (auto& result : results)
    {
        processedSubTaskIds.insert(result.subtaskid());
        m_results.emplace(result.subtaskid(), std::move(result));
    }

    m_subTaskQueueManager->CreateQueue(subTasks, processedSubTaskIds);
}

void SubTaskQueueAccessorImpl::GrabSubTask(SubTaskGrabbedCallback onSubTaskGrabbedCallback)
{
    m_subTaskQueueManager->GrabSubTask(onSubTaskGrabbedCallback);
}

void SubTaskQueueAccessorImpl::CompleteSubTask(const std::string& subTaskId, const SGProcessing::SubTaskResult& subTaskResult)
{
    m_subTaskResultStorage->AddSubTaskResult(subTaskResult);
    m_subTaskStateStorage->ChangeSubTaskState(
        subTaskId, SGProcessing::SubTaskState::PROCESSED);

    m_resultChannel->Publish(subTaskResult.SerializeAsString());
    m_logger->debug("[RESULT_SENT]. ({}).", subTaskId);
}

void SubTaskQueueAccessorImpl::OnResultReceived(SGProcessing::SubTaskResult&& subTaskResult)
{
    std::string subTaskId = subTaskResult.subtaskid();

    // Results accumulation
    std::lock_guard<std::mutex> guard(m_mutexResults);
    m_results.emplace(subTaskId, std::move(subTaskResult));

    m_subTaskQueueManager->ChangeSubTaskProcessingStates({ subTaskId }, true);

    // Task processing finished
    if (m_subTaskQueueManager->IsProcessed()) 
    {
        std::set<std::string> invalidSubTaskIds;

        auto queue = m_subTaskQueueManager->GetQueueSnapshot();
        std::list<SGProcessing::SubTask> subTasks;
        for (size_t subTaskIdx = 0; subTaskIdx < queue->subtasks_size(); ++subTaskIdx)
        {
            subTasks.push_back(queue->subtasks(subTaskIdx));
        }

        bool valid = ValidateResults(subTasks, invalidSubTaskIds);

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
                    result->CopyFrom(r.second);
                }
                m_taskResultProcessingSink(taskResult);
                // @todo Notify other nodes that the task is finalized
            }
            else
            {
                // @todo Process task finalization expiration
            }
        }
        else
        {
            m_subTaskQueueManager->ChangeSubTaskProcessingStates(invalidSubTaskIds, false);
            for (const auto& subTaskId : invalidSubTaskIds)
            {
                m_results.erase(subTaskId);
            }
        }
    }
    // @todo Check that the queue processing is continued when subtasks invalidated
}

std::vector<std::tuple<std::string, SGProcessing::SubTaskResult>> SubTaskQueueAccessorImpl::GetResults() const
{
    std::lock_guard<std::mutex> guard(m_mutexResults);
    std::vector<std::tuple<std::string, SGProcessing::SubTaskResult>> results;
    for (auto& item : m_results)
    {
        results.push_back({ item.first, item.second });
    }
    std::sort(results.begin(), results.end(),
        [](const std::tuple<std::string, SGProcessing::SubTaskResult>& v1,
            const std::tuple<std::string, SGProcessing::SubTaskResult>& v2) { return std::get<0>(v1) < std::get<0>(v2); });

    return results;
}

void SubTaskQueueAccessorImpl::OnResultChannelMessage(
    std::weak_ptr<SubTaskQueueAccessorImpl> weakThis,
    boost::optional<const sgns::ipfs_pubsub::GossipPubSub::Message&> message)
{
    auto _this = weakThis.lock();
    if (!_this)
    {
        return;
    }
    
    if (message)
    {
        SGProcessing::SubTaskResult result;
        if (result.ParseFromArray(message->data.data(), static_cast<int>(message->data.size())))
        {
            _this->m_logger->debug("[RESULT_RECEIVED]. ({}).", result.ipfs_results_data_id());

            _this->OnResultReceived(std::move(result));
        }
    }
}

bool SubTaskQueueAccessorImpl::ValidateResults(
    const std::list<SGProcessing::SubTask>& subTasks,
    std::set<std::string>& invalidSubTaskIds)
{
    bool areResultsValid = true;
    // Compare result hashes for each chunk
    // If a chunk hashes didn't match each other add the all subtasks with invalid hashes to VALID ITEMS LIST
    std::map<std::string, std::vector<uint32_t>> chunks;
    for (const auto subTask : subTasks)
    {
        auto itResult = m_results.find(subTask.subtaskid());
        if (itResult != m_results.end())
        {
            if (itResult->second.chunk_hashes_size() != subTask.chunkstoprocess_size())
            {
                m_logger->error("WRONG_RESULT_HASHES_LENGTH {}", subTask.subtaskid());
                invalidSubTaskIds.insert(subTask.subtaskid());
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
            invalidSubTaskIds.insert(subTask.subtaskid());
        }
    }

    for (const auto subTask : subTasks)
    {
        if ((invalidSubTaskIds.find(subTask.subtaskid()) == invalidSubTaskIds.end())
            && !CheckSubTaskResultHashes(subTask, chunks))
        {
            invalidSubTaskIds.insert(subTask.subtaskid());
        }
    }

    if (!invalidSubTaskIds.empty())
    {
        areResultsValid = false;
    }
    return areResultsValid;
}

bool SubTaskQueueAccessorImpl::CheckSubTaskResultHashes(
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
