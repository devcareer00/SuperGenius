#include "processing_subtask_queue_accessor_impl.hpp"

namespace sgns::processing
{
SubTaskStorageImpl::SubTaskStorageImpl(
    std::shared_ptr<sgns::ipfs_pubsub::GossipPubSub> gossipPubSub,
    std::shared_ptr<ProcessingSubTaskQueueManager> subTaskQueueManager,
    std::function<void(const SGProcessing::TaskResult&)> taskResultProcessingSink)
    : m_gossipPubSub(gossipPubSub)
    , m_subTaskQueueManager(subTaskQueueManager)
    , m_taskResultProcessingSink(taskResultProcessingSink)
{
    // @todo replace hardcoded channel identified with an input value
    m_resultChannel = std::make_shared<ipfs_pubsub::GossipPubSubTopic>(m_gossipPubSub, "RESULT_CHANNEL_ID");
    m_resultChannel->Subscribe(std::bind(&SubTaskStorageImpl::OnResultChannelMessage, this, std::placeholders::_1));
}

void SubTaskStorageImpl::GrabSubTask(SubTaskGrabbedCallback onSubTaskGrabbedCallback)
{
    m_subTaskQueueManager->GrabSubTask(onSubTaskGrabbedCallback);
}

void SubTaskStorageImpl::CompleteSubTask(const std::string& subTaskId, const SGProcessing::SubTaskResult& subTaskResult)
{
    m_resultChannel->Publish(subTaskResult.SerializeAsString());
    m_logger->debug("[RESULT_SENT]. ({}).", subTaskId);
}


void SubTaskStorageImpl::OnResultReceived(const std::string& subTaskId, const SGProcessing::SubTaskResult& subTaskResult)
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

std::vector<std::tuple<std::string, SGProcessing::SubTaskResult>> SubTaskStorageImpl::GetResults() const
{
    std::lock_guard<std::mutex> guard(m_mutexResults);
    std::vector<std::tuple<std::string, SGProcessing::SubTaskResult>> results;
    for (auto& item : m_results)
    {
        results.push_back({ item.first, *item.second });
    }
    std::sort(results.begin(), results.end(),
        [](const std::tuple<std::string, SGProcessing::SubTaskResult>& v1,
            const std::tuple<std::string, SGProcessing::SubTaskResult>& v2) { return std::get<0>(v1) < std::get<0>(v2); });

    return results;
}

void SubTaskStorageImpl::OnResultChannelMessage(
    boost::optional<const sgns::ipfs_pubsub::GossipPubSub::Message&> message)
{
    if (message)
    {
        auto result = std::make_shared<SGProcessing::SubTaskResult>();
        if (result->ParseFromArray(message->data.data(), static_cast<int>(message->data.size())))
        {
            m_logger->debug("[RESULT_RECEIVED]. ({}).", result->ipfs_results_data_id());

            OnResultReceived(result->subtaskid(), *result);

            // Results accumulation
            {
                std::lock_guard<std::mutex> guard(m_mutexResults);
                m_results.insert({ result->subtaskid(), std::move(result) });
            }
        }
    }
}
}
