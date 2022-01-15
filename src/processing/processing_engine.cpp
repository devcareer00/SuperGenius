#include "processing_engine.hpp"

namespace sgns::processing
{
ProcessingEngine::ProcessingEngine(
    std::shared_ptr<sgns::ipfs_pubsub::GossipPubSub> gossipPubSub,
    std::string nodeId,
    std::shared_ptr<ProcessingCore> processingCore)
    : m_gossipPubSub(gossipPubSub)
    , m_nodeId(std::move(nodeId))
    , m_processingCore(processingCore)
{
}

void ProcessingEngine::StartQueueProcessing(
    std::shared_ptr<SubTaskStorage> subTaskStorage)
{
    std::lock_guard<std::mutex> queueGuard(m_mutexSubTaskQueue);
    m_subTaskStorage = subTaskStorage;

    // @todo replace hardcoded channel identified with an input value
    m_resultChannel = std::make_shared<ipfs_pubsub::GossipPubSubTopic>(m_gossipPubSub, "RESULT_CHANNEL_ID");
    m_resultChannel->Subscribe(std::bind(&ProcessingEngine::OnResultChannelMessage, this, std::placeholders::_1));

    m_subTaskStorage->GrabSubTask(std::bind(&ProcessingEngine::OnSubTaskGrabbed, this, std::placeholders::_1));
}

void ProcessingEngine::StopQueueProcessing()
{
    std::lock_guard<std::mutex> queueGuard(m_mutexSubTaskQueue);
    m_subTaskStorage.reset();
}

bool ProcessingEngine::IsQueueProcessingStarted() const
{
    std::lock_guard<std::mutex> queueGuard(m_mutexSubTaskQueue);
    return (m_subTaskStorage.get() != nullptr);
}

void ProcessingEngine::OnSubTaskGrabbed(boost::optional<const SGProcessing::SubTask&> subTask)
{
    if (subTask)
    {
        m_logger->debug("[GRABBED]. ({}).", subTask->subtaskid());
        ProcessSubTask(*subTask);
    }
}

void ProcessingEngine::ProcessSubTask(SGProcessing::SubTask subTask)
{
    m_logger->debug("[PROCESSING_STARTED]. ({}).", subTask.subtaskid());
    std::thread thread([subTask(std::move(subTask)), this]()
    {
        SGProcessing::SubTaskResult result;
        // @todo set initial hash code that depends on node id
        m_processingCore->ProcessSubTask(subTask, result, std::hash<std::string>{}(m_nodeId));
        // @todo replace results_channel with subtaskid
        result.set_subtaskid(subTask.subtaskid());
        m_logger->debug("[PROCESSED]. ({}).", subTask.subtaskid());
        m_resultChannel->Publish(result.SerializeAsString());
        m_logger->debug("[RESULT_SENT]. ({}).", subTask.subtaskid());

        std::lock_guard<std::mutex> queueGuard(m_mutexSubTaskQueue);
        if (m_subTaskStorage)
        {
            // @todo Should a new subtask be grabbed once the perivious one is processed?
            m_subTaskStorage->GrabSubTask(std::bind(&ProcessingEngine::OnSubTaskGrabbed, this, std::placeholders::_1));
        }
    });
    thread.detach();
}

std::vector<std::tuple<std::string, SGProcessing::SubTaskResult>> ProcessingEngine::GetResults() const
{
    std::lock_guard<std::mutex> guard(m_mutexResults);
    std::vector<std::tuple<std::string, SGProcessing::SubTaskResult>> results;
    for (auto& item : m_results)
    {
        results.push_back({item.first, *item.second});
    }
    std::sort(results.begin(), results.end(),
        [](const std::tuple<std::string, SGProcessing::SubTaskResult>& v1,
            const std::tuple<std::string, SGProcessing::SubTaskResult>& v2) { return std::get<0>(v1) < std::get<0>(v2); });

    return results;
}

void ProcessingEngine::OnResultChannelMessage(
    boost::optional<const sgns::ipfs_pubsub::GossipPubSub::Message&> message)
{
    if (message)
    {
        auto result = std::make_shared<SGProcessing::SubTaskResult>();
        if (result->ParseFromArray(message->data.data(), static_cast<int>(message->data.size())))
        {
            m_logger->debug("[RESULT_RECEIVED]. ({}).", result->ipfs_results_data_id());

            std::lock_guard<std::mutex> queueGuard(m_mutexSubTaskQueue);
            if (m_subTaskStorage)
            {
                m_subTaskStorage->CompleteSubTask(result->subtaskid(), *result);
            }
            // Results accumulation
            {
                std::lock_guard<std::mutex> guard(m_mutexResults);
                m_results.insert({ result->subtaskid(), std::move(result) });
            }
        }
    }
}
}
