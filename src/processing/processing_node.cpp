#include "processing_node.hpp"
#include "processing_subtask_queue_channel_pubsub.hpp"
#include <processing/processing_subtask_queue_accessor_impl.hpp>

namespace sgns::processing
{
////////////////////////////////////////////////////////////////////////////////
ProcessingNode::ProcessingNode(
    std::shared_ptr<sgns::ipfs_pubsub::GossipPubSub> gossipPubSub,
    std::shared_ptr<SubTaskStateStorage> subTaskStateStorage,
    std::shared_ptr<SubTaskResultStorage> subTaskResultStorage,
    std::shared_ptr<ProcessingCore> processingCore,
    std::function<void(const SGProcessing::TaskResult&)> taskResultProcessingSink)
    : m_gossipPubSub(std::move(gossipPubSub))
    , m_nodeId(m_gossipPubSub->GetLocalAddress())
    , m_processingCore(processingCore)
    , m_subTaskStateStorage(subTaskStateStorage)
    , m_subTaskResultStorage(subTaskResultStorage)
    , m_taskResultProcessingSink(taskResultProcessingSink)
{
}

ProcessingNode::~ProcessingNode()
{
    if (m_processingEngine)
    {
        m_processingEngine->StopQueueProcessing();
    }
}

void ProcessingNode::Initialize(const std::string& processingQueueChannelId, size_t msSubscriptionWaitingDuration)
{
    // Subscribe to subtask queue channel
    auto processingQueueChannel = std::make_shared<ProcessingSubTaskQueueChannelPubSub>(m_gossipPubSub, processingQueueChannelId);

    m_subtaskQueueManager = std::make_shared<ProcessingSubTaskQueueManager>(
        processingQueueChannel, m_gossipPubSub->GetAsioContext(), m_nodeId);

    m_subTaskQueueAccessor = std::make_shared<SubTaskQueueAccessorImpl>(
        m_gossipPubSub,
        m_subtaskQueueManager,
        m_subTaskStateStorage,
        m_subTaskResultStorage,
        m_taskResultProcessingSink);

    processingQueueChannel->SetQueueRequestSink(
        std::bind(&ProcessingSubTaskQueueManager::ProcessSubTaskQueueRequestMessage,
            m_subtaskQueueManager, std::placeholders::_1));

    processingQueueChannel->SetQueueUpdateSink(
        std::bind(&ProcessingSubTaskQueueManager::ProcessSubTaskQueueMessage,
            m_subtaskQueueManager, std::placeholders::_1));

    m_processingEngine = std::make_shared<ProcessingEngine>(m_nodeId, m_processingCore);
        
    // Run messages processing once all dependent object are created
    processingQueueChannel->Listen(msSubscriptionWaitingDuration);

    // Keep the channel
    m_queueChannel = processingQueueChannel;
}

void ProcessingNode::AttachTo(const std::string& processingQueueChannelId, size_t msSubscriptionWaitingDuration)
{
    Initialize(processingQueueChannelId, msSubscriptionWaitingDuration);
    m_processingEngine->StartQueueProcessing(m_subTaskQueueAccessor);

    // Set timer to handle queue request timeout
}

void ProcessingNode::CreateProcessingHost(
    const std::string& processingQueueChannelId,
    std::list<SGProcessing::SubTask>& subTasks,
    size_t msSubscriptionWaitingDuration)
{
    Initialize(processingQueueChannelId, msSubscriptionWaitingDuration);

    m_subTaskQueueAccessor->AssignSubTasks(subTasks);

    m_processingEngine->StartQueueProcessing(m_subTaskQueueAccessor);
}

bool ProcessingNode::HasQueueOwnership() const
{
    return (m_subtaskQueueManager && m_subtaskQueueManager->HasOwnership());
}

////////////////////////////////////////////////////////////////////////////////
}
