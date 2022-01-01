#include "processing_node.hpp"

namespace sgns::processing
{
////////////////////////////////////////////////////////////////////////////////
ProcessingNode::ProcessingNode(
    std::shared_ptr<sgns::ipfs_pubsub::GossipPubSub> gossipPubSub,
    std::shared_ptr<ProcessingCore> processingCore,
    std::function<void(const SGProcessing::TaskResult&)> taskResultProcessingSink)
    : m_gossipPubSub(std::move(gossipPubSub))
    , m_nodeId(m_gossipPubSub->GetLocalAddress())
    , m_processingCore(processingCore)
    , m_taskResultProcessingSink(taskResultProcessingSink)
{    
}

ProcessingNode::~ProcessingNode()
{
}

void ProcessingNode::Initialize(const std::string& processingQueueChannelId, size_t msSubscriptionWaitingDuration)
{
    using GossipPubSubTopic = sgns::ipfs_pubsub::GossipPubSubTopic;

    // Subscribe to the dataBlock channel
    m_processingQueueChannel = std::make_shared<GossipPubSubTopic>(m_gossipPubSub, processingQueueChannelId);

    m_subtaskQueueManager = std::make_shared<ProcessingSubTaskQueueManager>(
        m_processingQueueChannel, m_gossipPubSub->GetAsioContext(), m_nodeId);
    m_processingEngine = std::make_unique<ProcessingEngine>(
        m_gossipPubSub, m_nodeId, m_processingCore, m_taskResultProcessingSink);

    // Run messages processing once all dependent object are created
    m_processingQueueChannel->Subscribe(std::bind(&ProcessingNode::OnProcessingChannelMessage, this, std::placeholders::_1));
    if (msSubscriptionWaitingDuration > 0)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(msSubscriptionWaitingDuration));
    }
}

void ProcessingNode::AttachTo(const std::string& processingChannelId, size_t msSubscriptionWaitingDuration)
{
    Initialize(processingChannelId, msSubscriptionWaitingDuration);
    m_processingEngine->StartQueueProcessing(m_subtaskQueueManager);

}

void ProcessingNode::CreateProcessingHost(
    const std::string& processingQueueChannelId,
    std::list<std::unique_ptr<SGProcessing::SubTask>>& subTasks,
    size_t msSubscriptionWaitingDuration)
{
    Initialize(processingQueueChannelId, msSubscriptionWaitingDuration);

    m_subtaskQueueManager->CreateQueue(subTasks);

    m_processingEngine->StartQueueProcessing(m_subtaskQueueManager);
}

void ProcessingNode::OnProcessingChannelMessage(boost::optional<const sgns::ipfs_pubsub::GossipPubSub::Message&> message)
{
    if (message)
    {
        SGProcessing::ProcessingChannelMessage channelMesssage;
        if (channelMesssage.ParseFromArray(message->data.data(), static_cast<int>(message->data.size())))
        {
            if (channelMesssage.has_subtask_queue_request())
            {
                HandleSubTaskQueueRequest(channelMesssage);
            }
            else if (channelMesssage.has_subtask_queue())
            {
                HandleSubTaskQueue(channelMesssage);
            }
        }
    }
}

void ProcessingNode::HandleSubTaskQueueRequest(SGProcessing::ProcessingChannelMessage& channelMesssage)
{
    if (m_subtaskQueueManager)
    {
        m_subtaskQueueManager->ProcessSubTaskQueueRequestMessage(
            channelMesssage.subtask_queue_request());
    }
}

void ProcessingNode::HandleSubTaskQueue(SGProcessing::ProcessingChannelMessage& channelMesssage)
{
    if (m_subtaskQueueManager)
    {
        m_subtaskQueueManager->ProcessSubTaskQueueMessage(
            channelMesssage.release_subtask_queue());
    }
}

bool ProcessingNode::HasQueueOwnership() const
{
    return (m_subtaskQueueManager && m_subtaskQueueManager->HasOwnership());
}

////////////////////////////////////////////////////////////////////////////////
}
