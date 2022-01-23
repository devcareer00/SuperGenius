#include "processing_service.hpp"

namespace sgns::processing
{
ProcessingServiceImpl::ProcessingServiceImpl(
    std::shared_ptr<sgns::ipfs_pubsub::GossipPubSub> gossipPubSub,
    size_t maximalNodesCount,
    std::shared_ptr<SubTaskEnqueuer> subTaskEnqueuer,
    std::shared_ptr<ProcessingCore> processingCore)
    : m_gossipPubSub(gossipPubSub)
    , m_context(gossipPubSub->GetAsioContext())
    , m_maximalNodesCount(maximalNodesCount)
    , m_subTaskEnqueuer(subTaskEnqueuer)
    , m_processingCore(processingCore)
    , m_timerChannelListRequestTimeout(*m_context.get())
    , m_channelListRequestTimeout(boost::posix_time::seconds(5))
{
}

void ProcessingServiceImpl::Listen(const std::string& processingGridChannelId)
{
    using GossipPubSubTopic = sgns::ipfs_pubsub::GossipPubSubTopic;
    m_gridChannel = std::make_unique<GossipPubSubTopic>(m_gossipPubSub, processingGridChannelId);
    m_gridChannel->Subscribe(std::bind(&ProcessingServiceImpl::OnMessage, this, std::placeholders::_1));
}

void ProcessingServiceImpl::SendChannelListRequest()
{
    SGProcessing::GridChannelMessage gridMessage;
    auto channelRequest = gridMessage.mutable_processing_channel_request();
    channelRequest->set_environment("any");

    m_gridChannel->Publish(gridMessage.SerializeAsString());
    m_logger->debug("List of processing channels requested");

    m_timerChannelListRequestTimeout.expires_from_now(m_channelListRequestTimeout);
    m_timerChannelListRequestTimeout.async_wait(std::bind(&ProcessingServiceImpl::HandleRequestTimeout, this));
}

void ProcessingServiceImpl::OnMessage(boost::optional<const sgns::ipfs_pubsub::GossipPubSub::Message&> message)
{
    if (message)
    {
        SGProcessing::GridChannelMessage gridMessage;
        if (gridMessage.ParseFromArray(message->data.data(), static_cast<int>(message->data.size())))
        {
            if (gridMessage.has_processing_channel_response())
            {
                auto response = gridMessage.processing_channel_response();

                m_logger->debug("Processing channel received. id:{}", response.channel_id());

                AcceptProcessingChannel(response.channel_id());
            }
            else if (gridMessage.has_processing_channel_request())
            {
                // @todo chenk environment requirements
                PublishLocalChannelList();
            }
        }
    }
}

void ProcessingServiceImpl::AcceptProcessingChannel(
    const std::string& processingQueuelId)
{
    auto& processingNodes = GetProcessingNodes();
    if (processingNodes.size() < m_maximalNodesCount)
    {
        auto node = std::make_shared<ProcessingNode>(
            m_gossipPubSub,
            m_processingCore,
            [](const SGProcessing::TaskResult&) {}); // @todo Add notification of finished task
        node->AttachTo(processingQueuelId);
        processingNodes[processingQueuelId] = node;
    }

    if (processingNodes.size() == m_maximalNodesCount)
    {
        m_timerChannelListRequestTimeout.expires_at(boost::posix_time::pos_infin);
    }
}

void ProcessingServiceImpl::PublishLocalChannelList()
{
    const auto& processingNodes = GetProcessingNodes();
    for (auto itNode = processingNodes.begin(); itNode != processingNodes.end(); ++itNode)
    {
        // Only channel host answers to reduce a number of published messages
        if (itNode->second->HasQueueOwnership())
        {
            SGProcessing::GridChannelMessage gridMessage;
            auto channelResponse = gridMessage.mutable_processing_channel_response();
            channelResponse->set_channel_id(itNode->first);

            m_gridChannel->Publish(gridMessage.SerializeAsString());
            m_logger->debug("Channel published. {}", channelResponse->channel_id());
        }
    }
}

std::map<std::string, std::shared_ptr<ProcessingNode>>& ProcessingServiceImpl::GetProcessingNodes()
{
    return m_processingNodes;
}

size_t ProcessingServiceImpl::GetProcessingNodesCount() const
{
    return m_processingNodes.size();
}

void ProcessingServiceImpl::SetChannelListRequestTimeout(
    boost::posix_time::time_duration channelListRequestTimeout)
{
    m_channelListRequestTimeout = channelListRequestTimeout;
}

void ProcessingServiceImpl::HandleRequestTimeout()
{
    m_timerChannelListRequestTimeout.expires_at(boost::posix_time::pos_infin);
    auto& processingNodes = GetProcessingNodes();
    while (processingNodes.size() < m_maximalNodesCount)
    {
        std::string subTaskQueueId;
        std::list<SGProcessing::SubTask> subTasks;
        if (m_subTaskEnqueuer->EnqueueSubTasks(subTaskQueueId, subTasks))
        {
            auto node = std::make_shared<ProcessingNode>(
                m_gossipPubSub,
                m_processingCore,
                [](const SGProcessing::TaskResult&) {}); // @todo Add notification of finished task

            // @todo Figure out if the task is still available for other peers
            // @todo Check if it is better to call EnqueueSubTasks within host 
            // and return subTaskQueueId from processing host?
            node->CreateProcessingHost(subTaskQueueId, subTasks);

            processingNodes[subTaskQueueId] = node;
            m_logger->debug("New processing channel created. {}", subTaskQueueId);
        }
        else
        {
            // If no tasks enquued, try to get available slots in existent queues
            SendChannelListRequest();
            break;
        }
    }
}
}
