#ifndef SUPERGENIUS_PROCESSING_SUBTASK_QUEUE_CHANNEL_PUBSUB_HPP
#define SUPERGENIUS_PROCESSING_SUBTASK_QUEUE_CHANNEL_PUBSUB_HPP

#include <processing/processing_subtask_queue_channel.hpp>

#include <ipfs_pubsub/gossip_pubsub_topic.hpp>

namespace sgns::processing
{
class ProcessingSubTaskQueueChannelPubSub : public ProcessingSubTaskQueueChannel
{
public:
    typedef std::function<bool(const SGProcessing::SubTaskQueueRequest&)> QueueRequestSink;
    typedef std::function<bool(SGProcessing::SubTaskQueue*)> QueueUpdateSink;

    ProcessingSubTaskQueueChannelPubSub(
        std::shared_ptr<sgns::ipfs_pubsub::GossipPubSub> gossipPubSub,
        const std::string& processingQueueChannelId);

    void RequestQueueOwnership(const std::string& nodeId) override;
    void PublishQueue(std::shared_ptr<SGProcessing::SubTaskQueue> queue) override;

    void SetQueueRequestSink(QueueRequestSink queueRequestSink);
    void SetQueueUpdateSink(QueueUpdateSink queueUpdateSink);

    void Listen(size_t msSubscriptionWaitingDuration);

private:
    std::shared_ptr<sgns::ipfs_pubsub::GossipPubSubTopic> m_processingQueueChannel;

    void OnProcessingChannelMessage(boost::optional<const sgns::ipfs_pubsub::GossipPubSub::Message&> message);

    void HandleSubTaskQueueRequest(SGProcessing::ProcessingChannelMessage& channelMesssage);
    void HandleSubTaskQueue(SGProcessing::ProcessingChannelMessage& channelMesssage);

    std::shared_ptr<sgns::ipfs_pubsub::GossipPubSub> m_gossipPubSub;
    std::shared_ptr<boost::asio::io_context> m_context;

    std::function<bool(const SGProcessing::SubTaskQueueRequest&)> m_queueRequestSink;
    std::function<bool(SGProcessing::SubTaskQueue*)> m_queueUpdateSink;
};
}
#endif // SUPERGENIUS_PROCESSING_SUBTASK_QUEUE_CHANNEL_PUBSUB_HPP