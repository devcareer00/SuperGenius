/**
* Header file for the distrubuted processing node
* @author creativeid00
*/

#ifndef GRPC_FOR_SUPERGENIUS_PROCESSING_NODE
#define GRPC_FOR_SUPERGENIUS_PROCESSING_NODE

#include <processing/processing_engine.hpp>

#include <ipfs_pubsub/gossip_pubsub_topic.hpp>

namespace sgns::processing
{
/**
* A node for distributed computation.
* Allows to conduct a computation processing by multiple workers 
*/
class ProcessingNode
{
public:
    /** Constructs a processing node
    * @param gossipPubSub - pubsub service
    */
    ProcessingNode(
        std::shared_ptr<sgns::ipfs_pubsub::GossipPubSub> gossipPubSub,
        std::shared_ptr<ProcessingCore> processingCore,
        std::function<void(const SGProcessing::TaskResult&)> taskResultProcessingSink);

    ~ProcessingNode();
    /** Attaches the node to the processing channel
    * @param processingQueueChannelId - identifier of a processing queue channel
    * @return flag indicating if the room is joined for block data processing
    */
    void AttachTo(const std::string& processingQueueChannelId, size_t msSubscriptionWaitingDuration = 0);
    void CreateProcessingHost(
        const std::string& processingQueueChannelId,
        std::list<std::unique_ptr<SGProcessing::SubTask>>& subTasks,
        size_t msSubscriptionWaitingDuration = 0);

    bool HasQueueOwnership() const;

private:
    void Initialize(const std::string& processingQueueChannelId, size_t msSubscriptionWaitingDuration);
    void OnProcessingChannelMessage(boost::optional<const sgns::ipfs_pubsub::GossipPubSub::Message&> message);

    void HandleSubTaskQueueRequest(SGProcessing::ProcessingChannelMessage& channelMesssage);
    void HandleSubTaskQueue(SGProcessing::ProcessingChannelMessage& channelMesssage);

    std::shared_ptr<sgns::ipfs_pubsub::GossipPubSub> m_gossipPubSub;
    std::shared_ptr<sgns::ipfs_pubsub::GossipPubSubTopic> m_processingQueueChannel;

    std::string m_nodeId;
    std::shared_ptr<ProcessingCore> m_processingCore;

    std::unique_ptr<ProcessingEngine> m_processingEngine;
    std::shared_ptr<ProcessingSubTaskQueueManager> m_subtaskQueueManager;
    std::function<void(const SGProcessing::TaskResult&)> m_taskResultProcessingSink;
};
}

#endif // GRPC_FOR_SUPERGENIUS_PROCESSING_NODE
