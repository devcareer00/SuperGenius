/**
* Header file for the distrubuted processing Room
* @author creativeid00
*/

#ifndef GRPC_FOR_SUPERGENIUS_PROCESSING_ENGINE_HPP
#define GRPC_FOR_SUPERGENIUS_PROCESSING_ENGINE_HPP

#include <processing/processing_core.hpp>
#include <processing/processing_subtask_storage.hpp>
#include <base/logger.hpp>

#include <ipfs_pubsub/gossip_pubsub_topic.hpp>

namespace sgns::processing
{
/** Handles subtask processing and processing results accumulation
*/
class ProcessingEngine
{
public:
    /** Create a processing engine object
    * @param processingCore specific processing core that process a subtask using specific algorithm
    * @param gossipPubSub pubsub host which is used to create subscriptions to result channels
    * @param nodeId - current processing node ID
    */
    ProcessingEngine(
        std::shared_ptr<sgns::ipfs_pubsub::GossipPubSub> gossipPubSub,
        std::string nodeId,
        std::shared_ptr<ProcessingCore> processingCore);

    // @todo rename to StartProcessing
    void StartQueueProcessing(
        std::shared_ptr<SubTaskStorage> subTaskStorage);

    void StopQueueProcessing();
    bool IsQueueProcessingStarted() const;

    std::vector<std::tuple<std::string, SGProcessing::SubTaskResult>> GetResults() const;

private:
    void OnSubTaskGrabbed(boost::optional<const SGProcessing::SubTask&> subTask);

    /** Processes a subtask and send the processing result to corresponding result channel
    * @param subTask - subtask that should be processed
    */
    void ProcessSubTask(SGProcessing::SubTask subTask);

    void OnResultChannelMessage(boost::optional<const sgns::ipfs_pubsub::GossipPubSub::Message&> message);

    std::shared_ptr<sgns::ipfs_pubsub::GossipPubSub> m_gossipPubSub;
    std::string m_nodeId;
    std::shared_ptr<ProcessingCore> m_processingCore;

    std::shared_ptr<SubTaskStorage> m_subTaskStorage;
    std::function<void(const SGProcessing::TaskResult&)> m_taskResultProcessingSink;
    std::map<std::string, std::shared_ptr<SGProcessing::SubTaskResult>> m_results;
    std::shared_ptr<sgns::ipfs_pubsub::GossipPubSubTopic> m_resultChannel;

    mutable std::mutex m_mutexResults;
    mutable std::mutex m_mutexSubTaskQueue;
    
    base::Logger m_logger = base::createLogger("ProcessingEngine");
};
}

#endif // GRPC_FOR_SUPERGENIUS_PROCESSING_ENGINE_HPP
