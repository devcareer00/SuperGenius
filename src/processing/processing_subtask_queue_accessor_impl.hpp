/**
* Header file for subtask queue accessor implementation
* @author creativeid00
*/

#ifndef SUPERGENIUS_PROCESSING_SUBTASK_QUEUE_ACCESSOR_IMPL_HPP
#define SUPERGENIUS_PROCESSING_SUBTASK_QUEUE_ACCESSOR_IMPL_HPP

#include <processing/processing_subtask_queue_accessor.hpp>
#include <processing/processing_subtask_queue_manager.hpp>

#include <ipfs_pubsub/gossip_pubsub_topic.hpp>

namespace sgns::processing
{
/** Subtask storage implementation
*/
class SubTaskQueueAccessorImpl: public SubTaskQueueAccessor
{
public:
    /** Create sub-task queue accessor implementation object
    * @param gossipPubSub pubsub host which is used to create subscriptions to result channel
    */
    SubTaskQueueAccessorImpl(
        std::shared_ptr<sgns::ipfs_pubsub::GossipPubSub> gossipPubSub,
        std::shared_ptr<ProcessingSubTaskQueueManager> subTaskQueueManager,
        std::function<void(const SGProcessing::TaskResult&)> taskResultProcessingSink);

    void GrabSubTask(SubTaskGrabbedCallback onSubTaskGrabbedCallback) override;
    void CompleteSubTask(const std::string& subTaskId, const SGProcessing::SubTaskResult& subTaskResult) override;

    std::vector<std::tuple<std::string, SGProcessing::SubTaskResult>> GetResults() const;

private:
    void OnResultReceived(const std::string& subTaskId, const SGProcessing::SubTaskResult& subTaskResult);
    
    void OnResultChannelMessage(boost::optional<const sgns::ipfs_pubsub::GossipPubSub::Message&> message);

    std::shared_ptr<sgns::ipfs_pubsub::GossipPubSub> m_gossipPubSub;
    std::shared_ptr<ProcessingSubTaskQueueManager> m_subTaskQueueManager;
    std::function<void(const SGProcessing::TaskResult&)> m_taskResultProcessingSink;

    std::shared_ptr<sgns::ipfs_pubsub::GossipPubSubTopic> m_resultChannel;

    mutable std::mutex m_mutexResults;
    std::map<std::string, std::shared_ptr<SGProcessing::SubTaskResult>> m_results;

    base::Logger m_logger = base::createLogger("ProcessingSubTaskQueueAccessorImpl");
};
}

#endif // SUPERGENIUS_PROCESSING_SUBTASK_QUEUE_ACCESSOR_IMPL_HPP
