/**
* Header file for the distributed subtasks queue
* @author creativeid00
*/

#ifndef GRPC_FOR_SUPERGENIUS_PROCESSING_SUBTASK_QUEUE_HPP
#define GRPC_FOR_SUPERGENIUS_PROCESSING_SUBTASK_QUEUE_HPP

#include "processing_core.hpp"

#include "SGProcessing.pb.h"

#include <ipfs_pubsub/gossip_pubsub_topic.hpp>

/** Distributed subtask queue implementation
*/
class ProcessingSubTaskQueue
{
public:
    typedef std::function<void(boost::optional<const SGProcessing::SubTask&>)> SubTaskGrabbedCallback;

    /** Construct an empty queue
    * @param queueChannel - task processing channel
    * @param localNodeId local processing node ID
    * @param proceessingCore - custom task processing algorithm
    */
    ProcessingSubTaskQueue(
        std::shared_ptr<sgns::ipfs_pubsub::GossipPubSubTopic> queueChannel,
        std::shared_ptr<boost::asio::io_context> context,
        const std::string& localNodeId,
        std::shared_ptr<ProcessingCore> processingCore);

    /** Create a subtask queue by splitting the task to subtasks using the processing code
    * @param task - task that should be split into subtasks
    */
    void CreateQueue(const SGProcessing::Task& task);

    /** Asynchronous getting of a subtask from the queue
    * @param onSubTaskGrabbedCallback a callback that is called when a grapped iosubtask is locked by the local node
    */
    void GrabSubTask(SubTaskGrabbedCallback onSubTaskGrabbedCallback);

    /** Transfer the queue ownership to another processing node
    * @param nodeId - processing node ID that the ownership should be transferred
    */
    bool MoveOwnershipTo(const std::string& nodeId);

    /** Rollbacks the queue ownership to the previous state
    * @return true if the ownership is successfully rolled back
    */
    bool RollbackOwnership();

    /** Checks id the local processing node owns the queue
    * @return true is the lolca node owns the queue
    */
    bool HasOwnership() const;

    /** Changes the local queue state with respect to passed queue snapshot
    * The method should be called from a processing channel message handler
    * @param queue received queue snapshot
    */
    bool ProcessSubTaskQueueMessage(SGProcessing::SubTaskQueue* queue);

    /** Changes the local queue state with respect to passed queue request
    * The method should be called from a processing channel message handler
    * @param request is a request for the queue ownership transferring
    */
    bool ProcessSubTaskQueueRequestMessage(const SGProcessing::SubTaskQueueRequest& request);

    /** Returns the current local queue snapshot
    * @return the queue snapshot
    */
    std::unique_ptr<SGProcessing::SubTaskQueue> GetQueueSnapshot() const;
private:
    /** Updates the local queue with a snapshot that have the most recent timestamp
    * @param queue - the queue snapshot
    */
    bool UpdateQueue(SGProcessing::SubTaskQueue* queue);

    void ChangeOwnershipTo(const std::string& nodeId);
    void HandleQueueRequestTimeout(const boost::system::error_code& ec);

    void PublishSubTaskQueue() const;
    bool HasOwnershipUnlocked() const;
    void LockSubTask();
    void LogQueue() const;

    std::shared_ptr<sgns::ipfs_pubsub::GossipPubSubTopic> m_queueChannel;
    std::shared_ptr<boost::asio::io_context> m_context;
    std::string m_localNodeId;
    std::shared_ptr<ProcessingCore> m_processingCore;
    std::shared_ptr<SGProcessing::SubTaskQueue> m_queue;
    mutable std::mutex m_queueMutex;
    std::list<SubTaskGrabbedCallback> m_onSubTaskGrabbedCallbacks;

    boost::asio::deadline_timer m_dltQueueResponseTimeout;
    boost::posix_time::time_duration m_queueResponseTimeout;

    libp2p::common::Logger m_logger = libp2p::common::createLogger("ProcessingSubTaskQueue");
};

#endif // GRPC_FOR_SUPERGENIUS_PROCESSING_TASK_QUEUE_HPP