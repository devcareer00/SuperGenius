/**
* Header file for the distributed subtasks queue
* @author creativeid00
*/

#ifndef SUPERGENIUS_PROCESSING_SHARED_QUEUE_HPP
#define SUPERGENIUS_PROCESSING_SHARED_QUEUE_HPP

#include "processing_shared_queue_data_view.hpp"
#include <libp2p/common/logger.hpp>

namespace sgns::processing
{
/** Distributed queue implementation
*/
class SharedQueue
{
public:
    /** Construct an empty queue
    * @param localNodeId local processing node ID
    */
    SharedQueue(const std::string& localNodeId);

    /** Create a subtask queue by splitting the task to subtasks using the processing code
    * @param task - task that should be split into subtasks
    */
    void CreateQueue(std::shared_ptr<SharedQueueDataView> queue);

    /** Asynchronous getting of a subtask from the queue
    * @param onSubTaskGrabbedCallback a callback that is called when a grapped iosubtask is locked by the local node
    */
    bool GrabItem(size_t& grabbedItemIndex);

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

    /** Updates the local queue with a snapshot that have the most recent timestamp
    * @param queue - the queue snapshot
    */
    bool UpdateQueue(std::shared_ptr<SharedQueueDataView> queue);

    bool UnlockExpiredItems(std::chrono::system_clock::duration expirationTimeout);

    std::chrono::system_clock::time_point GetLastLockTimestamp() const;

private:
    void ChangeOwnershipTo(const std::string& nodeId);

    bool LockItem(size_t& lockedItemIndex);

    void LogQueue() const;

    std::string m_localNodeId;
    std::shared_ptr<SharedQueueDataView> m_queue;

    libp2p::common::Logger m_logger = libp2p::common::createLogger("ProcessingSharedQueue");
};
}

#endif // SUPERGENIUS_PROCESSING_SHARED_QUEUE_HPP