#include "processing_subtask_queue.hpp"

////////////////////////////////////////////////////////////////////////////////
ProcessingSubTaskQueue::ProcessingSubTaskQueue(
    std::shared_ptr<sgns::ipfs_pubsub::GossipPubSubTopic> queueChannel,
    std::shared_ptr<boost::asio::io_context> context,
    const std::string& localNodeId,
    std::shared_ptr<ProcessingCore> processingCore)
    : m_queueChannel(queueChannel)
    , m_context(std::move(context))
    , m_localNodeId(localNodeId)
    , m_processingCore(processingCore)
    , m_dltQueueResponseTimeout(*m_context.get())
    , m_queueResponseTimeout(boost::posix_time::seconds(5))
{
}

void ProcessingSubTaskQueue::CreateQueue(const SGProcessing::Task& task)
{
    using SubTaskList = ProcessingCore::SubTaskList;

    auto timestamp = std::chrono::system_clock::now();

    SubTaskList subTasks;
    m_processingCore->SplitTask(task, subTasks);

    std::lock_guard<std::mutex> guard(m_queueMutex);
    m_queue = std::make_shared<SGProcessing::SubTaskQueue>();
    for (auto itSubTask = subTasks.begin(); itSubTask != subTasks.end(); ++itSubTask)
    {
        auto item = m_queue->add_items();
        item->set_allocated_subtask(itSubTask->release());
    }

    // Set queue owner
    m_queue->set_owner_node_id(m_localNodeId);
    m_queue->set_last_update_timestamp(timestamp.time_since_epoch().count());

    PublishSubTaskQueue();
}

bool ProcessingSubTaskQueue::UpdateQueue(SGProcessing::SubTaskQueue* queue)
{
    if (queue)
    {
        std::shared_ptr<SGProcessing::SubTaskQueue> pQueue(queue);
        if (!m_queue
            || (m_queue->last_update_timestamp() < queue->last_update_timestamp()))
        {
            std::string nodeId = queue->owner_node_id();
            m_queue.swap(pQueue);

            LogQueue();

            return true;
        }
    }
    return false;
}

void ProcessingSubTaskQueue::LockSubTask()
{
    // The method has to be called in scoped lock of queue mutex
    if (!m_onSubTaskGrabbedCallbacks.empty())
    {
        auto items = m_queue->mutable_items();
        for (int itemIdx = 0; itemIdx < items->size(); ++itemIdx)
        {
            if (items->Get(itemIdx).lock_node_id().empty())
            {
                auto timestamp = std::chrono::system_clock::now();
                auto item = items->Mutable(itemIdx);
                item->set_lock_node_id(m_localNodeId);
                item->set_lock_timestamp(timestamp.time_since_epoch().count());
                m_queue->set_last_update_timestamp(timestamp.time_since_epoch().count());

                LogQueue();
                PublishSubTaskQueue();

                m_onSubTaskGrabbedCallbacks.front()({ item->subtask() });
                m_onSubTaskGrabbedCallbacks.pop_front();

                if (m_onSubTaskGrabbedCallbacks.empty())
                {
                    break;
                }
            }
        }

        // No available subtasks found
        while (!m_onSubTaskGrabbedCallbacks.empty())
        {
            // Let the requester know that there are no available subtasks
            m_onSubTaskGrabbedCallbacks.front()({});
            // Reset the callback
            m_onSubTaskGrabbedCallbacks.pop_front();
        }
    }
}

void ProcessingSubTaskQueue::GrabSubTask(SubTaskGrabbedCallback onSubTaskGrabbedCallback)
{
    std::lock_guard<std::mutex> guard(m_queueMutex);
    m_onSubTaskGrabbedCallbacks.push_back(std::move(onSubTaskGrabbedCallback));
    if (HasOwnershipUnlocked())
    {
        LockSubTask();
    }
    else
    {
        // Send a request to grab a subtask queue
        SGProcessing::ProcessingChannelMessage message;
        auto request = message.mutable_subtask_queue_request();
        request->set_node_id(m_localNodeId);
        m_queueChannel->Publish(message.SerializeAsString());
    }
}

bool ProcessingSubTaskQueue::MoveOwnershipTo(const std::string& nodeId)
{
    std::lock_guard<std::mutex> guard(m_queueMutex);
    if (HasOwnershipUnlocked())
    {
        ChangeOwnershipTo(nodeId);
        return true;
    }
    return false;
}

void ProcessingSubTaskQueue::ChangeOwnershipTo(const std::string& nodeId)
{
    auto timestamp = std::chrono::system_clock::now();
    m_queue->set_last_update_timestamp(timestamp.time_since_epoch().count());
    m_queue->set_owner_node_id(nodeId);

    LogQueue();
    PublishSubTaskQueue();

    if (HasOwnershipUnlocked())
    {
        LockSubTask();
    }
}

bool ProcessingSubTaskQueue::RollbackOwnership()
{
    if (HasOwnershipUnlocked())
    {
        // Do nothing. The node is already the queue owner
        return true;
    }

    // Find the current queue owner
    auto ownerNodeId = m_queue->owner_node_id();
    int ownerNodeIdx = -1;
    for (int itemIdx = 0; itemIdx < m_queue->items_size(); ++itemIdx)
    {
        if (m_queue->items(itemIdx).lock_node_id() == ownerNodeId)
        {
            ownerNodeIdx = itemIdx;
            break;
        }
    }

    if (ownerNodeIdx >= 0)
    {
        // Check if the local node is the previous queue owner
        for (int idx = 1; idx < m_queue->items_size(); ++idx)
        {
            // Loop cyclically over queue items in backward direction starting from item[ownerNodeIdx - 1]
            // and excluding the item[ownerNodeIdx]
            int itemIdx = (m_queue->items_size() + ownerNodeIdx - idx) % m_queue->items_size();
            if (!m_queue->items(itemIdx).lock_node_id().empty())
            {
                if (m_queue->items(itemIdx).lock_node_id() == m_localNodeId)
                {
                    // The local node is the previous queue owner
                    ChangeOwnershipTo(m_localNodeId);
                    return true;
                }
                else
                {
                    // Another node should take the ownership
                    return false;
                }
            }
        }
    }
    else
    {
        // The queue owner didn't lock any subtask
        // Find the last locked item
        for (int idx = 1; idx <= m_queue->items_size(); ++idx)
        {
            int itemIdx = (m_queue->items_size() - idx);
            if (!m_queue->items(itemIdx).lock_node_id().empty())
            {
                if (m_queue->items(itemIdx).lock_node_id() == m_localNodeId)
                {
                    // The local node is the last locked item
                    ChangeOwnershipTo(m_localNodeId);
                    return true;
                }
                else
                {
                    // Another node should take the ownership
                    return false;
                }
            }
        }
    }

    // No locked items found
    // Allow the local node to take the ownership
    ChangeOwnershipTo(m_localNodeId);
    return true;
}

bool ProcessingSubTaskQueue::HasOwnership() const
{
    std::lock_guard<std::mutex> guard(m_queueMutex);
    return HasOwnershipUnlocked();
}

bool ProcessingSubTaskQueue::HasOwnershipUnlocked() const
{
    return (m_queue && m_queue->owner_node_id() == m_localNodeId);
}

void ProcessingSubTaskQueue::PublishSubTaskQueue() const
{
    SGProcessing::ProcessingChannelMessage message;
    message.set_allocated_subtask_queue(m_queue.get());
    std::string nodeId = m_queue->owner_node_id();
    m_queueChannel->Publish(message.SerializeAsString());
    message.release_subtask_queue();
    m_logger->debug("QUEUE_PUBLISHED");
}

bool ProcessingSubTaskQueue::ProcessSubTaskQueueMessage(SGProcessing::SubTaskQueue* queue)
{
    std::lock_guard<std::mutex> guard(m_queueMutex);
    m_dltQueueResponseTimeout.expires_at(boost::posix_time::pos_infin);

    bool queueChanged = UpdateQueue(queue);
    if (queueChanged && HasOwnershipUnlocked())
    {
        LockSubTask();
    }
    return queueChanged;
}

bool ProcessingSubTaskQueue::ProcessSubTaskQueueRequestMessage(
    const SGProcessing::SubTaskQueueRequest& request)
{
    std::lock_guard<std::mutex> guard(m_queueMutex);
    if (HasOwnershipUnlocked())
    {
        ChangeOwnershipTo(request.node_id());
        return true;
    }

    m_logger->debug("QUEUE_REQUEST_RECEIVED");
    m_dltQueueResponseTimeout.expires_from_now(m_queueResponseTimeout);
    m_dltQueueResponseTimeout.async_wait(std::bind(
        &ProcessingSubTaskQueue::HandleQueueRequestTimeout, this, std::placeholders::_1));

    return false;
}

void ProcessingSubTaskQueue::HandleQueueRequestTimeout(const boost::system::error_code& ec)
{
    if (ec != boost::asio::error::operation_aborted)
    {
        std::lock_guard<std::mutex> guard(m_queueMutex);
        m_logger->debug("QUEUE_REQUEST_TIMEOUT");
        m_dltQueueResponseTimeout.expires_at(boost::posix_time::pos_infin);
        RollbackOwnership();
    }
}

std::unique_ptr<SGProcessing::SubTaskQueue> ProcessingSubTaskQueue::GetQueueSnapshot() const
{
    auto queue = std::make_unique<SGProcessing::SubTaskQueue>();

    std::lock_guard<std::mutex> guard(m_queueMutex);
    if (m_queue)
    {
        queue->CopyFrom(*m_queue.get());
    }
    return std::move(queue);
}

void ProcessingSubTaskQueue::LogQueue() const
{
    if (m_logger->level() <= spdlog::level::trace)
    {
        std::stringstream ss;
        ss << "{";
        ss << "\"owner_node_id\":\"" << m_queue->owner_node_id() << "\"";
        ss << "," << "\"last_update_timestamp\":" << m_queue->last_update_timestamp();
        ss << "," << "\"items\":[";
        for (int itemIdx = 0; itemIdx < m_queue->items_size(); ++itemIdx)
        {
            auto item = m_queue->items(itemIdx);
            ss << "{\"lock_node_id\":\"" << item.lock_node_id() << "\"";
            ss << ",\"set_lock_timestamp\":" << item.lock_timestamp() << "},";
        }
        ss << "]}";

        m_logger->trace(ss.str());
    }
}

////////////////////////////////////////////////////////////////////////////////