#include "processing_shared_queue.hpp"

namespace sgns::processing
{
////////////////////////////////////////////////////////////////////////////////
SharedQueue::SharedQueue(
    const std::string& localNodeId)
    : m_localNodeId(localNodeId)
{
}

void SharedQueue::CreateQueue(std::shared_ptr<SharedQueueDataView> queue)
{
    m_queue = std::move(queue);
    ChangeOwnershipTo(m_localNodeId);
}

bool SharedQueue::UpdateQueue(std::shared_ptr<SharedQueueDataView> queue)
{
    if (!m_queue
        || (m_queue->GetQueueLastUpdateTimestamp() < queue->GetQueueLastUpdateTimestamp()))
    {
        m_queue.swap(queue);
        LogQueue();
        return true;
    }
    return false;
}

bool SharedQueue::LockItem(size_t& lockedItemIndex)
{
    // The method has to be called in scoped lock of queue mutex
    for (int itemIdx = 0; itemIdx < m_queue->Size(); ++itemIdx)
    {
        if (!m_queue->IsExcluded(itemIdx) && m_queue->GetItemLockNodeId(itemIdx).empty())
        {
            auto timestamp = std::chrono::system_clock::now();

            m_queue->SetItemLockNodeId(itemIdx, m_localNodeId);
            m_queue->SetItemLockTimestamp(itemIdx, timestamp);

            m_queue->SetQueueLastUpdateTimestamp(timestamp);

            LogQueue();

            lockedItemIndex = itemIdx;
            return true;
        }
    }

    return false;
}

bool SharedQueue::GrabItem(size_t& grabbedItemIndex)
{
    if (HasOwnership())
    {
        return LockItem(grabbedItemIndex);
    }

    return false;
}

bool SharedQueue::MoveOwnershipTo(const std::string& nodeId)
{
    if (HasOwnership())
    {
        ChangeOwnershipTo(nodeId);
        return true;
    }
    return false;
}

void SharedQueue::ChangeOwnershipTo(const std::string& nodeId)
{
    auto timestamp = std::chrono::system_clock::now();
    m_queue->SetQueueOwnerNodeId(nodeId);
    m_queue->SetQueueLastUpdateTimestamp(timestamp);
    LogQueue();
}

bool SharedQueue::RollbackOwnership()
{
    if (HasOwnership())
    {
        // Do nothing. The node is already the queue owner
        return true;
    }

    // Find the current queue owner
    auto ownerNodeId = m_queue->GetQueueOwnerNodeId();
    int ownerNodeIdx = -1;
    for (int itemIdx = 0; itemIdx < m_queue->Size(); ++itemIdx)
    {
        if (m_queue->GetItemLockNodeId(itemIdx) == ownerNodeId)
        {
            ownerNodeIdx = itemIdx;
            break;
        }
    }

    if (ownerNodeIdx >= 0)
    {
        // Check if the local node is the previous queue owner
        for (int idx = 1; idx < m_queue->Size(); ++idx)
        {
            // Loop cyclically over queue items in backward direction starting from item[ownerNodeIdx - 1]
            // and excluding the item[ownerNodeIdx]
            int itemIdx = (m_queue->Size() + ownerNodeIdx - idx) % m_queue->Size();
            if (!m_queue->GetItemLockNodeId(itemIdx).empty())
            {
                if (m_queue->GetItemLockNodeId(itemIdx) == m_localNodeId)
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
        for (int idx = 1; idx <= m_queue->Size(); ++idx)
        {
            int itemIdx = (m_queue->Size() - idx);
            if (!m_queue->GetItemLockNodeId(itemIdx).empty())
            {
                if (m_queue->GetItemLockNodeId(itemIdx) == m_localNodeId)
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

bool SharedQueue::HasOwnership() const
{
    return (m_queue->GetQueueOwnerNodeId() == m_localNodeId);
}

bool SharedQueue::UnlockExpiredItems(std::chrono::system_clock::duration expirationTimeout)
{

    bool unlocked = false;

    if (HasOwnership())
    {
        auto timestamp = std::chrono::system_clock::now();
        for (int itemIdx = 0; itemIdx < m_queue->Size(); ++itemIdx)
        {
            // Check if a subtask is locked, expired and no result was obtained for it
            // @todo replace the result channel with subtask id to identify a subtask that should be unlocked
            if (!m_queue->GetItemLockNodeId(itemIdx).empty()
                && !m_queue->IsExcluded(itemIdx))
            {
                auto expirationTime = m_queue->GetItemLockTimestamp(itemIdx) + expirationTimeout;
                if (timestamp > expirationTime)
                {
                    // Unlock the item
                    m_queue->SetItemLockNodeId(itemIdx, "");
                    m_queue->SetItemLockTimestamp(itemIdx, std::chrono::system_clock::time_point::min());
                    unlocked = true;
                }
            }
        }
    }

    return unlocked;
}

std::chrono::system_clock::time_point SharedQueue::GetLastLockTimestamp() const
{
    std::chrono::system_clock::time_point lastLockTimestamp;

    for (int itemIdx = 0; itemIdx < m_queue->Size(); ++itemIdx)
    {
        // Check if an item is locked and no result was obtained for it
        if (!m_queue->GetItemLockNodeId(itemIdx).empty()
            && !m_queue->IsExcluded(itemIdx))
        {
            lastLockTimestamp = std::max(lastLockTimestamp, m_queue->GetItemLockTimestamp(itemIdx));
        }
    }

    return lastLockTimestamp;
}

void SharedQueue::LogQueue() const
{
    if (m_logger->level() <= spdlog::level::trace)
    {
        std::stringstream ss;
        ss << "{";
        ss << "\"owner_node_id\":\"" << m_queue->GetQueueOwnerNodeId() << "\"";
        ss << "," << "\"last_update_timestamp\":" << m_queue->GetQueueLastUpdateTimestamp().time_since_epoch().count();
        ss << "," << "\"items\":[";
        for (int itemIdx = 0; itemIdx < m_queue->Size(); ++itemIdx)
        {
            ss << "{\"lock_node_id\":\"" << m_queue->GetItemLockNodeId(itemIdx) << "\"";
            ss << ",\"lock_timestamp\":" << m_queue->GetItemLockTimestamp(itemIdx).time_since_epoch().count();
            ss << ",\"is_excluded\":" << m_queue->IsExcluded(itemIdx);
            ss << "},";
        }
        ss << "]}";

        m_logger->trace(ss.str());
    }
}
}

////////////////////////////////////////////////////////////////////////////////