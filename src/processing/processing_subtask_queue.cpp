#include "processing_subtask_queue.hpp"

namespace sgns::processing
{
////////////////////////////////////////////////////////////////////////////////
namespace
{
    class SubTaskQueueDataView : public SharedQueueDataView
    {
    public:
        SubTaskQueueDataView(
            std::shared_ptr<SGProcessing::SubTaskQueue> queue,
            const std::map<std::string, SGProcessing::SubTaskResult>& results);

        const std::string& GetQueueOwnerNodeId() const override;
        void SetQueueOwnerNodeId(const std::string& nodeId) const override;

        std::chrono::system_clock::time_point GetQueueLastUpdateTimestamp() const override;
        void SetQueueLastUpdateTimestamp(std::chrono::system_clock::time_point ts) const override;

        const std::string& GetItemLockNodeId(size_t itemIdx) const override;
        void SetItemLockNodeId(size_t itemIdx, const std::string& nodeId) const override;

        std::chrono::system_clock::time_point GetItemLockTimestamp(size_t itemIdx) const override;
        void SetItemLockTimestamp(size_t itemIdx, std::chrono::system_clock::time_point ts) const override;

        bool IsExcluded(size_t itemIdx) const override;

        size_t Size() const override;

    private:
        std::shared_ptr<SGProcessing::SubTaskQueue> m_queue;
        const std::map<std::string, SGProcessing::SubTaskResult>& m_results;
    };

    SubTaskQueueDataView::SubTaskQueueDataView(
        std::shared_ptr<SGProcessing::SubTaskQueue> queue,
        const std::map<std::string, SGProcessing::SubTaskResult>& results)
        : m_queue(queue)
        , m_results(results)
    {
    }

    const std::string& SubTaskQueueDataView::GetQueueOwnerNodeId() const
    {
        return m_queue->owner_node_id();
    }

    void SubTaskQueueDataView::SetQueueOwnerNodeId(const std::string& nodeId) const
    {
        m_queue->set_owner_node_id(nodeId);
    }

    std::chrono::system_clock::time_point SubTaskQueueDataView::GetQueueLastUpdateTimestamp() const
    {
        return std::chrono::system_clock::time_point(
            std::chrono::system_clock::duration(m_queue->last_update_timestamp()));
    }

    void SubTaskQueueDataView::SetQueueLastUpdateTimestamp(std::chrono::system_clock::time_point ts) const
    {
        m_queue->set_last_update_timestamp(ts.time_since_epoch().count());
    }

    const std::string& SubTaskQueueDataView::GetItemLockNodeId(size_t itemIdx) const
    {
        return m_queue->items(itemIdx).lock_node_id();
    }

    void SubTaskQueueDataView::SetItemLockNodeId(size_t itemIdx, const std::string& nodeId) const
    {
        return m_queue->mutable_items(itemIdx)->set_lock_node_id(nodeId);
    }

    std::chrono::system_clock::time_point SubTaskQueueDataView::GetItemLockTimestamp(size_t itemIdx) const
    {
        return std::chrono::system_clock::time_point(
            std::chrono::system_clock::duration(m_queue->items(itemIdx).lock_timestamp()));
    }

    void SubTaskQueueDataView::SetItemLockTimestamp(size_t itemIdx, std::chrono::system_clock::time_point ts) const
    {
        return m_queue->mutable_items(itemIdx)->set_lock_timestamp(ts.time_since_epoch().count());
    }

    bool SubTaskQueueDataView::IsExcluded(size_t itemIdx) const
    {
        return (m_results.find(m_queue->items(itemIdx).subtask().results_channel()) != m_results.end());
    }

    size_t SubTaskQueueDataView::Size() const
    {
        return m_queue->items_size();
    }
}

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
    , m_dltGrabSubTaskTimeout(*m_context.get())
    , m_grabSubTaskTimeout(boost::posix_time::seconds(10))
    , m_sharedQueue(localNodeId)
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

    m_sharedQueue.CreateQueue(std::make_shared<SubTaskQueueDataView>(m_queue, m_results));
    PublishSubTaskQueue();
}

bool ProcessingSubTaskQueue::UpdateQueue(SGProcessing::SubTaskQueue* pQueue)
{
    if (pQueue)
    {
        std::shared_ptr<SGProcessing::SubTaskQueue> queue(pQueue);
        if (m_sharedQueue.UpdateQueue(std::make_shared<SubTaskQueueDataView>(queue, m_results)))
        {
            m_queue.swap(queue);
            LogQueue();
            return true;
        }
    }
    return false;
}

void ProcessingSubTaskQueue::ProcessPendingSubTaskGrabbing()
{
    // The method has to be called in scoped lock of queue mutex
    m_dltGrabSubTaskTimeout.expires_at(boost::posix_time::pos_infin);
    while (!m_onSubTaskGrabbedCallbacks.empty())
    {
        size_t itemIdx;
        if (m_sharedQueue.GrabItem(itemIdx))
        {
            LogQueue();
            PublishSubTaskQueue();

            m_onSubTaskGrabbedCallbacks.front()({ m_queue->items(itemIdx).subtask() });
            m_onSubTaskGrabbedCallbacks.pop_front();
        }
        else
        {
            break;
        }
    }

    if (!m_onSubTaskGrabbedCallbacks.empty())
    {
        if (m_results.size() < m_queue->items_size())
        {
            // Wait for subtasks are processed
            m_dltGrabSubTaskTimeout.expires_from_now(m_grabSubTaskTimeout);
            m_dltGrabSubTaskTimeout.async_wait(std::bind(
                &ProcessingSubTaskQueue::HandleGrabSubTaskTimeout, this, std::placeholders::_1));
        }
        else
        {
            while (!m_onSubTaskGrabbedCallbacks.empty())
            {
                // Let the requester know that there are no available subtasks
                m_onSubTaskGrabbedCallbacks.front()({});
                // Reset the callback
                m_onSubTaskGrabbedCallbacks.pop_front();
            }
        }
    }
}

void ProcessingSubTaskQueue::HandleGrabSubTaskTimeout(const boost::system::error_code& ec)
{
    if (ec != boost::asio::error::operation_aborted)
    {
        std::lock_guard<std::mutex> guard(m_queueMutex);
        m_dltGrabSubTaskTimeout.expires_at(boost::posix_time::pos_infin);
        if (!m_onSubTaskGrabbedCallbacks.empty()
            && (m_results.size() < m_queue->items_size()))
        {
            GrabSubTasks();
        }
    }
}

void ProcessingSubTaskQueue::GrabSubTask(SubTaskGrabbedCallback onSubTaskGrabbedCallback)
{
    std::lock_guard<std::mutex> guard(m_queueMutex);
    m_onSubTaskGrabbedCallbacks.push_back(std::move(onSubTaskGrabbedCallback));
    GrabSubTasks();
}

void ProcessingSubTaskQueue::GrabSubTasks()
{
    if (m_sharedQueue.HasOwnership())
    {
        ProcessPendingSubTaskGrabbing();
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
    if (m_sharedQueue.MoveOwnershipTo(nodeId))
    {
        LogQueue();
        PublishSubTaskQueue();
        return true;
    }
    return false;
}

bool ProcessingSubTaskQueue::HasOwnership() const
{
    std::lock_guard<std::mutex> guard(m_queueMutex);
    return m_sharedQueue.HasOwnership();
}

void ProcessingSubTaskQueue::PublishSubTaskQueue() const
{
    SGProcessing::ProcessingChannelMessage message;
    message.set_allocated_subtask_queue(m_queue.get());
    m_queueChannel->Publish(message.SerializeAsString());
    message.release_subtask_queue();
    m_logger->debug("QUEUE_PUBLISHED");
}

bool ProcessingSubTaskQueue::ProcessSubTaskQueueMessage(SGProcessing::SubTaskQueue* queue)
{
    std::lock_guard<std::mutex> guard(m_queueMutex);
    m_dltQueueResponseTimeout.expires_at(boost::posix_time::pos_infin);

    bool queueChanged = UpdateQueue(queue);
    if (queueChanged && m_sharedQueue.HasOwnership())
    {
        ProcessPendingSubTaskGrabbing();
    }
    return queueChanged;
}

bool ProcessingSubTaskQueue::ProcessSubTaskQueueRequestMessage(
    const SGProcessing::SubTaskQueueRequest& request)
{
    std::lock_guard<std::mutex> guard(m_queueMutex);
    if (m_sharedQueue.MoveOwnershipTo(request.node_id()))
    {
        LogQueue();
        PublishSubTaskQueue();
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
        if (m_sharedQueue.RollbackOwnership())
        {
            LogQueue();
            PublishSubTaskQueue();

            if (m_sharedQueue.HasOwnership())
            {
                ProcessPendingSubTaskGrabbing();
            }
        }
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

void ProcessingSubTaskQueue::AddSubTaskResult(
    const std::string& resultChannel, const SGProcessing::SubTaskResult& subTaskResult)
{
    std::lock_guard<std::mutex> guard(m_queueMutex);
    SGProcessing::SubTaskResult result;
    result.CopyFrom(subTaskResult);
    m_results.insert(std::make_pair(resultChannel, std::move(result)));
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
            ss << ",\"lock_timestamp\":" << item.lock_timestamp() << "},";
        }
        ss << "]}";

        m_logger->trace(ss.str());
    }
}
}

////////////////////////////////////////////////////////////////////////////////