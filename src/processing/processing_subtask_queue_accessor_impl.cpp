#include "processing_subtask_queue_accessor_impl.hpp"

namespace sgns::processing
{
SubTaskQueueAccessorImpl::SubTaskQueueAccessorImpl(
    std::shared_ptr<sgns::ipfs_pubsub::GossipPubSub> gossipPubSub,
    std::shared_ptr<ProcessingSubTaskQueueManager> subTaskQueueManager,
    std::shared_ptr<SubTaskStateStorage> subTaskStateStorage,
    std::shared_ptr<SubTaskResultStorage> subTaskResultStorage,
    std::function<void(const SGProcessing::TaskResult&)> taskResultProcessingSink)
    : m_gossipPubSub(gossipPubSub)
    , m_subTaskQueueManager(subTaskQueueManager)
    , m_subTaskStateStorage(subTaskStateStorage)
    , m_subTaskResultStorage(subTaskResultStorage)
    , m_taskResultProcessingSink(taskResultProcessingSink)
{
    // @todo replace hardcoded channel identified with an input value
    m_resultChannel = std::make_shared<ipfs_pubsub::GossipPubSubTopic>(m_gossipPubSub, "RESULT_CHANNEL_ID");
    m_logger->debug("[CREATED] this: {}, thread_id {}", reinterpret_cast<size_t>(this), std::this_thread::get_id());
}

SubTaskQueueAccessorImpl::~SubTaskQueueAccessorImpl()
{
    m_logger->debug("[RELEASED] this: {}, thread_id {}", reinterpret_cast<size_t>(this), std::this_thread::get_id());
}

void SubTaskQueueAccessorImpl::ConnectToSubTaskQueue(std::function<void()> onSubTaskQueueConnectedEventSink)
{
    m_subTaskQueueManager->SetSubTaskQueueAssignmentEventSink(
        std::bind(&SubTaskQueueAccessorImpl::OnSubTaskQueueAssigned, 
            this, std::placeholders::_1, std::placeholders::_2,  onSubTaskQueueConnectedEventSink));

    // It cannot be called in class constructor because shared_from_this doesn't work for the case
    // The weak_from_this() is required to prevent a case when the message processing callback
    // is called using an invalid 'this' pointer to destroyed object
    m_resultChannel->Subscribe(
        std::bind(
            &SubTaskQueueAccessorImpl::OnResultChannelMessage,
            weak_from_this(), std::placeholders::_1));
}

void SubTaskQueueAccessorImpl::AssignSubTasks(std::list<SGProcessing::SubTask>& subTasks)
{
    for (const auto& subTask : subTasks)
    {
        m_subTaskStateStorage->ChangeSubTaskState(
            subTask.subtaskid(), SGProcessing::SubTaskState::ENQUEUED);
    }
    m_subTaskQueueManager->CreateQueue(subTasks);
}

void SubTaskQueueAccessorImpl::UpdateResultsFromStorage(const std::vector<std::string>& subTaskIds)
{
    std::vector<SGProcessing::SubTaskResult> results;
    m_subTaskResultStorage->GetSubTaskResults(subTaskIds, results);

    m_logger->debug("[RESULTS_LOADED] {} results loaded from results storage", results.size());

    if (!results.empty())
    {
        for (auto& result : results)
        {
            m_results.emplace(result.subtaskid(), std::move(result));
        }
    }
}

void SubTaskQueueAccessorImpl::OnSubTaskQueueAssigned(
    const std::vector<std::string>& subTaskIds, std::set<std::string>& processedSubTaskIds,
     std::function<void()> onSubTaskQueueConnectedEventSink)
{
    std::lock_guard<std::mutex> guard(m_mutexResults);
    UpdateResultsFromStorage(subTaskIds);

    for (const auto& [subTaskId, result]: m_results)
    {
        processedSubTaskIds.insert(subTaskId);
    }

    // Call it asynchronously finalize initialization
    m_gossipPubSub->GetAsioContext()->post([onSubTaskQueueConnectedEventSink]() {
        onSubTaskQueueConnectedEventSink();
        });
}

void SubTaskQueueAccessorImpl::GrabSubTask(SubTaskGrabbedCallback onSubTaskGrabbedCallback)
{
    m_subTaskQueueManager->GrabSubTask(onSubTaskGrabbedCallback);
}

void SubTaskQueueAccessorImpl::CompleteSubTask(const std::string& subTaskId, const SGProcessing::SubTaskResult& subTaskResult)
{
    m_subTaskResultStorage->AddSubTaskResult(subTaskResult);
    m_subTaskStateStorage->ChangeSubTaskState(
        subTaskId, SGProcessing::SubTaskState::PROCESSED);

    m_resultChannel->Publish(subTaskResult.SerializeAsString());
    m_logger->debug("[RESULT_SENT]. ({}).", subTaskId);
}

void SubTaskQueueAccessorImpl::OnResultReceived(SGProcessing::SubTaskResult&& subTaskResult)
{
    std::string subTaskId = subTaskResult.subtaskid();

    // Results accumulation
    std::lock_guard<std::mutex> guard(m_mutexResults);
    m_results.emplace(subTaskId, std::move(subTaskResult));

    m_subTaskQueueManager->ChangeSubTaskProcessingStates({ subTaskId }, true);

    // Loading results from results storage
    // @todo Check if the action needs to be executed by timer or add some kind of subscription to results data in 
    // CRDT datatore
    if (!m_subTaskQueueManager->IsProcessed())
    {
        const auto& queue = m_subTaskQueueManager->GetQueueSnapshot();
        std::vector<std::string> subTaskIds;
        // @todo optimized the loop
        // Instead of iterating over the whole subtask list keep pending results id list
        // m_results U pending results = full ids set
        for (size_t subTaskIdx = 0; subTaskIdx < queue->subtasks().items_size(); ++subTaskIdx)
        {
            const auto& subTask = queue->subtasks().items(subTaskIdx);
            subTaskIds.push_back(subTask.subtaskid());
        }

        UpdateResultsFromStorage(subTaskIds);

        std::set<std::string> processedSubTaskIds;
        for (const auto& [subTaskId, result] : m_results)
        {
            processedSubTaskIds.insert(result.subtaskid());
            m_results.emplace(result.subtaskid(), std::move(result));
        }
        m_subTaskQueueManager->ChangeSubTaskProcessingStates(processedSubTaskIds, true);

    }

    // Task processing finished
    if (m_subTaskQueueManager->IsProcessed()) 
    {
        std::set<std::string> invalidSubTaskIds;
        auto queue = m_subTaskQueueManager->GetQueueSnapshot();

        if (!FinalizeQueueProcessing(queue->subtasks(), invalidSubTaskIds))
        {
            m_subTaskQueueManager->ChangeSubTaskProcessingStates(invalidSubTaskIds, false);
        }
    }
}

bool SubTaskQueueAccessorImpl::FinalizeQueueProcessing(
    const SGProcessing::SubTaskCollection& subTasks,
    std::set<std::string>& invalidSubTaskIds)
{
    bool valid = m_validationCore.ValidateResults(subTasks, m_results, invalidSubTaskIds);

    bool isFinalized = false;
    m_logger->debug("RESULTS_VALIDATED: {}", valid ? "VALID" : "INVALID");
    if (valid)
    {
        // @todo Add a test where the owner disconnected, but the last valid result is received by slave nodes
        // @todo Request the ownership instead of just checking
        if (m_subTaskQueueManager->HasOwnership())
        {
            SGProcessing::TaskResult taskResult;
            auto results = taskResult.mutable_subtask_results();
            for (const auto& r : m_results)
            {
                auto result = results->Add();
                result->CopyFrom(r.second);
            }
            m_taskResultProcessingSink(taskResult);
        }
        else
        {
            // @todo Process task finalization expiration
        }
        isFinalized = true;
    }
    else
    {
        for (const auto& subTaskId : invalidSubTaskIds)
        {
            m_results.erase(subTaskId);
        }
    }
    return isFinalized;
}

std::vector<std::tuple<std::string, SGProcessing::SubTaskResult>> SubTaskQueueAccessorImpl::GetResults() const
{
    std::lock_guard<std::mutex> guard(m_mutexResults);
    std::vector<std::tuple<std::string, SGProcessing::SubTaskResult>> results;
    for (auto& item : m_results)
    {
        results.push_back({ item.first, item.second });
    }
    std::sort(results.begin(), results.end(),
        [](const std::tuple<std::string, SGProcessing::SubTaskResult>& v1,
            const std::tuple<std::string, SGProcessing::SubTaskResult>& v2) { return std::get<0>(v1) < std::get<0>(v2); });

    return results;
}

void SubTaskQueueAccessorImpl::OnResultChannelMessage(
    std::weak_ptr<SubTaskQueueAccessorImpl> weakThis,
    boost::optional<const sgns::ipfs_pubsub::GossipPubSub::Message&> message)
{
    auto _this = weakThis.lock();
    if (!_this)
    {
        return;
    }
    
    if (message)
    {
        SGProcessing::SubTaskResult result;
        if (result.ParseFromArray(message->data.data(), static_cast<int>(message->data.size())))
        {
            _this->m_logger->debug("[RESULT_RECEIVED]. ({}).", result.subtaskid());

            _this->OnResultReceived(std::move(result));
        }
    }
}
}
