#include "processing_engine.hpp"

namespace sgns::processing
{
ProcessingEngine::ProcessingEngine(
    std::string nodeId,
    std::shared_ptr<ProcessingCore> processingCore)
    : m_nodeId(std::move(nodeId))
    , m_processingCore(processingCore)
{
}

ProcessingEngine::~ProcessingEngine()
{
    m_logger->debug("[RELEASED] this: {}", reinterpret_cast<size_t>(this));
}

void ProcessingEngine::StartQueueProcessing(
    std::shared_ptr<SubTaskQueueAccessor> subTaskQueueAccessor)
{
    std::lock_guard<std::mutex> queueGuard(m_mutexSubTaskQueue);
    m_subTaskQueueAccessor = subTaskQueueAccessor;

    m_subTaskQueueAccessor->GrabSubTask(
        std::bind(
            &ProcessingEngine::OnSubTaskGrabbed, 
            shared_from_this(), std::placeholders::_1));
}

void ProcessingEngine::StopQueueProcessing()
{
    std::lock_guard<std::mutex> queueGuard(m_mutexSubTaskQueue);
    m_subTaskQueueAccessor.reset();
    m_logger->debug("[PROCESSING_STOPPED] this: {}", reinterpret_cast<size_t>(this));
}

bool ProcessingEngine::IsQueueProcessingStarted() const
{
    std::lock_guard<std::mutex> queueGuard(m_mutexSubTaskQueue);
    return (m_subTaskQueueAccessor.get() != nullptr);
}

void ProcessingEngine::OnSubTaskGrabbed(boost::optional<const SGProcessing::SubTask&> subTask)
{
    if (subTask)
    {
        m_logger->debug("[GRABBED]. ({}).", subTask->subtaskid());
        ProcessSubTask(*subTask);
    }

    // When results for all subtasks are available, no subtask is received (optnull).
}

void ProcessingEngine::ProcessSubTask(SGProcessing::SubTask subTask)
{
    m_logger->debug("[PROCESSING_STARTED]. ({}).", subTask.subtaskid());
    std::thread thread([subTask(std::move(subTask)), _this(shared_from_this())]()
    {
        SGProcessing::SubTaskResult result;
        // @todo set initial hash code that depends on node id
        _this->m_processingCore->ProcessSubTask(
            subTask, result, std::hash<std::string>{}(_this->m_nodeId));
        // @todo replace results_channel with subtaskid
        result.set_subtaskid(subTask.subtaskid());
        _this->m_logger->debug("[PROCESSED]. ({}).", subTask.subtaskid());
        std::lock_guard<std::mutex> queueGuard(_this->m_mutexSubTaskQueue);
        if (_this->m_subTaskQueueAccessor)
        {
            _this->m_subTaskQueueAccessor->CompleteSubTask(subTask.subtaskid(), result);
            // @todo Should a new subtask be grabbed once the perivious one is processed?
            _this->m_subTaskQueueAccessor->GrabSubTask(
                std::bind(&ProcessingEngine::OnSubTaskGrabbed, _this, std::placeholders::_1));
        }
    });
    thread.detach();
}
}
