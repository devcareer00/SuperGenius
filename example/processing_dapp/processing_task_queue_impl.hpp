#ifndef GRPC_FOR_SUPERGENIUS_PROCESSING_TASK_QUEUE_IMPL_HPP
#define GRPC_FOR_SUPERGENIUS_PROCESSING_TASK_QUEUE_IMPL_HPP

#include <processing/processing_task_queue.hpp>
#include <crdt/globaldb/globaldb.hpp>

#include <boost/format.hpp>

#include <optional>

namespace sgns::processing
{
    class ProcessingTaskQueueImpl : public ProcessingTaskQueue
    {
    public:
        ProcessingTaskQueueImpl(
            std::shared_ptr<sgns::crdt::GlobalDB> db)
            : m_db(db)
            , m_processingTimeout(std::chrono::seconds(10))
        {
        }

        bool GrabTask(std::string& grabbedTaskKey, SGProcessing::Task& task) override
        {
            m_logger->info("GRAB_TASK");

            auto queryTasks = m_db->QueryKeyValues("tasks");
            if (queryTasks.has_failure())
            {
                m_logger->info("Unable list tasks from CRDT datastore");
                return false;
            }

            std::set<std::string> lockedTasks;
            if (queryTasks.has_value())
            {
                m_logger->info("TASK_QUEUE_SIZE: {}", queryTasks.value().size());
                bool isTaskGrabbed = false;
                for (auto element : queryTasks.value())
                {
                    auto taskKey = m_db->KeyToString(element.first);
                    if (taskKey.has_value())
                    {
                        bool isTaskLocked = IsTaskLocked(taskKey.value());
                        m_logger->debug("TASK_QUEUE_ITEM: {}, LOCKED: {}", taskKey.value(), isTaskLocked);

                        if (!isTaskLocked)
                        {
                            if (task.ParseFromArray(element.second.data(), element.second.size()))
                            {
                                if (LockTask(taskKey.value()))
                                {
                                    m_logger->debug("TASK_LOCKED {}", taskKey.value());
                                    grabbedTaskKey = taskKey.value();
                                    return true;
                                }
                            }
                        }
                        else
                        {
                            m_logger->debug("TASK_PREVIOUSLY_LOCKED {}", taskKey.value());
                            lockedTasks.insert(taskKey.value());
                        }
                    }
                    else
                    {
                        m_logger->debug("Undable to convert a key to string");
                    }
                }

                // No task was grabbed so far
                for (auto lockedTask : lockedTasks)
                {
                    if (MoveExpiredTaskLock(lockedTask, task))
                    {
                        grabbedTaskKey = lockedTask;
                        return true;
                    }
                }

            }
            return false;
        }

        bool CompleteTask(const std::string& taskKey, const SGProcessing::TaskResult& taskResult) override
        {
            sgns::base::Buffer data;
            data.put(taskResult.SerializeAsString());

            auto transaction = m_db->BeginTransaction();
            transaction->AddToDelta(sgns::crdt::HierarchicalKey("task_results/" + taskKey), data);
            transaction->RemoveFromDelta(sgns::crdt::HierarchicalKey("lock_" + taskKey));
            transaction->RemoveFromDelta(sgns::crdt::HierarchicalKey(taskKey));

            auto res = transaction->PublishDelta();
            m_logger->debug("TASK_COMPLETED: {}", taskKey);
            return !res.has_failure();
        }

        bool IsTaskLocked(const std::string& taskKey)
        {
            auto lockData = m_db->Get(sgns::crdt::HierarchicalKey("lock_" + taskKey));
            return (!lockData.has_failure() && lockData.has_value());
        }

        bool LockTask(const std::string& taskKey)
        {
            auto timestamp = std::chrono::system_clock::now();

            SGProcessing::TaskLock lock;
            lock.set_task_id(taskKey);
            lock.set_lock_timestamp(timestamp.time_since_epoch().count());

            sgns::base::Buffer lockData;
            lockData.put(lock.SerializeAsString());

            auto res = m_db->Put(sgns::crdt::HierarchicalKey("lock_" + taskKey), lockData);
            return !res.has_failure();
        }

        bool MoveExpiredTaskLock(const std::string& taskKey, SGProcessing::Task& task)
        {
            auto timestamp = std::chrono::system_clock::now();

            auto lockData = m_db->Get(sgns::crdt::HierarchicalKey("lock_" + taskKey));
            if (!lockData.has_failure() && lockData.has_value())
            {
                // Check task expiration
                SGProcessing::TaskLock lock;
                if (lock.ParseFromArray(lockData.value().data(), lockData.value().size()))
                {
                    auto expirationTime =
                        std::chrono::system_clock::time_point(
                            std::chrono::system_clock::duration(lock.lock_timestamp())) + m_processingTimeout;
                    if (timestamp > expirationTime)
                    {
                        auto taskData = m_db->Get(taskKey);

                        if (!taskData.has_failure())
                        {
                            if (task.ParseFromArray(taskData.value().data(), taskData.value().size()))
                            {
                                if (LockTask(taskKey))
                                {
                                    return true;
                                }
                            }
                        }
                        else
                        {
                            m_logger->debug("Unable to find a task {}", taskKey);
                        }
                    }
                }
            }
            return false;
        }

    private:
        std::shared_ptr<sgns::crdt::GlobalDB> m_db;
        std::chrono::system_clock::duration m_processingTimeout;
        sgns::base::Logger m_logger = sgns::base::createLogger("ProcessingTaskQueueImpl");
    };

}

#endif // GRPC_FOR_SUPERGENIUS_PROCESSING_TASK_QUEUE_HPP
