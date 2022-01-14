#include <processing/processing_engine.hpp>
#include <processing/processing_subtask_storage_impl.hpp>
#include <processing/processing_subtask_queue_channel_pubsub.hpp>

#include <gtest/gtest.h>

#include <libp2p/log/configurator.hpp>
#include <libp2p/log/logger.hpp>

#include <iostream>
#include <boost/functional/hash.hpp>

using namespace sgns::processing;

namespace
{
    class ProcessingCoreImpl : public ProcessingCore
    {
    public:
        ProcessingCoreImpl(size_t processingMillisec)
            : m_processingMillisec(processingMillisec)
        {
        }

        void SplitTask(const SGProcessing::Task& task, SubTaskList& subTasks) override
        {
            auto subtask = std::make_unique<SGProcessing::SubTask>();
            subTasks.push_back(std::move(subtask));
        }

        void ProcessSubTask(
            const SGProcessing::SubTask& subTask, SGProcessing::SubTaskResult& result,
            uint32_t initialHashCode) override 
        {
            if (m_processingMillisec > 0)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(m_processingMillisec));
            }

            auto itResultHashes = m_chunkResultHashes.find(subTask.results_channel());

            size_t subTaskResultHash = initialHashCode;
            for (int chunkIdx = 0; chunkIdx < subTask.chunkstoprocess_size(); ++chunkIdx)
            {
                size_t chunkHash = 0;
                if (itResultHashes != m_chunkResultHashes.end())
                {
                    chunkHash = itResultHashes->second[chunkIdx];
                }
                else
                {
                    const auto& chunk = subTask.chunkstoprocess(chunkIdx);
                    // Chunk result hash should be calculated
                    // Chunk data hash is calculated just as a stub
                    chunkHash = std::hash<std::string>{}(chunk.SerializeAsString());
                }

                result.add_chunk_hashes(chunkHash);
                boost::hash_combine(subTaskResultHash, chunkHash);
            }

            result.set_result_hash(subTaskResultHash);

            m_processedSubTasks.push_back(subTask);
            m_initialHashes.push_back(initialHashCode);
        };

        std::vector<SGProcessing::SubTask> m_processedSubTasks;
        std::vector<uint32_t> m_initialHashes;

        std::map<std::string, std::vector<size_t>> m_chunkResultHashes;
    private:
        size_t m_processingMillisec;
    };
}

const std::string logger_config(R"(
# ----------------
sinks:
  - name: console
    type: console
    color: true
groups:
  - name: processing_engine_test
    sink: console
    level: info
    children:
      - name: libp2p
      - name: Gossip
# ----------------
  )");
class ProcessingSubTaskStorageImplTest : public ::testing::Test
{
public:
    virtual void SetUp() override
    {
        // prepare log system
        auto logging_system = std::make_shared<soralog::LoggingSystem>(
            std::make_shared<soralog::ConfiguratorFromYAML>(
                // Original LibP2P logging config
                std::make_shared<libp2p::log::Configurator>(),
                // Additional logging config for application
                logger_config));
        logging_system->configure();

        libp2p::log::setLoggingSystem(logging_system);
        libp2p::log::setLevelOfGroup("processing_engine_test", soralog::Level::DEBUG);
    }
};

/**
 * @given A queue containing 2 subtasks
 * @when Subtasks are finished and chunk hashes are valid
 * @then Task finalization sink is called.
 */
TEST_F(ProcessingSubTaskStorageImplTest, TaskFinalization)
{
    auto pubs1 = std::make_shared<sgns::ipfs_pubsub::GossipPubSub>();;
    pubs1->Start(40001, {});

    auto queueChannel = std::make_shared<ProcessingSubTaskQueueChannelPubSub>(pubs1, "QUEUE_CHANNEL_ID");

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    auto processingCore = std::make_shared<ProcessingCoreImpl>(100);

    auto nodeId1 = "NODE_1";

    bool isTaskFinalized = false;
    ProcessingEngine engine1(pubs1, nodeId1, processingCore);
    processingCore->m_chunkResultHashes["RESULT_CHANNEL_ID1"] = { 0 };
    processingCore->m_chunkResultHashes["RESULT_CHANNEL_ID2"] = { 0 };

    SGProcessing::ProcessingChunk chunk1;
    chunk1.set_chunkid("CHUNK_1");
    chunk1.set_n_subchunks(1);
    chunk1.set_line_stride(1);
    chunk1.set_offset(0);
    chunk1.set_stride(1);
    chunk1.set_subchunk_height(10);
    chunk1.set_subchunk_width(10);

    auto queue = std::make_unique<SGProcessing::SubTaskQueue>();
    // Local queue wrapped owns the queue
    queue->mutable_processing_queue()->set_owner_node_id(nodeId1);
    {
        auto item = queue->mutable_processing_queue()->add_items();
        auto subTask = queue->add_subtasks();
        subTask->set_results_channel("RESULT_CHANNEL_ID1");
        auto chunk = subTask->add_chunkstoprocess();
        chunk->CopyFrom(chunk1);
    }
    {
        auto item = queue->mutable_processing_queue()->add_items();
        auto subTask = queue->add_subtasks();
        subTask->set_results_channel("RESULT_CHANNEL_ID2");
        auto chunk = subTask->add_chunkstoprocess();
        chunk->CopyFrom(chunk1);
    }

    auto processingQueueManager1 = std::make_shared<ProcessingSubTaskQueueManager>(
        queueChannel, pubs1->GetAsioContext(), nodeId1);
    processingQueueManager1->ProcessSubTaskQueueMessage(queue.release());

    auto subTaskStorage1 = std::make_shared<SubTaskStorageImpl>(processingQueueManager1,
        [&isTaskFinalized](const SGProcessing::TaskResult&) { isTaskFinalized = true; });

    engine1.StartQueueProcessing(subTaskStorage1);

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    pubs1->Stop();

    ASSERT_TRUE(isTaskFinalized);
}

/**
 * @given A queue containing 2 subtasks
 * @when Subtasks contains invalid chunk hashes
 * @then The subtasks processing is restarted.
 */
TEST_F(ProcessingSubTaskStorageImplTest, InvalidSubTasksRestart)
{
    auto pubs1 = std::make_shared<sgns::ipfs_pubsub::GossipPubSub>();;
    pubs1->Start(40001, {});

    auto queueChannel = std::make_shared<ProcessingSubTaskQueueChannelPubSub>(pubs1, "QUEUE_CHANNEL_ID");

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    // The processing core 1 has invalid chunk result hashes
    auto processingCore1 = std::make_shared<ProcessingCoreImpl>(500);
    processingCore1->m_chunkResultHashes["RESULT_CHANNEL_ID1"] = { 0 };
    processingCore1->m_chunkResultHashes["RESULT_CHANNEL_ID2"] = { 1 };


    // The processing core 2 has invalid chunk result hashes
    auto processingCore2 = std::make_shared<ProcessingCoreImpl>(100);
    processingCore2->m_chunkResultHashes["RESULT_CHANNEL_ID1"] = { 0 };
    processingCore2->m_chunkResultHashes["RESULT_CHANNEL_ID2"] = { 0 };

    auto nodeId1 = "NODE_1";
    auto nodeId2 = "NODE_2";

    SGProcessing::ProcessingChunk chunk1;
    chunk1.set_chunkid("CHUNK_1");
    chunk1.set_n_subchunks(1);
    chunk1.set_line_stride(1);
    chunk1.set_offset(0);
    chunk1.set_stride(1);
    chunk1.set_subchunk_height(10);
    chunk1.set_subchunk_width(10);

    auto queue = std::make_unique<SGProcessing::SubTaskQueue>();
    // Local queue wrapped owns the queue
    queue->mutable_processing_queue()->set_owner_node_id(nodeId1);
    {
        auto item = queue->mutable_processing_queue()->add_items();
        auto subTask = queue->add_subtasks();
        subTask->set_results_channel("RESULT_CHANNEL_ID1");
        auto chunk = subTask->add_chunkstoprocess();
        chunk->CopyFrom(chunk1);
    }
    {
        auto item = queue->mutable_processing_queue()->add_items();
        auto subTask = queue->add_subtasks();
        subTask->set_results_channel("RESULT_CHANNEL_ID2");
        auto chunk = subTask->add_chunkstoprocess();
        chunk->CopyFrom(chunk1);
    }

    auto processingQueueManager1 = std::make_shared<ProcessingSubTaskQueueManager>(
        queueChannel, pubs1->GetAsioContext(), nodeId1);
    processingQueueManager1->ProcessSubTaskQueueMessage(queue.release());

    bool isTaskFinalized1 = false;
    ProcessingEngine engine1(pubs1, nodeId1, processingCore1);

    bool isTaskFinalized2 = false;
    ProcessingEngine engine2(pubs1, nodeId2, processingCore2);

    auto subTaskStorage1 = std::make_shared<SubTaskStorageImpl>(processingQueueManager1,
        [&isTaskFinalized1](const SGProcessing::TaskResult&) { isTaskFinalized1 = true; });

    engine1.StartQueueProcessing(subTaskStorage1);

    // Wait for queue processing by node1
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    // No task finalization should be called when there are invalid chunk results
    ASSERT_FALSE(isTaskFinalized1);

    auto processingQueueManager2 = std::make_shared<ProcessingSubTaskQueueManager>(
        queueChannel, pubs1->GetAsioContext(), nodeId2);

    // Change queue owner
    SGProcessing::SubTaskQueueRequest queueRequest;
    queueRequest.set_node_id(nodeId2);
    auto updatedQueue = processingQueueManager1->ProcessSubTaskQueueRequestMessage(queueRequest);

    // Synchronize the queues
    processingQueueManager2->ProcessSubTaskQueueMessage(processingQueueManager1->GetQueueSnapshot().release());
    processingQueueManager1->ProcessSubTaskQueueMessage(processingQueueManager2->GetQueueSnapshot().release());
    engine1.StopQueueProcessing();

    // Wait for failed tasks expiration
    // @todo Automatically mark failed tasks as exired
    std::this_thread::sleep_for(std::chrono::milliseconds(10000));

    auto subTaskStorage2 = std::make_shared<SubTaskStorageImpl>(processingQueueManager2,
        [&isTaskFinalized2](const SGProcessing::TaskResult&) { isTaskFinalized2 = true; });

    engine2.StartQueueProcessing(subTaskStorage2);

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    pubs1->Stop();

    // Task should be finalized because chunks have valid hashes
    ASSERT_TRUE(isTaskFinalized2);
}
