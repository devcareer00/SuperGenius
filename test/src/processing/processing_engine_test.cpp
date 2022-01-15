#include <processing/processing_engine.hpp>
#include <processing/processing_subtask_storage.hpp>

#include <gtest/gtest.h>

#include <libp2p/log/configurator.hpp>
#include <libp2p/log/logger.hpp>

#include <iostream>
#include <boost/functional/hash.hpp>

using namespace sgns::processing;

namespace
{
    class SubTaskStorageMock : public SubTaskStorage
    {
    public:
        SubTaskStorageMock(boost::asio::io_context& context)
            : m_context(context)
        {
        }

        void GrabSubTask(SubTaskGrabbedCallback onSubTaskGrabbedCallback) override
        {
            if (!subTasks.empty())
            {
                m_context.post([this, onSubTaskGrabbedCallback]() {
                    onSubTaskGrabbedCallback(subTasks.front());
                    subTasks.pop_front();
                });
            }
        }

        void CompleteSubTask(const std::string& subTaskId, const SGProcessing::SubTaskResult& subTaskResult) override
        {
            // Do nothing
        }

        std::list<SGProcessing::SubTask> subTasks;

    private:
        boost::asio::io_context& m_context;
    };

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
class ProcessingEngineTest : public ::testing::Test
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
 * @given A node is subscribed to result channel 
 * @when A result is published to the channel
 * @then The node receives the result
 */
TEST_F(ProcessingEngineTest, SubscribtionToResultChannel)
{
    auto pubs1 = std::make_shared<sgns::ipfs_pubsub::GossipPubSub>();;
    pubs1->Start(40001, {});

    auto pubs2 = std::make_shared<sgns::ipfs_pubsub::GossipPubSub>();;
    pubs2->Start(40001, {pubs1->GetLocalAddress()});

    sgns::ipfs_pubsub::GossipPubSubTopic resultChannel(pubs1, "RESULT_CHANNEL_ID");
    resultChannel.Subscribe([](boost::optional<const sgns::ipfs_pubsub::GossipPubSub::Message&> message)
    {     
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    auto processingCore = std::make_shared<ProcessingCoreImpl>(0);

    auto nodeId = "NODE_1";
    ProcessingEngine engine(pubs1, nodeId, processingCore);

    // Empty storage used to prevent result publishing by the engine.
    auto subTaskStorage = std::make_shared<SubTaskStorageMock>(*pubs1->GetAsioContext());
    engine.StartQueueProcessing(subTaskStorage);

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Publish result to the results channel
    SGProcessing::SubTaskResult result;    
    result.set_subtaskid("SUBTASK_ID");
    resultChannel.Publish(result.SerializeAsString());

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    pubs1->Stop();
    pubs2->Stop();

    // No duplicates should be received
    ASSERT_EQ(1, engine.GetResults().size());
    EXPECT_EQ("SUBTASK_ID", std::get<0>(engine.GetResults()[0]));
}

/**
 * @given A queue containing subtasks
 * @when Processing is started
 * @then ProcessingCore::ProcessSubTask is called for each subtask.
 */
TEST_F(ProcessingEngineTest, SubTaskProcessing)
{
    auto pubs1 = std::make_shared<sgns::ipfs_pubsub::GossipPubSub>();;
    pubs1->Start(40001, {});

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    auto processingCore = std::make_shared<ProcessingCoreImpl>(0);

    auto nodeId = "NODE_1";
    ProcessingEngine engine(pubs1, nodeId, processingCore);

    auto subTaskStorage = std::make_shared<SubTaskStorageMock>(*pubs1->GetAsioContext());
    {
        SGProcessing::SubTask subTask;
        subTask.set_results_channel("RESULT_CHANNEL_ID1");
        subTaskStorage->subTasks.push_back(std::move(subTask));
    }
    {
        SGProcessing::SubTask subTask;
        subTask.set_results_channel("RESULT_CHANNEL_ID2");
        subTaskStorage->subTasks.push_back(std::move(subTask));
    }

    SGProcessing::SubTask subTask;
    subTask.set_results_channel("RESULT_CHANNEL_ID");

    engine.StartQueueProcessing(subTaskStorage);

    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    pubs1->Stop();

    ASSERT_EQ(2, processingCore->m_processedSubTasks.size());
    EXPECT_EQ("RESULT_CHANNEL_ID1", processingCore->m_processedSubTasks[0].results_channel());
    EXPECT_EQ("RESULT_CHANNEL_ID2", processingCore->m_processedSubTasks[1].results_channel());
}

/**
 * @given A queue containing 2 subtasks
 * @when 2 engines sequentually start the queue processing
 * @then Each of them processes only 1 subtask from the queue.
 */
TEST_F(ProcessingEngineTest, SharedSubTaskProcessing)
{
    auto pubs1 = std::make_shared<sgns::ipfs_pubsub::GossipPubSub>();;
    pubs1->Start(40001, {});

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    auto processingCore = std::make_shared<ProcessingCoreImpl>(500);

    auto nodeId1 = "NODE_1";
    auto nodeId2 = "NODE_2";

    ProcessingEngine engine1(pubs1, nodeId1, processingCore);
    ProcessingEngine engine2(pubs1, nodeId2, processingCore);

    auto subTaskStorage1 = std::make_shared<SubTaskStorageMock>(*pubs1->GetAsioContext());
    {
        SGProcessing::SubTask subTask;
        subTask.set_results_channel("RESULT_CHANNEL_ID1");
        subTaskStorage1->subTasks.push_back(std::move(subTask));
    }

    auto subTaskStorage2 = std::make_shared<SubTaskStorageMock>(*pubs1->GetAsioContext());
    {
        SGProcessing::SubTask subTask;
        subTask.set_results_channel("RESULT_CHANNEL_ID2");
        subTaskStorage2->subTasks.push_back(std::move(subTask));
    }

    engine1.StartQueueProcessing(subTaskStorage1);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    engine2.StartQueueProcessing(subTaskStorage2);
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    pubs1->Stop();

    ASSERT_EQ(2, processingCore->m_initialHashes.size());
    EXPECT_EQ(static_cast<uint32_t>(std::hash<std::string>{}(nodeId1)), processingCore->m_initialHashes[0]);
    EXPECT_EQ(static_cast<uint32_t>(std::hash<std::string>{}(nodeId2)), processingCore->m_initialHashes[1]);
}
