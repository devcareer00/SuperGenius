#include <processing/processing_service.hpp>
#include <crdt/globaldb/keypair_file_storage.hpp>
#include <crdt/globaldb/globaldb.hpp>

#include <libp2p/multi/multibase_codec/multibase_codec_impl.hpp>

#include <boost/program_options.hpp>
#include <boost/format.hpp>

#include <iostream>

using namespace sgns::processing;

namespace
{
    class ProcessingCoreImpl : public ProcessingCore
    {
    public:
        ProcessingCoreImpl(size_t nSubtasks, size_t subTaskProcessingTime)
            : m_nSubtasks(nSubtasks)
            , m_subTaskProcessingTime(subTaskProcessingTime)
        {
        }

        void SplitTask(const SGProcessing::Task& task, SubTaskList& subTasks) override
        {
            for (size_t i = 0; i < m_nSubtasks; ++i)
            {
                auto subtask = std::make_unique<SGProcessing::SubTask>();
                subtask->set_ipfsblock(task.ipfs_block_id());
                subtask->set_results_channel((boost::format("%s_subtask_%d") % task.results_channel() % i).str());
                subTasks.push_back(std::move(subtask));
            }
        }

        void  ProcessSubTask(
            const SGProcessing::SubTask& subTask, SGProcessing::SubTaskResult& result,
            uint32_t initialHashCode) override 
        {
            std::cout << "SubTask processing started. " << subTask.results_channel() << std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(m_subTaskProcessingTime));
            std::cout << "SubTask processed. " << subTask.results_channel() << std::endl;
            result.set_ipfs_results_data_id((boost::format("%s_%s") % "RESULT_IPFS" % subTask.results_channel()).str());
        }
        
    private:
        size_t m_nSubtasks;
        size_t m_subTaskProcessingTime;
    };


    class ProcessingTaksQueueImpl : public ProcessingTaksQueue
    {
    public:
        ProcessingTaksQueueImpl(
            std::shared_ptr<sgns::crdt::GlobalDB> db)
            : m_db(db)
        {
        }

        bool GrabTask(SGProcessing::Task& task) override
        {
            m_logger->info("GRAB_TASK");

            auto queryKeyValues = m_db->QueryKeyValues("tasks");
            if (queryKeyValues.has_failure())
            {
                m_logger->info("Unable list keys from CRDT datastore");
                return false;
            }

            if (queryKeyValues.has_value())
            {
                m_logger->info("TASK_QUEUE_SIZE: {}", queryKeyValues.value().size());
                for (auto element : queryKeyValues.value())
                {
                    sgns::base::Buffer keyPrefix;
                    keyPrefix.put(element.first);

                    m_logger->info("TASK_QUEUE_ELEMENT_KEY: {}", keyPrefix.toString());
                    // @todo Check if the task is not locked
                    if (task.ParseFromArray(element.second.data(), element.second.size()))
                    {
                        return true;
                    }
                }   
            }
            return false;
        }

        bool CompleteTask(SGProcessing::TaskResult& taskResult) override
        {
            sgns::base::Buffer valueBuffer;
            valueBuffer.put(taskResult.SerializeAsString());
            auto res = m_db->Put(sgns::crdt::HierarchicalKey("task_results/" + taskResult.task_ipfs_block_id()), valueBuffer);

            return !res.has_failure();
        }

    private:
        std::shared_ptr<sgns::crdt::GlobalDB> m_db;

        sgns::base::Logger m_logger = sgns::base::createLogger("ProcessingTaksQueueImpl");
    };

    // cmd line options
    struct Options 
    {
        size_t serviceIndex = 0;
        size_t subTaskProcessingTime = 0; // ms
        size_t roomSize = 0;
        size_t disconnect = 0;
        size_t nSubTasks = 5;
        size_t channelListRequestTimeout = 5000;
        // optional remote peer to connect to
        std::optional<std::string> remote;
    };

    boost::optional<Options> parseCommandLine(int argc, char** argv) {
        namespace po = boost::program_options;
        try 
        {
            Options o;
            std::string remote;

            po::options_description desc("processing service options");
            desc.add_options()("help,h", "print usage message")
                ("remote,r", po::value(&remote), "remote service multiaddress to connect to")
                ("processingtime,p", po::value(&o.subTaskProcessingTime), "subtask processing time (ms)")
                ("roomsize,s", po::value(&o.roomSize), "subtask processing time (ms)")
                ("disconnect,d", po::value(&o.disconnect), "disconnect after (ms)")
                ("nsubtasks,n", po::value(&o.nSubTasks), "number of subtasks that task is split to")
                ("channellisttimeout,t", po::value(&o.channelListRequestTimeout), "chnnel list request timeout (ms)")
                ("serviceindex,i", po::value(&o.serviceIndex), "index of the service in computational grid (has to be a unique value)");

            po::variables_map vm;
            po::store(parse_command_line(argc, argv, desc), vm);
            po::notify(vm);

            if (vm.count("help") != 0 || argc == 1) 
            {
                std::cerr << desc << "\n";
                return boost::none;
            }

            if (o.serviceIndex == 0) 
            {
                std::cerr << "Service index should be > 0\n";
                return boost::none;
            }

            if (o.subTaskProcessingTime == 0)
            {
                std::cerr << "SubTask processing time should be > 0\n";
                return boost::none;
            }

            if (o.roomSize == 0)
            {
                std::cerr << "Processing room size should be > 0\n";
                return boost::none;
            }

            if (!remote.empty())
            {
                o.remote = remote;
            }

            return o;
        }
        catch (const std::exception& e) 
        {
            std::cerr << e.what() << std::endl;
        }
        return boost::none;
    }
}

int main(int argc, char* argv[])
{
    auto options = parseCommandLine(argc, argv);
    if (!options)
    {
        return 1;
    }

    auto loggerPubSub = libp2p::common::createLogger("GossipPubSub");
    //loggerPubSub->set_level(spdlog::level::trace);

    auto loggerProcessingEngine = libp2p::common::createLogger("ProcessingEngine");
    loggerProcessingEngine->set_level(spdlog::level::trace);

    auto loggerProcessingService = libp2p::common::createLogger("ProcessingService");
    loggerProcessingService->set_level(spdlog::level::trace);

    auto loggerProcessingQueue = libp2p::common::createLogger("ProcessingSubTaskQueue");
    loggerProcessingQueue->set_level(spdlog::level::debug);
    
    auto loggerGlobalDB = libp2p::common::createLogger("GlobalDB");
    loggerGlobalDB->set_level(spdlog::level::debug);

    auto loggerDAGSyncer = libp2p::common::createLogger("GraphsyncDAGSyncer");
    loggerDAGSyncer->set_level(spdlog::level::trace);

    auto loggerBroadcaster = libp2p::common::createLogger("PubSubBroadcasterExt");
    loggerBroadcaster->set_level(spdlog::level::debug);
    
    const std::string processingGridChannel = "GRID_CHANNEL_ID";

    auto pubsubKeyPath = (boost::format("CRDT.Datastore.TEST.%d/pubs_processor") % options->serviceIndex).str();
    auto pubs = std::make_shared<sgns::ipfs_pubsub::GossipPubSub>(
        sgns::crdt::KeyPairFileStorage(pubsubKeyPath).GetKeyPair().value());

    std::vector<std::string> pubsubBootstrapPeers;
    if (options->remote)
    {
        pubsubBootstrapPeers = std::move(std::vector({ *options->remote }));
    }
    pubs->Start(40001, pubsubBootstrapPeers);

    const size_t maximalNodesCount = 1;

    boost::asio::deadline_timer timerToDisconnect(*pubs->GetAsioContext());
    if (options->disconnect > 0)
    {
        timerToDisconnect.expires_from_now(boost::posix_time::milliseconds(options->disconnect));
        timerToDisconnect.async_wait([pubs, &timerToDisconnect](const boost::system::error_code& error)
        {
            timerToDisconnect.expires_at(boost::posix_time::pos_infin);
            pubs->Stop();
        });
    }

    auto io = std::make_shared<boost::asio::io_context>();
    auto globalDB = std::make_shared<sgns::crdt::GlobalDB>(
        io, 
        (boost::format("CRDT.Datastore.TEST.%d") %  options->serviceIndex).str(), 
        40010 + options->serviceIndex,
        std::make_shared<sgns::ipfs_pubsub::GossipPubSubTopic>(pubs, "CRDT.Datastore.TEST.Channel"));

    auto crdtOptions = sgns::crdt::CrdtOptions::DefaultOptions();
    auto initRes = globalDB->Init(crdtOptions);

    std::thread iothread([io]() { io->run(); });

    auto taskQueue = std::make_shared<ProcessingTaksQueueImpl>(globalDB);

    auto processingCore = std::make_shared<ProcessingCoreImpl>(options->nSubTasks, options->subTaskProcessingTime);
    ProcessingServiceImpl processingService(pubs, maximalNodesCount, options->roomSize, taskQueue, processingCore);

    processingService.Listen(processingGridChannel);
    processingService.SetChannelListRequestTimeout(boost::posix_time::milliseconds(options->channelListRequestTimeout));

    processingService.SendChannelListRequest();

    // Gracefully shutdown on signal
    boost::asio::signal_set signals(*pubs->GetAsioContext(), SIGINT, SIGTERM);
    signals.async_wait(
        [&pubs, &io](const boost::system::error_code&, int)
        {
            pubs->Stop();
            io->stop();
        });

    pubs->Wait();
    iothread.join();

    return 0;
}

