#include "SGProcessing.pb.h"

#include <libp2p/multi/multibase_codec/multibase_codec_impl.hpp>
#include <crdt/globaldb/globaldb.hpp>

#include <iostream>
#include <boost/program_options.hpp>
#include <boost/format.hpp>

namespace
{  
    // cmd line options
    struct Options 
    {
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
                ;

            po::variables_map vm;
            po::store(parse_command_line(argc, argv, desc), vm);
            po::notify(vm);

            if (vm.count("help") != 0 || argc == 1) 
            {
                std::cerr << desc << "\n";
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
    

    const std::string processingGridChannel = "GRID_CHANNEL_ID";

    auto pubs = std::make_shared<sgns::ipfs_pubsub::GossipPubSub>();

    std::vector<std::string> pubsubBootstrapPeers;
    if (options->remote)
    {
        pubsubBootstrapPeers = std::move(std::vector({ *options->remote }));
    }

    pubs->Start(40001, pubsubBootstrapPeers);

    const size_t maximalNodesCount = 1;

    std::list<SGProcessing::Task> tasks;

    // Put a single task to Global DB
    // And wait for its processing
    SGProcessing::Task task;
    task.set_ipfs_block_id("IPFS_BLOCK_ID_1");
    task.set_block_len(1000);
    task.set_block_line_stride(2);
    task.set_block_stride(4);
    task.set_random_seed(0);
    task.set_results_channel("RESULT_CHANNEL_ID_1");
    tasks.push_back(std::move(task));

    auto io = std::make_shared<boost::asio::io_context>();

    sgns::crdt::GlobalDB globalDB(
        io, "CRDT.Datastore.TEST", "CRDT.Datastore.TEST.Channel");

    std::thread iothread([io]() { io->run(); });

    auto pubsub = std::make_shared<sgns::ipfs_pubsub::GossipPubSub>(globalDB.GetKeyPair().value());
    pubsub->Start(40001, pubsubBootstrapPeers);

    auto crdtOptions = sgns::crdt::CrdtOptions::DefaultOptions();

    globalDB.Start(pubsub, crdtOptions);
    auto dataStore = globalDB.GetDatastore();

    for (auto& task : tasks)
    {
        sgns::base::Buffer valueBuffer;
        valueBuffer.put(task.SerializeAsString());
        auto setKeyResult = dataStore->PutKey(sgns::crdt::HierarchicalKey("TASK_1"), valueBuffer);
        if (setKeyResult.has_failure())
        {
            std::cout << "Unable to put key-value to CRDT datastore." << std::endl;
        }

        // Check if data put
        auto getKeyResult = dataStore->GetKey(sgns::crdt::HierarchicalKey("TASK_1"));
        if (getKeyResult.has_failure())
        {
            std::cout << "Unable to find key in CRDT datastore"<< std::endl;
        }
        else
        {
            std::cout << "[" << "TASK_1" << "] -> " << std::endl;
            // getKeyResult.value().toString()
        }
    }

    // Gracefully shutdown on signal
    boost::asio::signal_set signals(*pubs->GetAsioContext(), SIGINT, SIGTERM);
    signals.async_wait(
        [&pubs](const boost::system::error_code&, int)
        {
            pubs->Stop();
        });

    pubs->Wait();

    return 0;
}

