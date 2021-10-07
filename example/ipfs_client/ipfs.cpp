#include <libp2p/multi/multibase_codec/multibase_codec_impl.hpp>
#include <libp2p/injector/host_injector.hpp>
#include <libp2p/injector/kademlia_injector.hpp>
#include <libp2p/host/host.hpp>
#include <libp2p/protocol/common/asio/asio_scheduler.hpp>
#include <libp2p/multi/content_identifier_codec.hpp>

#include <ipfs_lite/ipfs/graphsync/graphsync.hpp>
#include <ipfs_lite/ipfs/graphsync/impl/graphsync_impl.hpp>
#include <ipfs_lite/ipld/impl/ipld_node_impl.hpp>

#include <boost/program_options.hpp>
#include <boost/format.hpp>
#include <boost/di/extension/scopes/shared.hpp>

#include <libp2p/log/configurator.hpp>
#include <libp2p/log/logger.hpp>

#include <iostream>

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

            po::options_description desc("ipfs service options");
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
    //auto options = parseCommandLine(argc, argv);
    //if (!options)
    //{
    //    return EXIT_FAILURE;
    //}

    const std::string logger_config(R"(
    # ----------------
    sinks:
      - name: console
        type: console
        color: true
    groups:
      - name: main
        sink: console
        level: info
        children:
          - name: libp2p
          - name: kademlia
    # ----------------
    )");

    // prepare log system
    auto logging_system = std::make_shared<soralog::LoggingSystem>(
        std::make_shared<soralog::ConfiguratorFromYAML>(
            // Original LibP2P logging config
            std::make_shared<libp2p::log::Configurator>(),
            // Additional logging config for application
            logger_config));
    logging_system->configure();

    libp2p::log::setLoggingSystem(logging_system);

    auto loggerDAGSyncer = libp2p::log::createLogger("GraphsyncDAGSyncer");
    loggerDAGSyncer->setLevel(soralog::Level::TRACE);

    const std::string processingGridChannel = "GRID_CHANNEL_ID";

    libp2p::protocol::kademlia::Config kademlia_config;
    kademlia_config.randomWalk.enabled = true;
    kademlia_config.randomWalk.interval = std::chrono::seconds(300);
    kademlia_config.requestConcurency = 20;

    auto injector = libp2p::injector::makeHostInjector(
        // libp2p::injector::useKeyPair(kp), // Use predefined keypair
        libp2p::injector::makeKademliaInjector(
            libp2p::injector::useKademliaConfig(kademlia_config)));

    try
    {
        auto ma = libp2p::multi::Multiaddress::create("/ip4/127.0.0.1/tcp/40000").value();  // NOLINT

        auto io = injector.create<std::shared_ptr<boost::asio::io_context>>();
        auto host = injector.create<std::shared_ptr<libp2p::Host>>();

        auto kademlia =
            injector
            .create<std::shared_ptr<libp2p::protocol::kademlia::Kademlia>>();

        auto bootstrap_nodes = [] {
            std::vector<std::string> addresses = {
                "/ip4/202.61.244.123/tcp/4001/p2p/QmUikGKv3RysyCYZxNoGWjf8Y1qPjhrp8NeV4fYyNyqcDQ",
            };
            std::unordered_map<libp2p::peer::PeerId,
                std::vector<libp2p::multi::Multiaddress>>
                addresses_by_peer_id;

            for (auto& address : addresses) {
                auto ma = libp2p::multi::Multiaddress::create(address).value();
                auto peer_id_base58 = ma.getPeerId().value();
                auto peer_id = libp2p::peer::PeerId::fromBase58(peer_id_base58).value();

                addresses_by_peer_id[std::move(peer_id)].emplace_back(std::move(ma));
            }

            std::vector<libp2p::peer::PeerInfo> v;
            v.reserve(addresses_by_peer_id.size());
            for (auto& i : addresses_by_peer_id) {
                v.emplace_back(libp2p::peer::PeerInfo{
                    /*.id =*/ i.first, /*.addresses =*/ {std::move(i.second)} });
            }

            return v;
        }();

        auto peer_id = libp2p::peer::PeerId::fromBase58("12D3KooWSGCJYbM6uCvCF7cGWSitXSJTgEb7zjVCaxDyYNASTa8i").value();

        io->post([&] {
            auto listen = host->listen(ma);
            if (!listen) {
                std::cerr << "Cannot listen address " << ma.getStringAddress().data()
                    << ". Error: " << listen.error().message() << std::endl;
                std::exit(EXIT_FAILURE);
            }

            for (auto& bootstrap_node : bootstrap_nodes) {
                kademlia->addPeer(bootstrap_node, true);
            }

            host->start();

            auto res = kademlia->findPeer(peer_id, [&](libp2p::outcome::result<libp2p::peer::PeerInfo> pi) {
                if (!pi.has_failure())
                {
                    std::cout << "Peer found: " << pi.value().id.toBase58() << std::endl;
                    std::cout << "Peers addresses: ";
                    for (auto& address : pi.value().addresses)
                    {
                        std::cout << address.getStringAddress() << ";";
                    }
                }
                else
                {
                    std::cout << "Peer not found. Peer: " << peer_id.toBase58()
                        << ", Reason: " << pi.error().message() << std::endl;
                }
                // Say to world about his providing
                //provide();

                // Ask provider from world
                //find_providers();

                //kademlia->start();
                });

            }); // 

        boost::asio::signal_set signals(*io, SIGINT, SIGTERM);
        signals.async_wait(
            [&io](const boost::system::error_code&, int) { io->stop(); });
        io->run();
    }
    catch (const std::exception& e)
    {
        std::cerr << "Exception: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

