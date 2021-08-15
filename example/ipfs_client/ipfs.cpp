#include <crdt/globaldb/globaldb.hpp>
#include <crdt/globaldb/keypair_file_storage.hpp>

#include <libp2p/multi/multibase_codec/multibase_codec_impl.hpp>
#include <libp2p/injector/host_injector.hpp>
#include <libp2p/host/host.hpp>
#include <libp2p/protocol/common/asio/asio_scheduler.hpp>

#include <ipfs_lite/ipfs/graphsync/graphsync.hpp>
#include <ipfs_lite/ipfs/graphsync/impl/graphsync_impl.hpp>
#include <ipfs_lite/ipld/impl/ipld_node_impl.hpp>

#include <boost/program_options.hpp>
#include <boost/format.hpp>
#include <boost/di/extension/scopes/shared.hpp>

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

    // MerkleDAG bridge interface for test purposes
    class TestDataService : public sgns::ipfs_lite::ipfs::graphsync::MerkleDagBridge {
    public:
        using Storage = std::map<sgns::CID, sgns::common::Buffer>;

        TestDataService& addData(const std::string& s) {
            insertNode(data_, s);
            return *this;
        }

        TestDataService& addExpected(const std::string& s) {
            insertNode(expected_, s);
            return *this;
        }

        const Storage& getData() const {
            return data_;
        }

        const Storage& getExpected() const {
            return expected_;
        }

        const Storage& getReceived() const {
            return received_;
        }

        // places into data_, returns true if expected
        bool onDataBlock(sgns::CID cid, sgns::common::Buffer data);

    private:
        static void insertNode(Storage& dst, const std::string& data_str);

        outcome::result<size_t> select(
            const sgns::CID& cid,
            gsl::span<const uint8_t> selector,
            std::function<bool(const sgns::CID& cid, const sgns::common::Buffer& data)> handler)
            const override;

        Storage data_;
        Storage expected_;
        Storage received_;
    };

    std::pair<std::shared_ptr<sgns::ipfs_lite::ipfs::graphsync::Graphsync>, std::shared_ptr<libp2p::Host>>
        createNodeObjects(std::shared_ptr<boost::asio::io_context> io) {

        // [boost::di::override] allows for creating multiple hosts for testing
        // purposes
        auto injector =
            libp2p::injector::makeHostInjector<boost::di::extension::shared_config>(
                boost::di::bind<boost::asio::io_context>.to(
                    io)[boost::di::override]);

        std::pair<std::shared_ptr<sgns::ipfs_lite::ipfs::graphsync::Graphsync>, std::shared_ptr<libp2p::Host>>
            objects;
        objects.second = injector.template create<std::shared_ptr<libp2p::Host>>();
        auto scheduler = std::make_shared<libp2p::protocol::AsioScheduler>(
            *io, libp2p::protocol::SchedulerConfig{});
        objects.first =
            std::make_shared<sgns::ipfs_lite::ipfs::graphsync::GraphsyncImpl>(objects.second, std::move(scheduler));
        return objects;
    }

    bool TestDataService::onDataBlock(sgns::CID cid, sgns::common::Buffer data) {
        bool expected = false;
        auto it = expected_.find(cid);
        if (it != expected_.end() && it->second == data) {
            expected = (received_.count(cid) == 0);
        }
        received_[cid] = std::move(data);
        return expected;
    }

    inline void TestDataService::insertNode(TestDataService::Storage& dst,
        const std::string& data_str) {
        using NodeImpl = sgns::ipfs_lite::ipld::IPLDNodeImpl;
        auto node = NodeImpl::createFromString(data_str);
        dst[node->getCID()] = node->getRawBytes();
    }

    inline outcome::result<size_t> TestDataService::select(
        const sgns::CID& cid,
        gsl::span<const uint8_t> selector,
        std::function<bool(const sgns::CID& cid, const sgns::common::Buffer& data)> handler)
        const {
        auto it = data_.find(cid);
        if (it != data_.end()) {
            handler(it->first, it->second);
            return 1;
        }
        return 0;
    }

    class Node {
    public:
        // total requests sent by all nodes in a test case
        static size_t requests_sent;

        // total responses received by all nodes in a test case
        static size_t responses_received;

        // n_responses_expected: count of responses received by the node after which
        // io->stop() is called
        Node(std::shared_ptr<boost::asio::io_context> io,
            std::shared_ptr<sgns::ipfs_lite::ipfs::graphsync::MerkleDagBridge> data_service,
            sgns::ipfs_lite::ipfs::graphsync::Graphsync::BlockCallback cb,
            size_t n_responses_expected)
            : io_(std::move(io)),
            data_service_(std::move(data_service)),
            block_cb_(std::move(cb)),
            n_responses_expected_(n_responses_expected) {
            std::tie(graphsync_, host_) = createNodeObjects(io_);
        }

        // stops graphsync and host, otherwise they can interact with further tests!
        void stop() {
            graphsync_->stop();
            host_->stop();
        }

        // returns peer ID, so they can connect to each other
        auto getId() const {
            return host_->getId();
        }

        // listens to network and starts nodes if not yet started
        void listen(const libp2p::multi::Multiaddress& listen_to) {
            auto listen_res = host_->listen(listen_to);
            if (!listen_res) {
                //logger->trace("Cannot listen to multiaddress {}, {}",
                //    listen_to.getStringAddress(),
                //    listen_res.error().message());
                return;
            }
            start();
        }

        // calls Graphsync's makeRequest
        void makeRequest(const libp2p::peer::PeerId& peer,
            boost::optional<libp2p::multi::Multiaddress> address,
            const sgns::CID& root_cid) {
            start();

            std::vector<sgns::ipfs_lite::ipfs::graphsync::Extension> extensions;
            sgns::ipfs_lite::ipfs::graphsync::ResponseMetadata response_metadata{};
            sgns::ipfs_lite::ipfs::graphsync::Extension response_metadata_extension =
                sgns::ipfs_lite::ipfs::graphsync::encodeResponseMetadata(response_metadata);
            extensions.push_back(response_metadata_extension);
            std::vector<sgns::CID> cids;
            sgns::ipfs_lite::ipfs::graphsync::Extension do_not_send_cids_extension = 
                sgns::ipfs_lite::ipfs::graphsync::encodeDontSendCids(cids);
            extensions.push_back(do_not_send_cids_extension);
            // unused code , request_ is deleted because Subscription have deleted copy-constructor and operator
                  // requests_.push_back(graphsync_->makeRequest(peer,
                  //                                             std::move(address),
                  //                                             root_cid,
                  //                                             {},
                  //                                             extensions,
                  //                                             requestProgressCallback()));
                  // Subscription subscription = graphsync_->makeRequest(peer,
                  //                                             std::move(address),
                  //                                             root_cid,
                  //                                             {},
                  //                                             extensions,
                  //                                             requestProgressCallback());
            requests_.push_back(std::shared_ptr<libp2p::protocol::Subscription>(
                new libp2p::protocol::Subscription(std::move(graphsync_->makeRequest(peer,
                std::move(address),
                root_cid,
                {},
                extensions,
                requestProgressCallback())))));

            //-------------------------------------------------------------------------------------

            ++requests_sent;
        }

    private:
        void start() {
            if (!started_) {
                graphsync_->start(data_service_, block_cb_);
                host_->start();
                started_ = true;
            }
        }

        // helper, returns requesu callback fn
        sgns::ipfs_lite::ipfs::graphsync::Graphsync::RequestProgressCallback requestProgressCallback() {
            static auto formatExtensions =
                [](const std::vector<sgns::ipfs_lite::ipfs::graphsync::Extension>& extensions) -> std::string {
                std::string s;
                for (const auto& item : extensions) {
                    s += fmt::format(
                        "({}: 0x{}) ", item.name, sgns::common::Buffer(item.data).toHex());
                }
                return s;
            };
            return [this](sgns::ipfs_lite::ipfs::graphsync::ResponseStatusCode code,
                const std::vector<sgns::ipfs_lite::ipfs::graphsync::Extension>& extensions) {
                    ++responses_received;
                    //logger->trace("request progress: code={}, extensions={}",
                    //    statusCodeToString(code),
                    //    formatExtensions(extensions));
                    if (++n_responses == n_responses_expected_) {
                        io_->stop();
                    }
            };
        }

        // asion context to be stopped when needed
        std::shared_ptr<boost::asio::io_context> io_;

        std::shared_ptr<sgns::ipfs_lite::ipfs::graphsync::Graphsync> graphsync_;

        std::shared_ptr<libp2p::Host> host_;

        std::shared_ptr<sgns::ipfs_lite::ipfs::graphsync::MerkleDagBridge> data_service_;

        sgns::ipfs_lite::ipfs::graphsync::Graphsync::BlockCallback block_cb_;

        // keeping subscriptions alive, otherwise they cancel themselves
        // class Subscription have non-copyable constructor and operator, so it can not be used in std::vector
        // std::vector<Subscription> requests_;

        std::vector<std::shared_ptr<sgns::ipfs_lite::ipfs::graphsync::Subscription >> requests_;

        size_t n_responses_expected_;
        size_t n_responses = 0;
        bool started_ = false;
    };

    size_t Node::requests_sent = 0;
    size_t Node::responses_received = 0;

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

    auto loggerDAGSyncer = libp2p::common::createLogger("GraphsyncDAGSyncer");
    loggerDAGSyncer->set_level(spdlog::level::trace);

    auto loggerBroadcaster = libp2p::common::createLogger("PubSubBroadcasterExt");
    loggerBroadcaster->set_level(spdlog::level::debug);

    const std::string processingGridChannel = "GRID_CHANNEL_ID";

    auto pubs = std::make_shared<sgns::ipfs_pubsub::GossipPubSub>(
        sgns::crdt::KeyPairFileStorage("CRDT.IPFS.TEST/ipfs").GetKeyPair().value());

    std::vector<std::string> pubsubBootstrapPeers;
    if (options->remote)
    {
        pubsubBootstrapPeers = std::move(std::vector({ *options->remote }));
    }
    pubs->Start(40001, pubsubBootstrapPeers);

    //------------------------------------------------------------------------------
    auto listen_to =
        libp2p::multi::Multiaddress::create("/ip4/127.0.0.1/tcp/40000").value();

    auto io = std::make_shared<boost::asio::io_context>();

    // strings from which we create blocks and CIDs
    std::vector<std::string> strings({ "xxx", "yyy", "zzz" });

    // creating instances
    auto server_data = std::make_shared<TestDataService>();

    // server block callback expects no blocks
    auto server_cb = [](sgns::CID, sgns::common::Buffer) { };

    auto client_data = std::make_shared<TestDataService>();

    // clienc block callback expect 3 blocks from the string above
    auto client_cb = [&client_data](sgns::CID cid, sgns::common::Buffer data) {
        if (!client_data->onDataBlock(std::move(cid), std::move(data))) {
        }
    };

    for (const auto& s : strings) {
        // client expects what server has

        server_data->addData(s);
        client_data->addExpected(s);
    }

    Node server(io, server_data, server_cb, 0);
    Node client(io, client_data, client_cb, 3);

    // starting all the stuff asynchronously

    io->post([&]() {
        // server listens
        server.listen(listen_to);
        auto peer = server.getId();
        bool use_address = true;

        // client makes 3 requests

        for (const auto& [cid, _] : client_data->getExpected()) {
            boost::optional<libp2p::multi::Multiaddress> address(listen_to);

            // don't need to pass the address more than once
            client.makeRequest(peer, use_address ? address : boost::none, cid);
            use_address = false;
        }
        });

    std::thread iothread([io]() { io->run(); });

    //------------------------------------------------------------------------------
    // Gracefully shutdown on signal
    boost::asio::signal_set signals(*pubs->GetAsioContext(), SIGINT, SIGTERM);
    signals.async_wait(
        [&pubs, &io](const boost::system::error_code&, int)
        {
            pubs->Stop();
            io->stop();
        });

    pubs->Wait();

    client.stop();
    server.stop();

    iothread.join();

    return 0;
}

