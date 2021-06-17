#include "common/logger.hpp"
#include "graphsync_acceptance_common.hpp"

// logger used by these tests
static std::shared_ptr<spdlog::logger> logger;

// max test case run time, always limited by various params
static size_t run_time_msec = 0;

namespace sgns::ipfs_lite::ipfs::graphsync::test {

  // Test node aggregate
  class Node {
   public:
    // total requests sent by all nodes in a test case
    static size_t requests_sent;

    // total responses received by all nodes in a test case
    static size_t responses_received;

    // n_responses_expected: count of responses received by the node after which
    // io->stop() is called
    Node(std::shared_ptr<boost::asio::io_context> io,
         std::shared_ptr<MerkleDagBridge> data_service,
         Graphsync::BlockCallback cb,
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
    void listen(const libp2p::multi::Multiaddress &listen_to) {
      auto listen_res = host_->listen(listen_to);
      if (!listen_res) {
        logger->trace("Cannot listen to multiaddress {}, {}",
                      listen_to.getStringAddress(),
                      listen_res.error().message());
        return;
      }
      start();
    }

    // calls Graphsync's makeRequest
    void makeRequest(const libp2p::peer::PeerId &peer,
                     boost::optional<libp2p::multi::Multiaddress> address,
                     const CID &root_cid) {
      start();

      std::vector<Extension> extensions;
      ResponseMetadata response_metadata{};
      Extension response_metadata_extension =
          encodeResponseMetadata(response_metadata);
      extensions.push_back(response_metadata_extension);
      std::vector<CID> cids;
      Extension do_not_send_cids_extension = encodeDontSendCids(cids);
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
       requests_.push_back(std::shared_ptr<Subscription>(new Subscription(std::move(graphsync_->makeRequest(peer,
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
    Graphsync::RequestProgressCallback requestProgressCallback() {
      static auto formatExtensions =
          [](const std::vector<Extension> &extensions) -> std::string {
        std::string s;
        for (const auto &item : extensions) {
          s += fmt::format(
              "({}: 0x{}) ", item.name, common::Buffer(item.data).toHex());
        }
        return s;
      };
      return [this](ResponseStatusCode code,
                    const std::vector<Extension> &extensions) {
        ++responses_received;
        logger->trace("request progress: code={}, extensions={}",
                      statusCodeToString(code),
                      formatExtensions(extensions));
        if (++n_responses == n_responses_expected_) {
          io_->stop();
        }
      };
    }

    // asion context to be stopped when needed
    std::shared_ptr<boost::asio::io_context> io_;

    std::shared_ptr<Graphsync> graphsync_;

    std::shared_ptr<libp2p::Host> host_;

    std::shared_ptr<MerkleDagBridge> data_service_;

    Graphsync::BlockCallback block_cb_;

    // keeping subscriptions alive, otherwise they cancel themselves
    // class Subscription have non-copyable constructor and operator, so it can not be used in std::vector
    // std::vector<Subscription> requests_;

    std::vector<std::shared_ptr<Subscription >> requests_;

    size_t n_responses_expected_;
    size_t n_responses = 0;
    bool started_ = false;
  };

  size_t Node::requests_sent = 0;
  size_t Node::responses_received = 0;

  namespace
  {
      boost::optional<libp2p::peer::PeerInfo> PeerInfoFromString(const std::string& str)
      {
          auto server_ma_res = libp2p::multi::Multiaddress::create(str);
          if (!server_ma_res)
          {
              return boost::none;
          }
          auto server_ma = std::move(server_ma_res.value());

          auto server_peer_id_str = server_ma.getPeerId();
          if (!server_peer_id_str)
          {
              return boost::none;
          }

          auto server_peer_id_res = libp2p::peer::PeerId::fromBase58(*server_peer_id_str);
          if (!server_peer_id_res)
          {
              return boost::none;
          }

          return libp2p::peer::PeerInfo{ server_peer_id_res.value(), {server_ma} };
      }
  }

  // Two nodes interact, one connection is utilized
  void testTwoNodesClientServer() {
    Node::requests_sent = 0;
    Node::responses_received = 0;

    auto io_server = std::make_shared<boost::asio::io_context>();
    auto io_client = std::make_shared<boost::asio::io_context>();

    // strings from which we create blocks and CIDs
    std::vector<std::string> strings({"xxx", "yyy", "zzz"});

    size_t unexpected = 0;

    // creating instances

    auto server_data = std::make_shared<TestDataService>();

    // server block callback expects no blocks
    auto server_cb = [&unexpected](CID, common::Buffer) { ++unexpected; };

    auto client_data = std::make_shared<TestDataService>();

    // clienc block callback expect 3 blocks from the string above
    auto client_cb = [&client_data, &unexpected](CID cid, common::Buffer data) {
      if (!client_data->onDataBlock(std::move(cid), std::move(data))) {
        ++unexpected;
      }
    };

    for (const auto &s : strings) {
      // client expects what server has

      server_data->addData(s);
      client_data->addExpected(s);
    }

    Node server(io_server, server_data, server_cb, 0);
    Node client(io_client, client_data, client_cb, 3);

    auto listen_to =
        libp2p::multi::Multiaddress::create("/ip4/127.0.0.1/tcp/40000/ipfs/" + server.getId().toBase58()).value();

    // starting all the stuff asynchronously
    server.listen(listen_to);

    auto listen_to2 =
        libp2p::multi::Multiaddress::create("/ip4/127.0.0.1/tcp/40000/ipfs/" + client.getId().toBase58()).value();
    client.listen(listen_to2);

    std::string lt(listen_to.getStringAddress().begin(), listen_to.getStringAddress().end());
    io_client->post([&, lt]() {
      // server listens
      auto pi = PeerInfoFromString(lt);
      auto peer = pi.value().id;
      bool use_address = true;

      // client makes 3 requests

      for (const auto &[cid, _] : client_data->getExpected()) {
        boost::optional<libp2p::multi::Multiaddress> address(pi.value().addresses[0]);

        // don't need to pass the address more than once
        client.makeRequest(peer, use_address ? address : boost::none, cid);
        use_address = false;
      }
    });

    std::thread t_server([&]() { io_server->run(); });
    std::thread t_client([&]() { io_client->run(); });
    //runEventLoop(io_server, run_time_msec);
    //runEventLoop(io_client, run_time_msec);

    t_server.join();
    t_client.join();
    
    client.stop();
    server.stop();

    logger->info("total requests sent {}, responses received {}",
                 Node::requests_sent,
                 Node::responses_received);
  }

  // Context for more complex cases
  struct NodeParams {
    // listen address
    boost::optional<libp2p::multi::Multiaddress> listen_to;

    // MerkleDAG stub for node
    std::shared_ptr<TestDataService> data_service;

    // Strings to make blocks and CIDs from them
    std::vector<std::string> strings;

    // peer ID
    boost::optional<libp2p::peer::PeerId> peer;
  };

  // N nodes communicate P2P with each other  and collect many blocks.
  // Each node has n_data data blocks
  void testManyNodesExchange(size_t N, size_t n_data) {
    Node::requests_sent = 0;
    Node::responses_received = 0;

    size_t unexpected_responses = 0;
    size_t total_responses = 0;
    size_t expected = 0;

    // creating parameters for N nodes

    std::vector<NodeParams> params;
    params.reserve(N);
    for (size_t i = 0; i < N; ++i) {
      auto &p = params.emplace_back();

      // The node #i will listen to 40000+i pore
      p.listen_to = libp2p::multi::Multiaddress::create(
                        fmt::format("/ip4/127.0.0.1/tcp/{}", 40000 + i))
                        .value();

      p.data_service = std::make_shared<TestDataService>();

      // the i-th node has data represented by strings data_i_j, j in[0, n_data)
      p.strings.reserve(n_data);
      for (size_t j = 0; j < n_data; ++j) {
        auto &s = p.strings.emplace_back(fmt::format("data_{}_{}", i, j));
        p.data_service->addData(s);
      }
    }

    auto io = std::make_shared<boost::asio::io_context>();

    // creating N nodes

    std::vector<Node> nodes;
    nodes.reserve(N);

    for (size_t i = 0; i < N; ++i) {
      auto &p = params[i];

      auto cb = [ds = p.data_service,
                 &expected,
                 &unexpected_responses,
                 &total_responses,
                 &io](CID cid, common::Buffer data) {
        logger->trace("data block received, {}:{}, {}/{}",
                      cid.toString().value(),
                      std::string((const char *)data.data(), data.size()),
                      total_responses + 1,
                      expected);
        if (!ds->onDataBlock(std::move(cid), std::move(data))) {
          ++unexpected_responses;
        } else if (++total_responses == expected) {
          io->stop();
        }
      };

      auto &n = nodes.emplace_back(io, p.data_service, cb, 0);

      // peer IDs are known only at this point
      p.peer = n.getId();

      for (size_t j = 0; j < N; ++j) {
        if (j != i) {
          for (const auto &s : params[j].strings)

            // each node expects data other hodes have
            p.data_service->addExpected(s);
        }
      }
    }

    // starting N nodes asynchronously

    io->post([&]() {
      for (size_t i = 0; i < N; ++i) {
        auto &p = params[i];
        auto &n = nodes[i];

        // each node listens
        n.listen(p.listen_to.value());
      }

      // will make connections in the next cycle
      io->post([&]() {
        for (size_t i = 0; i < N; ++i) {
          auto &p = params[i];
          auto &n = nodes[i];

          for (const auto &[cid, d] : p.data_service->getExpected()) {
            ++expected;
            for (const auto &p0 : params) {
              if (&p0 != &p) {
                logger->trace("request from {} to {} for {}:{}",
                              p.peer->toBase58().substr(46),
                              p0.peer->toBase58().substr(46),
                              cid.toString().value(),
                              std::string((const char *)d.data(), d.size()));

                // each node request every piece of expected data from
                // all other nodes. And gets RS_FULL_CONTENT 1 time per each
                // data block,
                // and respectively RS_NOT_FOUND will come N-2 times per block

                n.makeRequest(p0.peer.value(), p0.listen_to, cid);
              }
            }
          }
        }
      });
    });

    runEventLoop(io, run_time_msec);

    for (auto &n : nodes) {
      n.stop();
    }

    logger->info("total requests sent {}, responses received {}",
                 Node::requests_sent,
                 Node::responses_received);

  }  // namespace sgns::ipfs_lite::ipfs::graphsync::test

}  // namespace sgns::ipfs_lite::ipfs::graphsync::test

int main(int argc, char *argv[]) {
    logger = sgns::common::createLogger("test");
    logger->set_level(spdlog::level::trace);
    sgns::common::createLogger("graphsync")->set_level(spdlog::level::trace);

    sgns::ipfs_lite::ipfs::graphsync::test::testTwoNodesClientServer();
    return 0;
}
