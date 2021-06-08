#ifndef SUPERGENIUS_CRDT_GLOBALDB_HPP
#define SUPERGENIUS_CRDT_GLOBALDB_HPP

#include <crdt/crdt_options.hpp>
#include <outcome/outcome.hpp>

#include <ipfs_pubsub/gossip_pubsub_topic.hpp>
#include <crdt/crdt_datastore.hpp>

#include <boost/asio/io_context.hpp>

namespace sgns::crdt
{
class GlobalDB
{
public:
    GlobalDB(
        std::shared_ptr<boost::asio::io_context> context,
        std::string strDatabasePath, 
        std::string pubsubChannel,
        int pubsubListeningPort,
        std::vector<std::string> pubsubBootstrapPeers
        );

    outcome::result<void> Start(std::shared_ptr<CrdtOptions> crdtOptions);

    std::shared_ptr<ipfs_pubsub::GossipPubSub> GetGossipPubSub() const;
    std::shared_ptr<CrdtDatastore> GetDatastore() const;

private:
    std::shared_ptr<boost::asio::io_context> m_context;
    std::string m_strDatabasePath;
    size_t m_pubsubListeningPort;
    std::string m_pubsubChannel;
    std::vector<std::string> m_pubsubBootstrapPeers;

    std::shared_ptr<ipfs_pubsub::GossipPubSub> m_pubsub;
    std::shared_ptr<CrdtDatastore> m_crdtDatastore;

    sgns::base::Logger m_logger = sgns::base::createLogger("GlobalDB");
};
}
#endif // SUPERGENIUS_CRDT_GLOBALDB_HPP
