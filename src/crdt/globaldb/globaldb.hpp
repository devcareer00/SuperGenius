#ifndef SUPERGENIUS_CRDT_GLOBALDB_HPP
#define SUPERGENIUS_CRDT_GLOBALDB_HPP

#include <crdt/crdt_options.hpp>
#include <outcome/outcome.hpp>

#include <ipfs_pubsub/gossip_pubsub_topic.hpp>
#include <crdt/crdt_datastore.hpp>

#include <boost/asio/io_context.hpp>
#include <boost/filesystem/path.hpp>

namespace sgns::crdt
{
class GlobalDB
{
public:
    GlobalDB(
        std::shared_ptr<boost::asio::io_context> context,
        std::string strDatabasePath,
        std::shared_ptr<sgns::ipfs_pubsub::GossipPubSubTopic> broadcastChannel);

    outcome::result<void> Init(std::shared_ptr<CrdtOptions> crdtOptions);

    std::shared_ptr<CrdtDatastore> GetDatastore() const;

private:
    std::shared_ptr<boost::asio::io_context> m_context;
    std::string m_strDatabasePath;
    std::shared_ptr<sgns::ipfs_pubsub::GossipPubSubTopic> m_broadcastChannel;

    std::shared_ptr<CrdtDatastore> m_crdtDatastore;

    sgns::base::Logger m_logger = sgns::base::createLogger("GlobalDB");
};
}
#endif // SUPERGENIUS_CRDT_GLOBALDB_HPP
