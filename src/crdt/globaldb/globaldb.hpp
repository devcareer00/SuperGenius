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
        std::string pubsubChannel);

    outcome::result<void> Start(
        std::shared_ptr<sgns::ipfs_pubsub::GossipPubSub> pubSub,
        std::shared_ptr<CrdtOptions> crdtOptions);

    std::shared_ptr<CrdtDatastore> GetDatastore() const;

private:
    std::shared_ptr<boost::asio::io_context> m_context;
    std::string m_strDatabasePath;
    std::string m_pubsubChannel;

    std::shared_ptr<CrdtDatastore> m_crdtDatastore;

    sgns::base::Logger m_logger = sgns::base::createLogger("GlobalDB");
};
}
#endif // SUPERGENIUS_CRDT_GLOBALDB_HPP
