#include <crdt/globaldb/globaldb.hpp>
#include <crdt/globaldb/pubsub_broadcaster.hpp>

#include <crdt/crdt_datastore.hpp>
#include <crdt/graphsync_dagsyncer.hpp>
#include <ipfs_lite/ipfs/merkledag/impl/merkledag_service_impl.hpp>
#include <ipfs_lite/ipfs/impl/datastore_rocksdb.hpp>
#include <libp2p/crypto/key_marshaller/key_marshaller_impl.hpp>
#include <libp2p/crypto/key_validator/key_validator_impl.hpp>
#include <libp2p/multi/multiaddress.hpp>
#include <libp2p/peer/impl/identity_manager_impl.hpp>
#include <libp2p/peer/peer_id.hpp>
#include <libp2p/peer/peer_address.hpp>
#include <libp2p/host/host.hpp>
#include <crypto/ed25519/ed25519_provider_impl.hpp>
#include <libp2p/crypto/random_generator/boost_generator.hpp>
#include <libp2p/crypto/crypto_provider/crypto_provider_impl.hpp>
#include <libp2p/crypto/ed25519_provider/ed25519_provider_impl.hpp>
#include <libp2p/crypto/rsa_provider/rsa_provider_impl.hpp>
#include <libp2p/crypto/secp256k1_provider/secp256k1_provider_impl.hpp>
#include <libp2p/crypto/ecdsa_provider/ecdsa_provider_impl.hpp>
#include <libp2p/crypto/hmac_provider/hmac_provider_impl.hpp>
#include <libp2p/injector/host_injector.hpp>
#include <libp2p/protocol/echo.hpp>
#include <libp2p/protocol/common/asio/asio_scheduler.hpp>
#include <ipfs_lite/ipfs/graphsync/impl/graphsync_impl.hpp>
#include <libp2p/common/literals.hpp>

namespace sgns::crdt
{
namespace
{
using RocksDB = sgns::storage::rocksdb;
using Buffer = sgns::base::Buffer;
using CryptoProvider = libp2p::crypto::CryptoProviderImpl;
using IdentityManager = libp2p::peer::IdentityManagerImpl;
using KeyPair = libp2p::crypto::KeyPair;
using PrivateKey = libp2p::crypto::PrivateKey;
using PublicKey = libp2p::crypto::PublicKey;
using KeyMarshaller = libp2p::crypto::marshaller::KeyMarshallerImpl;
using KeyValidator = libp2p::crypto::validator::KeyValidatorImpl;
using PeerId = libp2p::peer::PeerId;
using PeerAddress = libp2p::peer::PeerAddress;
using CrdtOptions = sgns::crdt::CrdtOptions;
using CrdtDatastore = sgns::crdt::CrdtDatastore;
using HierarchicalKey = sgns::crdt::HierarchicalKey;
using PubSubBroadcaster = sgns::crdt::PubSubBroadcaster;
using GraphsyncDAGSyncer = sgns::crdt::GraphsyncDAGSyncer;
using RocksdbDatastore = sgns::ipfs_lite::ipfs::RocksdbDatastore;
using IpfsRocksDb = sgns::ipfs_lite::rocksdb;
using GossipPubSub = sgns::ipfs_pubsub::GossipPubSub;
using GraphsyncImpl = sgns::ipfs_lite::ipfs::graphsync::GraphsyncImpl;
using GossipPubSubTopic = sgns::ipfs_pubsub::GossipPubSubTopic;

/** Generate key pair or load it from file if available
*/
outcome::result<KeyPair> GetKeypair(const boost::filesystem::path& pathToKey, std::shared_ptr<KeyMarshaller>& keyMarshaller, const sgns::base::Logger& logger)
{
    KeyPair keyPair;

    auto cryptoProvider = std::make_shared<CryptoProvider>(
        std::make_shared<libp2p::crypto::random::BoostRandomGenerator>(),
        std::make_shared<libp2p::crypto::ed25519::Ed25519ProviderImpl>(),
        std::make_shared<libp2p::crypto::rsa::RsaProviderImpl>(),
        std::make_shared<libp2p::crypto::ecdsa::EcdsaProviderImpl>(),
        std::make_shared<libp2p::crypto::secp256k1::Secp256k1ProviderImpl>(),
        std::make_shared<libp2p::crypto::hmac::HmacProviderImpl>());

    auto keyValidator = std::make_shared<KeyValidator>(cryptoProvider);
    keyMarshaller = std::make_shared<KeyMarshaller>(keyValidator);

    if (!boost::filesystem::exists(pathToKey))
    {
        auto keyPairResult = cryptoProvider->generateKeys(libp2p::crypto::Key::Type::Ed25519,
            libp2p::crypto::common::RSAKeyType::RSA1024);

        if (keyPairResult.has_failure())
        {
            logger->error("Unable to generate key pair");
            return outcome::failure(boost::system::error_code{});
        }

        keyPair = keyPairResult.value();

        auto marshalPrivateKeyResult = keyMarshaller->marshal(keyPair.privateKey);
        if (marshalPrivateKeyResult.has_failure())
        {
            logger->error("Unable to marshal private key");
            return outcome::failure(boost::system::error_code{});
        }
        auto marshalPublicKeyResult = keyMarshaller->marshal(keyPair.publicKey);
        if (marshalPublicKeyResult.has_failure())
        {
            logger->error("Unable to marshal public key");
            return outcome::failure(boost::system::error_code{});
        }

        std::ofstream fileKey(pathToKey.string(), std::ios::out | std::ios::binary);
        std::copy(marshalPrivateKeyResult.value().key.cbegin(), marshalPrivateKeyResult.value().key.cend(),
            std::ostreambuf_iterator<char>(fileKey));
        std::copy(marshalPublicKeyResult.value().key.cbegin(), marshalPublicKeyResult.value().key.cend(),
            std::ostreambuf_iterator<char>(fileKey));
        fileKey.close();
    }
    else
    {
        std::ifstream fileKey(pathToKey.string(), std::ios::in | std::ios::binary);
        if (!fileKey.is_open())
        {
            logger->error("Unable to open key file: " + pathToKey.string());
            return outcome::failure(boost::system::error_code{});
        }
        std::istreambuf_iterator<char> it{ fileKey }, end;
        std::string ss{ it, end };

        std::vector<uint8_t> key = std::vector<uint8_t>(ss.begin(), ss.begin() + ss.size() / 2);
        libp2p::crypto::ProtobufKey privateProtobufKey{ key };

        key.clear();
        key = std::vector<uint8_t>(ss.begin() + ss.size() / 2, ss.end());
        libp2p::crypto::ProtobufKey publicProtobufKey{ key };

        auto unmarshalPrivateKeyResult = keyMarshaller->unmarshalPrivateKey(privateProtobufKey);
        if (unmarshalPrivateKeyResult.has_failure())
        {
            logger->error("Unable to unmarshal private key");
            return outcome::failure(boost::system::error_code{});
        }
        keyPair.privateKey = unmarshalPrivateKeyResult.value();

        auto unmarshalPublicKeyResult = keyMarshaller->unmarshalPublicKey(publicProtobufKey);
        if (unmarshalPublicKeyResult.has_failure())
        {
            logger->error("Unable to unmarshal public key");
            return outcome::failure(boost::system::error_code{});
        }
        keyPair.publicKey = unmarshalPublicKeyResult.value();
    }

    return keyPair;
}
}

GlobalDB::GlobalDB(
    std::shared_ptr<boost::asio::io_context> context,
    std::string strDatabasePath,
    std::string pubsubChannel,
    int pubsubListeningPort,
    std::vector<std::string> pubsubBootstrapPeers)
    : m_context(std::move(context))
    , m_strDatabasePath(std::move(strDatabasePath))
    , m_pubsubChannel(std::move(pubsubChannel))
    , m_pubsubListeningPort(std::move(pubsubListeningPort))
    , m_pubsubBootstrapPeers(std::move(pubsubBootstrapPeers))
{
}

outcome::result<void> GlobalDB::Start(std::shared_ptr<CrdtOptions> crdtOptions)
{
    auto logger = sgns::base::createLogger("globaldb");
    boost::filesystem::path databasePath = m_strDatabasePath;

    auto strDatabasePathAbsolute = boost::filesystem::absolute(databasePath).string();

    std::shared_ptr<RocksDB> dataStore = nullptr;

    // Create new database
    logger->info("Opening database " + strDatabasePathAbsolute);
    RocksDB::Options options;
    options.create_if_missing = true;  // intentionally
    try
    {
        auto dataStoreResult = RocksDB::create(boost::filesystem::absolute(databasePath).string(), options);
        dataStore = dataStoreResult.value();
    }
    catch (std::exception& e)
    {
        logger->error("Unable to open database: " + std::string(e.what()));
        return outcome::failure(boost::system::error_code{});
    }

    boost::filesystem::path keyPath = strDatabasePathAbsolute + "/key";
    logger->info("Path to keypairs " + keyPath.string());
    std::shared_ptr<KeyMarshaller> keyMarshaller = nullptr;
    auto keyPairResult = GetKeypair(keyPath, keyMarshaller, logger);
    if (keyPairResult.has_failure())
    {
        logger->error("Unable to get key pair");
        return outcome::failure(boost::system::error_code{});
    }

    PrivateKey privateKey = keyPairResult.value().privateKey;
    PublicKey publicKey = keyPairResult.value().publicKey;

    if (keyMarshaller == nullptr)
    {
        logger->error("Unable to marshal keys, keyMarshaller is NULL");
        return outcome::failure(boost::system::error_code{});
    }

    auto protobufKeyResult = keyMarshaller->marshal(publicKey);
    if (protobufKeyResult.has_failure())
    {
        logger->error("Unable to marshal public key");
        return outcome::failure(boost::system::error_code{});
    }

    auto peerIDResult = PeerId::fromPublicKey(protobufKeyResult.value());
    if (peerIDResult.has_failure())
    {
        logger->error("Unable to get peer ID from public key");
        return outcome::failure(boost::system::error_code{});
    }

    auto peerID = peerIDResult.value();
    logger->info("Peer ID from public key: " + peerID.toBase58());

    // injector creates and ties dependent objects
    auto injector = libp2p::injector::makeHostInjector<BOOST_DI_CFG>(
        boost::di::bind<boost::asio::io_context>.to(m_context)[boost::di::override]);

    // create asio context
    auto io = injector.create<std::shared_ptr<boost::asio::io_context>>();

    // Create new DAGSyncer
    IpfsRocksDb::Options rdbOptions;
    rdbOptions.create_if_missing = true;  // intentionally
    auto ipfsDBResult = IpfsRocksDb::create(dataStore->getDB());
    if (ipfsDBResult.has_error())
    {
        logger->error("Unable to create database for IPFS datastore");
        return outcome::failure(boost::system::error_code{});
    }

    auto ipfsDataStore = std::make_shared<RocksdbDatastore>(ipfsDBResult.value());

    // @todo Check if the port should be the object parameter
    auto listen_to = libp2p::multi::Multiaddress::create("/ip4/127.0.0.1/tcp/40000").value();
    auto dagSyncerHost = injector.create<std::shared_ptr<libp2p::Host>>();
    auto scheduler = std::make_shared<libp2p::protocol::AsioScheduler>(*io, libp2p::protocol::SchedulerConfig{});
    auto graphsync = std::make_shared<GraphsyncImpl>(dagSyncerHost, std::move(scheduler));
    auto dagSyncer = std::make_shared<GraphsyncDAGSyncer>(ipfsDataStore, graphsync, dagSyncerHost);

    // Start DagSyner listener 
    auto listenResult = dagSyncer->Listen(listen_to);
    if (listenResult.has_failure())
    {
        logger->warn("DAG syncer failed to listen " + std::string(listen_to.getStringAddress()));
        // @todo Check if the error is not fatal
    }

    // Create pubsub gossip node
    m_pubsub = std::make_shared<GossipPubSub>(keyPairResult.value());
    m_pubsub->Start(m_pubsubListeningPort, m_pubsubBootstrapPeers);
    auto gossipPubSubTopic = std::make_shared <GossipPubSubTopic>(m_pubsub, m_pubsubChannel);
    auto broadcaster = std::make_shared<PubSubBroadcaster>(gossipPubSubTopic);
    broadcaster->SetLogger(logger);

    m_crdtDatastore = std::make_shared<CrdtDatastore>(
        dataStore, HierarchicalKey("crdt"), dagSyncer, broadcaster, crdtOptions);
    if (m_crdtDatastore == nullptr)
    {
        logger->error("Unable to create CRDT datastore");
        return outcome::failure(boost::system::error_code{});
    }

    logger->info("Bootstrapping...");
    // TODO: bootstrapping
    //bstr, _ : = multiaddr.NewMultiaddr("/ip4/94.130.135.167/tcp/33123/ipfs/12D3KooWFta2AE7oiK1ioqjVAKajUJauZWfeM7R413K7ARtHRDAu");
    //inf, _ : = peer.AddrInfoFromP2pAddr(bstr);
    //list: = append(ipfslite.DefaultBootstrapPeers(), *inf);
    //ipfs.Bootstrap(list);
    //h.ConnManager().TagPeer(inf.ID, "keep", 100);

    //if (daemonMode && echoProtocol)
    //{
    //  // set a handler for Echo protocol
    //  libp2p::protocol::Echo echo{ libp2p::protocol::EchoConfig{} };
    //  host->setProtocolHandler(
    //    echo.getProtocolId(),
    //    [&echo](std::shared_ptr<libp2p::connection::Stream> received_stream) {
    //      echo.handle(std::move(received_stream));
    //    });
    //}
    return outcome::success();
}

std::shared_ptr<ipfs_pubsub::GossipPubSub> GlobalDB::GetGossipPubSub() const
{
    return m_pubsub;
}

std::shared_ptr<CrdtDatastore> GlobalDB::GetDatastore() const
{
    return m_crdtDatastore;
}
}