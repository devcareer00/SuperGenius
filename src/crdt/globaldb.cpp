#include <iterator>
#include <iostream>
#include <algorithm>
#include <crdt/crdt_datastore.hpp>
#include <boost/program_options.hpp>
#include <boost/algorithm/string/erase.hpp>
#include <boost/asio/io_context.hpp>
#include <storage/rocksdb/rocksdb.hpp>
#include <boost/filesystem.hpp>
#include <boost/random.hpp>
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

namespace sgns::crdt
{
  // TODO: Need to implement it based on new pubsub 
  class PubSubBroadcaster : public Broadcaster
  {
  public:

    PubSubBroadcaster() = default;

    void SetLogger(const sgns::base::Logger& logger) { this->logger_ = logger; }

    /**
    * Send {@param buff} payload to other replicas.
    * @return outcome::success on success or outcome::failure on error
    */
    virtual outcome::result<void> PubSubBroadcaster::Broadcast(const base::Buffer& buff) override
    {
      if (!buff.empty())
      {
        const std::string bCastData(buff.toString());
        //if (logger_ != nullptr)
        //{
        //  logger_->info("Broadcasting : " + bCastData);
        //}
        listOfBroadcasts_.push(bCastData);
      }
      return outcome::success();
    }

    /**
    * Obtain the next {@return} payload received from the network.
    * @return buffer value or outcome::failure on error
    */
    virtual outcome::result<base::Buffer> PubSubBroadcaster::Next() override
    {
      if (listOfBroadcasts_.empty())
      {
        //Broadcaster::ErrorCode::ErrNoMoreBroadcast
        return outcome::failure(boost::system::error_code{});
      }

      std::string strBuffer = listOfBroadcasts_.front();
      listOfBroadcasts_.pop();

      base::Buffer buffer;
      buffer.put(strBuffer);
      return buffer;
    }

    std::queue<std::string> listOfBroadcasts_;
    sgns::base::Logger logger_ = nullptr;
  };

  class CustomDagSyncer : public DAGSyncer
  {
  public:
    using IpfsDatastore = ipfs_lite::ipfs::IpfsDatastore;
    using MerkleDagServiceImpl = ipfs_lite::ipfs::merkledag::MerkleDagServiceImpl;

    CustomDagSyncer(std::shared_ptr<IpfsDatastore> service)
      : DAGSyncer(service)
    {
    }

    virtual outcome::result<bool> HasBlock(const CID& cid) const override
    {
      auto getNodeResult = this->getNode(cid);
      return getNodeResult.has_value();
    }
  };
}


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
using CustomDagSyncer = sgns::crdt::CustomDagSyncer;
using RocksdbDatastore = sgns::ipfs_lite::ipfs::RocksdbDatastore;
using IpfsRocksDb = sgns::ipfs_lite::rocksdb;
//using Host = sgns::crdt::Host;

namespace po = boost::program_options;

/** Display key and value added to CRDT datastore
*/
void PutHook(const std::string& k, const Buffer& v, const sgns::base::Logger& logger);

/** Display key removed from CRDT datastore
*/
void DeleteHook(const std::string& k, const sgns::base::Logger& logger);

/** Generate key pair or load it from file if available
*/
outcome::result<KeyPair> GetKeypair(const boost::filesystem::path& pathToKey,
  std::shared_ptr<KeyMarshaller>& keyMarshaller, const sgns::base::Logger& logger);

std::string getLocalIP(boost::asio::io_context& io);

int main(int argc, char** argv)
{
  std::string strDatabasePath;
  int portNumber = 0;
  bool daemonMode = false;
  po::options_description desc("Input arguments:");
  try
  {
    desc.add_options()
      ("help,h", "print help")
      ("daemon,d", "Running in daemon mode")
      ("databasePath,db", po::value<std::string>(&strDatabasePath)->default_value("CRDT.Datastore"),
        "Path to CRDT datastore")
      ("port, p", po::value<int>(&portNumber)->default_value(33123), "Port number")
      ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("daemon"))
    {
      daemonMode = true;
    }

    if (vm.count("help")) 
    {
      std::cout << desc << "\n";
      return EXIT_FAILURE;
    }
  }
  catch (std::exception& e)
  {
    std::cerr << "Error parsing arguments: " << e.what() << "\n";
    std::cout << desc << "\n";
    return EXIT_FAILURE;
  }
  auto logger = sgns::base::createLogger("globaldb");

  boost::filesystem::path databasePath = strDatabasePath;

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
    return EXIT_FAILURE;
  }

  boost::filesystem::path keyPath = strDatabasePathAbsolute + "/key";
  logger->info("Path to keypairs " + keyPath.string());
  std::shared_ptr<KeyMarshaller> keyMarshaller = nullptr;
  auto keyPairResult = GetKeypair(keyPath, keyMarshaller, logger);
  if (keyPairResult.has_failure())
  {
    logger->error("Unable to get key pair");
    return EXIT_FAILURE;
  }

  PrivateKey privateKey = keyPairResult.value().privateKey;
  PublicKey publicKey = keyPairResult.value().publicKey;

  if (keyMarshaller == nullptr)
  {
    logger->error("Unable to marshal keys, keyMarshaller is NULL");
    return EXIT_FAILURE;
  }

  auto protobufKeyResult = keyMarshaller->marshal(publicKey);
  if (protobufKeyResult.has_failure())
  {
    logger->error("Unable to marshal public key");
    return EXIT_FAILURE;
  }

  auto peerIDResult = PeerId::fromPublicKey(protobufKeyResult.value());
  if (peerIDResult.has_failure())
  {
    logger->error("Unable to get peer ID from public key");
    return EXIT_FAILURE;
  }

  auto peerID = peerIDResult.value();
  logger->info("Peer ID from public key: " + peerID.toBase58());

  // injector creates and ties dependent objects
  auto injector = libp2p::injector::makeHostInjector<BOOST_DI_CFG>();

  // create asio context
  auto io = injector.create<std::shared_ptr<boost::asio::io_context>>();

  // host is our local libp2p node
  auto host = injector.create<std::shared_ptr<libp2p::Host>>();

  // make peer uri of local node
  auto local_address_str = "/ip4/" + getLocalIP(*io) + "/tcp/" + std::to_string(portNumber)
    + "/p2p/" + host->getId().toBase58();

  auto listenResult = libp2p::multi::Multiaddress::create(local_address_str);
  if (listenResult.has_failure())
  {
    logger->error("Unable to create local multi address" + listenResult.error().message());
    return EXIT_FAILURE;
  }
  // TODO: Create pubsub gossip node

  // TODO: Implement pubsub broadcaster 
  auto broadcaster = std::make_shared<PubSubBroadcaster>();
  broadcaster->SetLogger(logger);

  // Create new DAGSyncer
  IpfsRocksDb::Options rdbOptions;
  rdbOptions.create_if_missing = true;  // intentionally
  auto ipfsDBResult = IpfsRocksDb::create(dataStore->getDB());
  if (ipfsDBResult.has_error())
  {
    logger->error("Unable to create database for IPFS datastore");
    return EXIT_FAILURE;
  }

  auto ipfsDataStore = std::make_shared<RocksdbDatastore>(ipfsDBResult.value());
  auto dagSyncer = std::make_shared<CustomDagSyncer>(ipfsDataStore);

  auto crdtOptions = CrdtOptions::DefaultOptions();
  crdtOptions->logger = logger;
  // Bind PutHook function pointer for notification purposes
  crdtOptions->putHookFunc = std::bind(&PutHook, std::placeholders::_1, std::placeholders::_2, logger);
  // Bind DeleteHook function pointer for notification purposes
  crdtOptions->deleteHookFunc = std::bind(&DeleteHook, std::placeholders::_1, logger);

  auto crdtDatastore = std::make_shared<CrdtDatastore>(dataStore, HierarchicalKey("crdt"), dagSyncer, broadcaster, crdtOptions);
  if (crdtDatastore == nullptr)
  {
    logger->error("Unable to create CRDT datastore");
    return EXIT_FAILURE;
  }

  logger->info("Bootstrapping...");

  std::string topicName = "globaldb-example";
  std::string netTopic = "globaldb-example-net";
  std::string config = "globaldb-example";

  auto peerAddressResult = PeerAddress::create(peerID, listenResult.value());
  if (peerAddressResult.has_failure())
  {
    logger->error("Unable to create peer address: " + peerAddressResult.error().message());
    return EXIT_FAILURE;
  }

  auto peerAddress = peerAddressResult.value();
  auto listenAddress = listenResult.value();

  // start the node as soon as async engine starts
  io->post([&] 
    {
    auto listen_res = host->listen(listenAddress);
    if (!listen_res) 
    {
      std::cout << "Cannot listen to multiaddress "
        << listenAddress.getStringAddress() << ", "
        << listen_res.error().message() << "\n";
      io->stop();
      return EXIT_FAILURE;
    }
    host->start();
    //TODO:
    //gossip->start();
    std::cout << "Node started\n";
    }
  );


  std::ostringstream streamDisplayDetails;
  streamDisplayDetails << "\n\n\nPeer ID: " << peerID.toBase58() << std::endl;
  streamDisplayDetails << "Listen address: " << listenAddress.getStringAddress() << std::endl;
  streamDisplayDetails << "Topic: " << topicName << std::endl;
  streamDisplayDetails << "Data folder: " << strDatabasePathAbsolute << std::endl;
  streamDisplayDetails << std::endl;
  streamDisplayDetails << "Ready!" << std::endl;
  streamDisplayDetails << std::endl;
  streamDisplayDetails << "Commands: " << std::endl;
  streamDisplayDetails << std::endl;
  streamDisplayDetails << "> list               -> list items in the store" << std::endl;
  streamDisplayDetails << "> get <key>          -> get value for a key" << std::endl;
  streamDisplayDetails << "> put <key> <value>  -> store value on a key" << std::endl;
  streamDisplayDetails << "> exit               -> quit" << std::endl;
  streamDisplayDetails << std::endl;
  std::cout << streamDisplayDetails.str();

  if (daemonMode)
  {
    std::cout << "Running in daemon mode\n" << std::endl;
    boost::asio::signal_set signals(*io, SIGINT, SIGTERM);
    signals.async_wait(
      [&io](const boost::system::error_code&, int) { io->stop(); });

    // run event loop
    io->run();
  }
  else
  {
    std::cout << "> ";
    std::string command;
    while (std::getline(std::cin, command))
    {
      if (command.empty())
      {
        std::cout << "> ";
      }
      else if (command == "exit" || command == "quit")
      {
        break;
      }
      else if (command == "list")
      {
        auto queryResult = crdtDatastore->QueryKeyValues("");
        if (queryResult.has_failure())
        {
          std::cout << "Unable list keys from CRDT datastore" << std::endl;
        }
        else
        {
          auto keysPrefixResult = crdtDatastore->GetKeysPrefix();
          if (keysPrefixResult.has_failure())
          {
            std::cout << "Unable to get key prefix from CRDT datastore" << std::endl;
            return EXIT_FAILURE;
          }
          auto valueSuffixResult = crdtDatastore->GetValueSuffix();
          if (valueSuffixResult.has_failure())
          {
            std::cout << "Unable to get value suffix from CRDT datastore" << std::endl;
            return EXIT_FAILURE;
          }

          auto keysPrefix = keysPrefixResult.value();
          auto valueSuffix = valueSuffixResult.value();
          for (const auto& element : queryResult.value())
          {
            // key name: /crdt/s/k/<key>/v
            auto strKey = std::string(element.first.toString());
            boost::algorithm::erase_first(strKey, keysPrefix);
            boost::algorithm::erase_last(strKey, valueSuffix);
            std::cout << "[" << strKey << "] -> " << element.second.toString() << std::endl;
          }
        }
      }
      else if (command.rfind("get") == 0)
      {
        std::string key = command.substr(3);
        key.erase(std::remove_if(key.begin(), key.end(), ::isspace), key.end());
        if (key.empty())
        {
          std::cout << "get <key>" << std::endl;
        }
        else
        {
          auto getKeyResult = crdtDatastore->GetKey(HierarchicalKey(key));
          if (getKeyResult.has_failure())
          {
            std::cout << "Unable to find key in CRDT datastore: " << key << std::endl;
          }
          else
          {
            std::cout << "[" << key << "] -> " << getKeyResult.value().toString() << std::endl;
          }
        }
      }
      else if (command.rfind("put") == 0)
      {
        size_t pos = 0;
        std::vector<std::string> commandList;
        std::string commandToParse = command;
        while ((pos = commandToParse.find(" ")) != std::string::npos)
        {
          commandList.push_back(commandToParse.substr(0, pos));
          commandToParse.erase(0, pos + 1);
        }
        if (!commandToParse.empty())
        {
          commandList.push_back(commandToParse);
        }

        if (commandList.size() < 3)
        {
          std::cout << "put <key> <value>" << std::endl;
        }
        else
        {
          auto key = commandList[1];
          std::string value = commandList[2];
          if (commandList.size() > 3)
          {
            for (int i = 3; i < commandList.size(); ++i)
            {
              value += " " + commandList[i];
            }
          }
          Buffer valueBuffer;
          valueBuffer.put(value);
          auto setKeyResult = crdtDatastore->PutKey(HierarchicalKey(key), valueBuffer);
          if (setKeyResult.has_failure())
          {
            std::cout << "Unable to put key-value to CRDT datastore: " << key << " " << value << std::endl;
          }
        }
      }
      std::cout << "> ";
    }
  }

  return EXIT_SUCCESS;
}

std::string getLocalIP(boost::asio::io_context& io)
{
  boost::asio::ip::tcp::resolver resolver(io);
  boost::asio::ip::tcp::resolver::query query(boost::asio::ip::host_name(), "");
  boost::asio::ip::tcp::resolver::iterator it = resolver.resolve(query);
  boost::asio::ip::tcp::resolver::iterator end;
  std::string addr("127.0.0.1");
  while (it != end) 
  {
    auto ep = it->endpoint();
    if (ep.address().is_v4()) {
      addr = ep.address().to_string();
      break;
    }
    ++it;
  }
  return addr;
}

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


void PutHook(const std::string& k, const Buffer& v, const sgns::base::Logger& logger)
{
  if (logger != nullptr)
  {
    std::string key = k;
    if (!key.empty() && key[0] == '/')
    {
      key.erase(0, 1);
    }
    logger->info("CRDT datastore: Added [" + key + "] -> " + std::string(v.toString()));
  }
}

void DeleteHook(const std::string& k, const sgns::base::Logger& logger)
{
  if (logger != nullptr)
  {
    std::string key = k;
    if (!key.empty() && key[0] == '/')
    {
      key.erase(0, 1);
    }
    logger->info("CRDT datastore: Removed [" + key + "]");
  }
}

