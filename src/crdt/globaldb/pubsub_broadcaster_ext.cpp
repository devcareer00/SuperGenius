#include "pubsub_broadcaster_ext.hpp"
#include <crdt/crdt_datastore.hpp>
#include <ipfs_lite/ipld/ipld_node.hpp>

namespace sgns::crdt
{
PubSubBroadcasterExt::PubSubBroadcasterExt(
    std::shared_ptr<GossipPubSubTopic> pubSubTopic,
    std::shared_ptr<sgns::crdt::GraphsyncDAGSyncer> dagSyncer,
    libp2p::peer::PeerId localPeerId)
    : gossipPubSubTopic_(pubSubTopic)
    , dagSyncer_(dagSyncer)
    , dataStore_(nullptr)
    , localPeerId_(localPeerId)
{
    if (gossipPubSubTopic_ != nullptr)
    {
        gossipPubSubTopic_->Subscribe(std::bind(&PubSubBroadcasterExt::OnMessage, this, std::placeholders::_1));
    }
}

void PubSubBroadcasterExt::OnMessage(boost::optional<const GossipPubSub::Message&> message)
{
    if (message)
    {
        std::string data(reinterpret_cast<const char*>(message->data.data()), message->data.size());
        auto peerId = libp2p::peer::PeerId::fromBytes(message->from);
        if (peerId.has_value())
        {
            std::scoped_lock lock(mutex_);
            if (peerId.value() != localPeerId_)
            {
                base::Buffer buf;
                buf.put(data);
                auto cids = dataStore_->DecodeBroadcast(buf);
                if (!cids.has_failure())
                {
                    for (const auto& cid : cids.value())
                    {
                        //dagSyncer_->RequestNode(peerId.value(), boost::none, cid);
                        dagSyncer_->RequestNode(localPeerId_, boost::none, cid);
                    }
                }
            }
            messageQueue_.push(std::make_tuple(std::move(peerId.value()), std::move(data)));
        }
    }
}

void PubSubBroadcasterExt::SetLogger(const sgns::base::Logger& logger)
{ 
    logger_ = logger; 
}

void PubSubBroadcasterExt::SetCrdtDataStore(CrdtDatastore* dataStore)
{
    dataStore_ = dataStore;
}

outcome::result<void> PubSubBroadcasterExt::Broadcast(const base::Buffer& buff)
{
    if (this->gossipPubSubTopic_ == nullptr)
    {
        return outcome::failure(boost::system::error_code{});
    }
    const std::string bCastData(buff.toString());
    gossipPubSubTopic_->Publish(bCastData);
    return outcome::success();
}

outcome::result<base::Buffer> PubSubBroadcasterExt::Next()
{
    std::scoped_lock lock(mutex_);
    if (messageQueue_.empty())
    {
        //Broadcaster::ErrorCode::ErrNoMoreBroadcast
        return outcome::failure(boost::system::error_code{});
    }

    std::string strBuffer = std::get<1>(messageQueue_.front());
    messageQueue_.pop();

    base::Buffer buffer;
    buffer.put(strBuffer);
    return buffer;
}
}
