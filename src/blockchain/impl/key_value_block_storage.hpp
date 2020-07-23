

#ifndef SUPERGENIUS_KEY_VALUE_BLOCK_STORAGE_HPP
#define SUPERGENIUS_KEY_VALUE_BLOCK_STORAGE_HPP

#include "blockchain/block_storage.hpp"

#include "blockchain/impl/common.hpp"
#include "base/logger.hpp"
#include "crypto/hasher.hpp"

namespace sgns::blockchain {

  class KeyValueBlockStorage : public BlockStorage {
   public:
    inline static const base::Buffer LAST_FINALIZED_BLOCK_HASH_LOOKUP_KEY =
        base::Buffer{}.put(":supergenius:last_finalized_block_hash");

    using BlockHandler = std::function<void(const primitives::Block &)>;

    enum class Error {
      BLOCK_EXISTS = 1,
      BODY_DOES_NOT_EXIST,
      JUSTIFICATION_DOES_NOT_EXIST,
      GENESIS_ALREADY_EXISTS,
      FINALIZED_BLOCK_NOT_FOUND,
    };

    ~KeyValueBlockStorage() override = default;

    static outcome::result<std::shared_ptr<KeyValueBlockStorage>> create(
        base::Buffer state_root,
        const std::shared_ptr<storage::BufferStorage> &storage,
        const std::shared_ptr<crypto::Hasher> &hasher,
        const BlockHandler &on_finalized_block_found);

    /**
     * Initialise block storage with existing data
     * @param storage underlying storage (must be empty)
     * @param hasher a hasher instance
     */
    static outcome::result<std::shared_ptr<KeyValueBlockStorage>> loadExisting(
        const std::shared_ptr<storage::BufferStorage> &storage,
        std::shared_ptr<crypto::Hasher> hasher,
        const BlockHandler &on_finalized_block_found);

    /**
     * Initialise block storage with a genesis block which is created inside
     * from merkle trie root
     * @param storage underlying storage (must be empty)
     * @param hasher a hasher instance
     */
    static outcome::result<std::shared_ptr<KeyValueBlockStorage>>
    createWithGenesis(base::Buffer state_root,
                      const std::shared_ptr<storage::BufferStorage> &storage,
                      std::shared_ptr<crypto::Hasher> hasher,
                      const BlockHandler &on_genesis_created);

    outcome::result<primitives::BlockHash> getLastFinalizedBlockHash()
        const override;
    outcome::result<void> setLastFinalizedBlockHash(
        const primitives::BlockHash &) override;

    outcome::result<primitives::BlockHeader> getBlockHeader(
        const primitives::BlockId &id) const override;
    outcome::result<primitives::BlockBody> getBlockBody(
        const primitives::BlockId &id) const override;
    outcome::result<primitives::BlockData> getBlockData(
        const primitives::BlockId &id) const override;
    outcome::result<primitives::Justification> getJustification(
        const primitives::BlockId &block) const override;

    outcome::result<primitives::BlockHash> putBlockHeader(
        const primitives::BlockHeader &header) override;
    outcome::result<void> putBlockData(
        primitives::BlockNumber block_number,
        const primitives::BlockData &block_data) override;
    outcome::result<primitives::BlockHash> putBlock(
        const primitives::Block &block) override;

    outcome::result<void> putJustification(
        const primitives::Justification &j,
        const primitives::BlockHash &hash,
        const primitives::BlockNumber &number) override;

    outcome::result<void> removeBlock(
        const primitives::BlockHash &hash,
        const primitives::BlockNumber &number) override;

   private:
    KeyValueBlockStorage(std::shared_ptr<storage::BufferStorage> storage,
                         std::shared_ptr<crypto::Hasher> hasher);

    outcome::result<void> ensureGenesisNotExists() const;

    std::shared_ptr<storage::BufferStorage> storage_;
    std::shared_ptr<crypto::Hasher> hasher_;
    base::Logger logger_;
  };
}  // namespace sgns::blockchain

OUTCOME_HPP_DECLARE_ERROR_2(sgns::blockchain, KeyValueBlockStorage::Error);

#endif  // SUPERGENIUS_KEY_VALUE_BLOCK_STORAGE_HPP
