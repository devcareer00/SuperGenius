

#ifndef SUPERGENIUS_CRYPTO_SECP256K1_PROVIDER_IMPL_HPP
#define SUPERGENIUS_CRYPTO_SECP256K1_PROVIDER_IMPL_HPP

#include <secp256k1.h>

#include "crypto/secp256k1_provider.hpp"

namespace sgns::crypto {

  enum class Secp256k1ProviderError {
    INVALID_ARGUMENT = 1,
    RECOVERY_FAILED,
  };

  class Secp256k1ProviderImpl : public Secp256k1Provider {
   public:
    ~Secp256k1ProviderImpl() override = default;

    Secp256k1ProviderImpl();

    outcome::result<secp256k1::ExpandedPublicKey> recoverPublickeyUncompressed(
        const secp256k1::RSVSignature &signature,
        const secp256k1::MessageHash &message_hash) const override;

    outcome::result<secp256k1::CompressedPublicKey> recoverPublickeyCompressed(
        const secp256k1::RSVSignature &signature,
        const secp256k1::MessageHash &message_hash) const override;

   private:
    outcome::result<secp256k1_pubkey> recoverPublickey(
        const secp256k1::RSVSignature &signature,
        const secp256k1::MessageHash &message_hash) const;

    std::unique_ptr<secp256k1_context, void (*)(secp256k1_context *)> context_;
  };
}  // namespace sgns::crypto

OUTCOME_HPP_DECLARE_ERROR_2(sgns::crypto, Secp256k1ProviderError);

#endif  // SUPERGENIUS_CRYPTO_SECP256K1_PROVIDER_IMPL_HPP
