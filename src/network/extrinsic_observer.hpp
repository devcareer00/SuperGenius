

#ifndef SUPERGENIUS_CORE_NETWORK_EXTRINSIC_OBSERVER_HPP
#define SUPERGENIUS_CORE_NETWORK_EXTRINSIC_OBSERVER_HPP

#include "api/service/author/author_api.hpp"
#include "common/blob.hpp"
#include "common/outcome.hpp"
#include "primitives/extrinsic.hpp"

namespace sgns::network {

  class ExtrinsicObserver {
   public:
    virtual ~ExtrinsicObserver() = default;

    virtual outcome::result<common::Hash256> onTxMessage(
        const primitives::Extrinsic &extrinsic) = 0;
  };

}  // namespace sgns::network

#endif  // SUPERGENIUS_CORE_NETWORK_EXTRINSIC_OBSERVER_HPP
