
#include "api/service/state/requests/get_storage.hpp"

namespace sgns::api::state::request {

  outcome::result<void> GetStorage::init(
      const jsonrpc::Request::Parameters &params) {
    if (params.size() > 2 || params.empty()) {
      throw jsonrpc::InvalidParametersFault("Incorrect number of params");
    }
    auto &param0 = params[0];
    if (! param0.IsString()) {
      throw jsonrpc::InvalidParametersFault(
          "Parameter 'key' must be a hex string");
    }
    auto &&key_str = param0.AsString();
    OUTCOME_TRY((auto &&, key), base::unhexWith0x(key_str));

    key_ = base::Buffer(std::move(key));

    if (params.size() > 1) {
      auto &param1 = params[1];
      if (! param1.IsString()) {
        throw jsonrpc::InvalidParametersFault(
            "Parameter 'at' must be a hex string");
      }
      auto &&at_str = param1.AsString();
      OUTCOME_TRY((auto &&, at_span), base::unhexWith0x(at_str));
      OUTCOME_TRY((auto &&, at), primitives::BlockHash::fromSpan(at_span));
      at_.reset(at);
    } else {
      at_.reset();
    }
    return outcome::success();
  }

  outcome::result<base::Buffer> GetStorage::execute() {
    return at_ ? api_->getStorage(key_, at_.value()) : api_->getStorage(key_);
  }

}  // namespace sgns::api::state::request
