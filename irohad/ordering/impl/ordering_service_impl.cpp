/**
 * Copyright Soramitsu Co., Ltd. 2017 All Rights Reserved.
 * http://soramitsu.co.jp
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "ordering/impl/ordering_service_impl.hpp"

/**
 * Will be published when transaction is received.
 */
struct TransactionEvent {};

namespace iroha {
  namespace ordering {
    OrderingServiceImpl::OrderingServiceImpl(
        std::shared_ptr<ametsuchi::PeerQuery> wsv, size_t max_size,
        size_t delay_milliseconds,
        std::shared_ptr<network::OrderingServiceTransport> transport,
        std::shared_ptr<uvw::Loop> loop)
        : loop_(std::move(loop)),
          timer_(loop_->resource<uvw::TimerHandle>()),
          wsv_(wsv),
          max_size_(max_size),
          delay_milliseconds_(delay_milliseconds),
          transport_(transport),
          proposal_height(2) {
      timer_->on<uvw::TimerEvent>([this](const auto &, auto &) {
        if (!queue_.empty()) {
          this->generateProposal();
        }
        timer_->start(uvw::TimerHandle::Time(delay_milliseconds_),
                      uvw::TimerHandle::Time(0));
      });

      this->on<TransactionEvent>([this](const auto &, auto &) {
        if (queue_.unsafe_size() >= max_size_) {
          timer_->stop();
          this->generateProposal();
          timer_->start(uvw::TimerHandle::Time(delay_milliseconds_),
                        uvw::TimerHandle::Time(0));
        }
      });

      timer_->start(uvw::TimerHandle::Time(delay_milliseconds_),
                    uvw::TimerHandle::Time(0));
    }

    void OrderingServiceImpl::onTransaction(model::Transaction transaction) {
      queue_.push(transaction);
      publish(TransactionEvent{});
    }

    void OrderingServiceImpl::generateProposal() {
      auto txs = decltype(std::declval<model::Proposal>().transactions)();
      for (model::Transaction tx;
           txs.size() < max_size_ and queue_.try_pop(tx);) {
        txs.push_back(std::move(tx));
      }

      model::Proposal proposal(txs);
      proposal.height = proposal_height++;

      publishProposal(std::move(proposal));
    }

    void OrderingServiceImpl::publishProposal(model::Proposal &&proposal) {
      std::vector<std::string> peers;
      for (const auto &peer : wsv_->getLedgerPeers().value()) {
        peers.push_back(peer.address);
      }
      transport_->publishProposal(std::move(proposal), peers);

    }



    OrderingServiceImpl::~OrderingServiceImpl() { timer_->close(); }
  }  // namespace ordering
}  // namespace iroha
