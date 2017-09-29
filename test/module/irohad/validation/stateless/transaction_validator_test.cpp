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

#include <gtest/gtest.h>
#include <chrono>
#include <crypto/crypto.hpp>
#include <crypto/hash.hpp>
#include <model/model_crypto_provider_impl.hpp>
#include <validation/impl/stateless_validator_impl.hpp>

iroha::model::Transaction sign(iroha::model::Transaction &tx,
                               const iroha::privkey_t &privkey,
                               const iroha::pubkey_t &pubkey) {
  auto tx_hash = iroha::hash(tx).to_string();

  auto sign = iroha::sign(tx_hash, pubkey, privkey);

  iroha::model::Signature signature{};
  signature.signature = sign;
  signature.pubkey = pubkey;

  tx.signatures.push_back(signature);

  return tx;
}

iroha::model::Transaction create_transaction() {
  iroha::model::Transaction tx{};
  tx.creator_account_id = "test";

  tx.tx_counter = 0;

  std::chrono::milliseconds now =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch());
  tx.created_ts = (iroha::ts64_t)now.count();
  return tx;
}

TEST(stateless_validation, stateless_validation_when_valid) {
  auto keypair = iroha::create_keypair();
  auto crypto_provider =
      std::make_shared<iroha::model::ModelCryptoProviderImpl>();
  iroha::validation::StatelessValidatorImpl transaction_validator(
      crypto_provider);

  auto tx = create_transaction();
  sign(tx, keypair.privkey, keypair.pubkey);

  ASSERT_TRUE(transaction_validator.validate(tx));
}

TEST(stateless_validation, stateless_validation_when_invalid_wrong_signature) {
  auto keypair = iroha::create_keypair();
  auto crypto_provider =
      std::make_shared<iroha::model::ModelCryptoProviderImpl>();
  iroha::validation::StatelessValidatorImpl transaction_validator(
      crypto_provider);

  auto tx = create_transaction();
  sign(tx, keypair.privkey, keypair.pubkey);

  tx.creator_account_id = "test1";

  ASSERT_FALSE(transaction_validator.validate(tx));
}

TEST(stateless_validation,
     stateless_validation_when_invalid_due_to_big_time_delay) {
  auto keypair = iroha::create_keypair();
  auto crypto_provider =
      std::make_shared<iroha::model::ModelCryptoProviderImpl>();
  iroha::validation::StatelessValidatorImpl transaction_validator(
      crypto_provider);

  auto tx = create_transaction();

  std::chrono::milliseconds now =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch());
  tx.created_ts = (iroha::ts64_t)(now.count() -
                                  1000 * 3600 * 25);  // tx created 25 hours ago
  sign(tx, keypair.privkey, keypair.pubkey);

  ASSERT_FALSE(transaction_validator.validate(tx));
}

TEST(stateless_validation,
     stateless_validation_when_invalid_due_to_tx_from_future) {
  auto keypair = iroha::create_keypair();
  auto crypto_provider =
      std::make_shared<iroha::model::ModelCryptoProviderImpl>();
  iroha::validation::StatelessValidatorImpl transaction_validator(
      crypto_provider);

  auto tx = create_transaction();

  std::chrono::milliseconds now =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch());
  tx.created_ts =
      (iroha::ts64_t)(now.count() + 1000 * 3600);  // tx created 1 hour later
  sign(tx, keypair.privkey, keypair.pubkey);

  ASSERT_FALSE(transaction_validator.validate(tx));
}
