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
#include "ametsuchi/impl/storage_impl.hpp"
#include "common/byteutils.hpp"
#include "framework/test_subscriber.hpp"
#include "model/commands/add_asset_quantity.hpp"
#include "model/commands/add_peer.hpp"
#include "model/commands/add_signatory.hpp"
#include "model/commands/create_account.hpp"
#include "model/commands/create_asset.hpp"
#include "model/commands/create_domain.hpp"
#include "model/commands/remove_signatory.hpp"
#include "model/commands/set_quorum.hpp"
#include "model/commands/transfer_asset.hpp"
#include "model/model_hash_provider_impl.hpp"
#include "module/irohad/ametsuchi/ametsuchi_fixture.hpp"

using namespace iroha::ametsuchi;
using namespace iroha::model;
using namespace framework::test_subscriber;

TEST_F(AmetsuchiTest, GetBlocksCompletedWhenCalled) {
  // Commit block => get block => observable completed
  auto storage =
      StorageImpl::create(block_store_path, redishost_, redisport_, pgopt_);
  ASSERT_TRUE(storage);
  auto blocks = storage->getBlockQuery();

  Block block;
  block.height = 1;

  auto ms = storage->createMutableStorage();
  ms->apply(block, [](const auto &, auto &, const auto &) { return true; });
  storage->commit(std::move(ms));

  auto completed_wrapper =
      make_test_subscriber<IsCompleted>(blocks->getBlocks(1, 1));
  completed_wrapper.subscribe();
  ASSERT_TRUE(completed_wrapper.validate());
}

TEST_F(AmetsuchiTest, SampleTest) {
  HashProviderImpl hashProvider;

  auto storage =
      StorageImpl::create(block_store_path, redishost_, redisport_, pgopt_);
  ASSERT_TRUE(storage);
  auto wsv = storage->getWsvQuery();
  auto blocks = storage->getBlockQuery();

  Transaction txn;
  txn.creator_account_id = "admin1";
  CreateDomain createDomain;
  createDomain.domain_name = "ru";
  txn.commands.push_back(std::make_shared<CreateDomain>(createDomain));
  CreateAccount createAccount;
  createAccount.account_name = "user1";
  createAccount.domain_id = "ru";
  txn.commands.push_back(std::make_shared<CreateAccount>(createAccount));

  Block block;
  block.transactions.push_back(txn);
  block.height = 1;
  block.prev_hash.fill(0);
  auto block1hash = hashProvider.get_hash(block);
  block.hash = block1hash;
  block.txs_number = block.transactions.size();

  {
    auto ms = storage->createMutableStorage();
    ms->apply(block, [](const auto &blk, auto &query, const auto &top_hash) {
      return true;
    });
    storage->commit(std::move(ms));
  }

  {
    auto account = wsv->getAccount(createAccount.account_name + "@"
                                   + createAccount.domain_id);
    ASSERT_TRUE(account);
    ASSERT_EQ(account->account_id,
              createAccount.account_name + "@" + createAccount.domain_id);
    ASSERT_EQ(account->domain_name, createAccount.domain_id);
  }

  txn = Transaction();
  txn.creator_account_id = "admin2";
  createAccount = CreateAccount();
  createAccount.account_name = "user2";
  createAccount.domain_id = "ru";
  txn.commands.push_back(std::make_shared<CreateAccount>(createAccount));
  CreateAsset createAsset;
  createAsset.domain_id = "ru";
  createAsset.asset_name = "RUB";
  createAsset.precision = 2;
  txn.commands.push_back(std::make_shared<CreateAsset>(createAsset));
  AddAssetQuantity addAssetQuantity;
  addAssetQuantity.asset_id = "RUB#ru";
  addAssetQuantity.account_id = "user1@ru";
  iroha::Amount asset_amount(150, 2);
  addAssetQuantity.amount = asset_amount;
  txn.commands.push_back(std::make_shared<AddAssetQuantity>(addAssetQuantity));
  TransferAsset transferAsset;
  transferAsset.src_account_id = "user1@ru";
  transferAsset.dest_account_id = "user2@ru";
  transferAsset.asset_id = "RUB#ru";
  transferAsset.description = "test transfer";
  iroha::Amount transfer_amount(100, 2);
  transferAsset.amount = transfer_amount;
  txn.commands.push_back(std::make_shared<TransferAsset>(transferAsset));

  block = Block();
  block.transactions.push_back(txn);
  block.height = 2;
  block.prev_hash = block1hash;
  auto block2hash = hashProvider.get_hash(block);
  block.hash = block2hash;
  block.txs_number = block.transactions.size();

  {
    auto ms = storage->createMutableStorage();
    ms->apply(block, [](const auto &, auto &, const auto &) { return true; });
    storage->commit(std::move(ms));
  }

  {
    auto asset1 = wsv->getAccountAsset("user1@ru", "RUB#ru");
    ASSERT_TRUE(asset1);
    ASSERT_EQ(asset1->account_id, "user1@ru");
    ASSERT_EQ(asset1->asset_id, "RUB#ru");
    ASSERT_EQ(asset1->balance, iroha::Amount(50, 2));
    auto asset2 = wsv->getAccountAsset("user2@ru", "RUB#ru");
    ASSERT_TRUE(asset2);
    ASSERT_EQ(asset2->account_id, "user2@ru");
    ASSERT_EQ(asset2->asset_id, "RUB#ru");
    ASSERT_EQ(asset2->balance, iroha::Amount(100, 2));
  }

  // Block store tests
  blocks->getBlocks(1, 2).subscribe([block1hash, block2hash](auto eachBlock) {
    if (eachBlock.height == 1) {
      EXPECT_EQ(eachBlock.hash, block1hash);
    } else if (eachBlock.height == 2) {
      EXPECT_EQ(eachBlock.hash, block2hash);
    }
  });

  blocks->getAccountTransactions("admin1").subscribe(
      [](auto tx) { EXPECT_EQ(tx.commands.size(), 2); });
  blocks->getAccountTransactions("admin2").subscribe(
      [](auto tx) { EXPECT_EQ(tx.commands.size(), 4); });

  blocks->getAccountAssetTransactions("user1@ru", "RUB#ru")
      .subscribe([](auto tx) { EXPECT_EQ(tx.commands.size(), 1); });
  blocks->getAccountAssetTransactions("user2@ru", "RUB#ru")
      .subscribe([](auto tx) { EXPECT_EQ(tx.commands.size(), 1); });
}

TEST_F(AmetsuchiTest, PeerTest) {
  auto storage =
      StorageImpl::create(block_store_path, redishost_, redisport_, pgopt_);
  ASSERT_TRUE(storage);
  auto wsv = storage->getWsvQuery();

  Transaction txn;
  AddPeer addPeer;
  addPeer.peer_key.at(0) = 1;
  addPeer.address = "192.168.0.1:50051";
  txn.commands.push_back(std::make_shared<AddPeer>(addPeer));

  Block block;
  block.transactions.push_back(txn);

  {
    auto ms = storage->createMutableStorage();
    ms->apply(block, [](const auto &, auto &, const auto &) { return true; });
    storage->commit(std::move(ms));
  }

  auto peers = wsv->getPeers();
  ASSERT_TRUE(peers);
  ASSERT_EQ(peers->size(), 1);
  ASSERT_EQ(peers->at(0).pubkey, addPeer.peer_key);
  ASSERT_EQ(peers->at(0).address, addPeer.address);
}

TEST_F(AmetsuchiTest, queryGetAccountAssetTransactionsTest) {
  HashProviderImpl hashProvider;

  auto storage =
      StorageImpl::create(block_store_path, redishost_, redisport_, pgopt_);
  ASSERT_TRUE(storage);
  auto wsv = storage->getWsvQuery();
  auto blocks = storage->getBlockQuery();

  const auto admin = "admin1";
  const auto domain = "domain";
  const auto user1name = "user1";
  const auto user2name = "user2";
  const auto user3name = "user3";
  const auto user1id = "user1@domain";
  const auto user2id = "user2@domain";
  const auto user3id = "user3@domain";
  const auto asset1name = "asset1";
  const auto asset2name = "asset2";
  const auto asset1id = "asset1#domain";
  const auto asset2id = "asset2#domain";

  // 1st tx
  Transaction txn;
  txn.creator_account_id = admin;
  CreateDomain createDomain;
  createDomain.domain_name = domain;
  txn.commands.push_back(std::make_shared<CreateDomain>(createDomain));
  CreateAccount createAccount1;
  createAccount1.account_name = user1name;
  createAccount1.domain_id = domain;
  txn.commands.push_back(std::make_shared<CreateAccount>(createAccount1));
  CreateAccount createAccount2;
  createAccount2.account_name = user2name;
  createAccount2.domain_id = domain;
  txn.commands.push_back(std::make_shared<CreateAccount>(createAccount2));
  CreateAccount createAccount3;
  createAccount3.account_name = user3name;
  createAccount3.domain_id = domain;
  txn.commands.push_back(std::make_shared<CreateAccount>(createAccount3));
  CreateAsset createAsset1;
  createAsset1.domain_id = domain;
  createAsset1.asset_name = asset1name;
  createAsset1.precision = 2;
  txn.commands.push_back(std::make_shared<CreateAsset>(createAsset1));
  CreateAsset createAsset2;
  createAsset2.domain_id = domain;
  createAsset2.asset_name = asset2name;
  createAsset2.precision = 2;
  txn.commands.push_back(std::make_shared<CreateAsset>(createAsset2));
  AddAssetQuantity addAssetQuantity1;
  addAssetQuantity1.asset_id = asset1id;
  addAssetQuantity1.account_id = user1id;
  addAssetQuantity1.amount = iroha::Amount(300, 2);
  txn.commands.push_back(std::make_shared<AddAssetQuantity>(addAssetQuantity1));
  AddAssetQuantity addAssetQuantity2;
  addAssetQuantity2.asset_id = asset2id;
  addAssetQuantity2.account_id = user2id;
  addAssetQuantity2.amount = iroha::Amount(250, 2);
  txn.commands.push_back(std::make_shared<AddAssetQuantity>(addAssetQuantity2));

  Block block;
  block.transactions.push_back(txn);
  block.height = 1;
  block.prev_hash.fill(0);
  auto block1hash = hashProvider.get_hash(block);
  block.hash = block1hash;
  block.txs_number = static_cast<uint16_t>(block.transactions.size());

  {
    auto ms = storage->createMutableStorage();
    ms->apply(block, [](const auto &blk, auto &query, const auto &top_hash) {
      return true;
    });
    storage->commit(std::move(ms));
  }

  {
    auto account1 = wsv->getAccount(user1id);
    ASSERT_TRUE(account1);
    ASSERT_EQ(account1->account_id, user1id);
    ASSERT_EQ(account1->domain_name, domain);
    auto account2 = wsv->getAccount(user2id);
    ASSERT_TRUE(account2);
    ASSERT_EQ(account2->account_id, user2id);
    ASSERT_EQ(account2->domain_name, domain);
    auto account3 = wsv->getAccount(user3id);
    ASSERT_TRUE(account3);
    ASSERT_EQ(account3->account_id, user3id);
    ASSERT_EQ(account3->domain_name, domain);

    auto asset1 = wsv->getAccountAsset(user1id, asset1id);
    ASSERT_TRUE(asset1);
    ASSERT_EQ(asset1->account_id, user1id);
    ASSERT_EQ(asset1->asset_id, asset1id);
    ASSERT_EQ(asset1->balance, iroha::Amount(300, 2));
    auto asset2 = wsv->getAccountAsset(user2id, asset2id);
    ASSERT_TRUE(asset2);
    ASSERT_EQ(asset2->account_id, user2id);
    ASSERT_EQ(asset2->asset_id, asset2id);
    ASSERT_EQ(asset2->balance, iroha::Amount(250, 2));
  }

  // 2th tx (user1 -> user2 # asset1)
  txn = Transaction();
  txn.creator_account_id = user1id;
  TransferAsset transferAsset;
  transferAsset.src_account_id = user1id;
  transferAsset.dest_account_id = user2id;
  transferAsset.asset_id = asset1id;
  transferAsset.amount = iroha::Amount(120, 2);
  txn.commands.push_back(std::make_shared<TransferAsset>(transferAsset));

  block = Block();
  block.transactions.push_back(txn);
  block.height = 2;
  block.prev_hash = block1hash;
  auto block2hash = hashProvider.get_hash(block);
  block.hash = block2hash;
  block.txs_number = static_cast<uint16_t>(block.transactions.size());

  {
    auto ms = storage->createMutableStorage();
    ms->apply(block, [](const auto &, auto &, const auto &) { return true; });
    storage->commit(std::move(ms));
  }

  {
    auto asset1 = wsv->getAccountAsset(user1id, asset1id);
    ASSERT_TRUE(asset1);
    ASSERT_EQ(asset1->account_id, user1id);
    ASSERT_EQ(asset1->asset_id, asset1id);
    ASSERT_EQ(asset1->balance, iroha::Amount(180, 2));
    auto asset2 = wsv->getAccountAsset(user2id, asset1id);
    ASSERT_TRUE(asset2);
    ASSERT_EQ(asset2->account_id, user2id);
    ASSERT_EQ(asset2->asset_id, asset1id);
    ASSERT_EQ(asset2->balance, iroha::Amount(120, 2));
  }

  // 3rd tx
  //   (user2 -> user3 # asset2)
  //   (user2 -> user1 # asset2)
  txn = Transaction();
  txn.creator_account_id = user2id;
  TransferAsset transferAsset1;
  transferAsset1.src_account_id = user2id;
  transferAsset1.dest_account_id = user3id;
  transferAsset1.asset_id = asset2id;
  transferAsset1.amount = iroha::Amount(150, 2);
  txn.commands.push_back(std::make_shared<TransferAsset>(transferAsset1));
  TransferAsset transferAsset2;
  transferAsset2.src_account_id = user2id;
  transferAsset2.dest_account_id = user1id;
  transferAsset2.asset_id = asset2id;
  transferAsset2.amount = iroha::Amount(10, 2);
  txn.commands.push_back(std::make_shared<TransferAsset>(transferAsset2));

  block = Block();
  block.transactions.push_back(txn);
  block.height = 3;
  block.prev_hash = block2hash;
  auto block3hash = hashProvider.get_hash(block);
  block.hash = block3hash;
  block.txs_number = static_cast<uint16_t>(block.transactions.size());

  {
    auto ms = storage->createMutableStorage();
    ms->apply(block, [](const auto &, auto &, const auto &) { return true; });
    storage->commit(std::move(ms));
  }

  {
    auto asset1 = wsv->getAccountAsset(user2id, asset2id);
    ASSERT_TRUE(asset1);
    ASSERT_EQ(asset1->account_id, user2id);
    ASSERT_EQ(asset1->asset_id, asset2id);
    ASSERT_EQ(asset1->balance, iroha::Amount(90, 2));
    auto asset2 = wsv->getAccountAsset(user3id, asset2id);
    ASSERT_TRUE(asset2);
    ASSERT_EQ(asset2->account_id, user3id);
    ASSERT_EQ(asset2->asset_id, asset2id);
    ASSERT_EQ(asset2->balance, iroha::Amount(150, 2));
    auto asset3 = wsv->getAccountAsset(user1id, asset2id);
    ASSERT_TRUE(asset3);
    ASSERT_EQ(asset3->account_id, user1id);
    ASSERT_EQ(asset3->asset_id, asset2id);
    ASSERT_EQ(asset3->balance, iroha::Amount(10, 2));
  }

  // Block store tests
  blocks->getBlocks(1, 3).subscribe(
      [block1hash, block2hash, block3hash](auto eachBlock) {
        if (eachBlock.height == 1) {
          EXPECT_EQ(eachBlock.hash, block1hash);
        } else if (eachBlock.height == 2) {
          EXPECT_EQ(eachBlock.hash, block2hash);
        } else if (eachBlock.height == 3) {
          EXPECT_EQ(eachBlock.hash, block3hash);
        }
      });

  blocks->getAccountTransactions(admin).subscribe(
      [](auto tx) { EXPECT_EQ(tx.commands.size(), 8); });
  blocks->getAccountTransactions(user1id).subscribe(
      [](auto tx) { EXPECT_EQ(tx.commands.size(), 1); });
  blocks->getAccountTransactions(user2id).subscribe(
      [](auto tx) { EXPECT_EQ(tx.commands.size(), 2); });
  blocks->getAccountTransactions(user3id).subscribe(
      [](auto tx) { EXPECT_EQ(tx.commands.size(), 0); });

  // (user1 -> user2 # asset1)
  // (user2 -> user3 # asset2)
  // (user2 -> user1 # asset2)
  blocks->getAccountAssetTransactions(user1id, asset1id).subscribe([](auto tx) {
    EXPECT_EQ(tx.commands.size(), 1);
  });
  blocks->getAccountAssetTransactions(user2id, asset1id).subscribe([](auto tx) {
    EXPECT_EQ(tx.commands.size(), 1);
  });
  blocks->getAccountAssetTransactions(user3id, asset1id).subscribe([](auto tx) {
    EXPECT_EQ(tx.commands.size(), 0);
  });
  blocks->getAccountAssetTransactions(user1id, asset2id).subscribe([](auto tx) {
    EXPECT_EQ(tx.commands.size(), 1);
  });
  blocks->getAccountAssetTransactions(user2id, asset2id).subscribe([](auto tx) {
    EXPECT_EQ(tx.commands.size(), 2);
  });
  blocks->getAccountAssetTransactions(user3id, asset2id).subscribe([](auto tx) {
    EXPECT_EQ(tx.commands.size(), 1);
  });
}

TEST_F(AmetsuchiTest, AddSignatoryTest) {
  HashProviderImpl hashProvider;

  auto storage =
      StorageImpl::create(block_store_path, redishost_, redisport_, pgopt_);
  ASSERT_TRUE(storage);
  auto wsv = storage->getWsvQuery();

  iroha::ed25519::pubkey_t pubkey1, pubkey2;
  pubkey1.at(0) = 1;
  pubkey2.at(0) = 2;

  auto user1id = "user1@domain";
  auto user2id = "user2@domain";

  // 1st tx (create user1 with pubkey1)
  Transaction txn;
  txn.creator_account_id = "admin1";
  CreateDomain createDomain;
  createDomain.domain_name = "domain";
  txn.commands.push_back(std::make_shared<CreateDomain>(createDomain));
  CreateAccount createAccount;
  createAccount.account_name = "user1";
  createAccount.domain_id = "domain";
  createAccount.pubkey = pubkey1;
  txn.commands.push_back(std::make_shared<CreateAccount>(createAccount));

  Block block;
  block.transactions.push_back(txn);
  block.height = 1;
  block.prev_hash.fill(0);
  auto block1hash = hashProvider.get_hash(block);
  block.hash = block1hash;
  block.txs_number = block.transactions.size();

  {
    auto ms = storage->createMutableStorage();
    ms->apply(block, [](const auto &blk, auto &query, const auto &top_hash) {
      return true;
    });
    storage->commit(std::move(ms));
  }

  {
    auto account = wsv->getAccount(user1id);
    ASSERT_TRUE(account);
    ASSERT_EQ(account->account_id, user1id);
    ASSERT_EQ(account->domain_name, createAccount.domain_id);

    auto signatories = wsv->getSignatories(user1id);
    ASSERT_TRUE(signatories);
    ASSERT_EQ(signatories->size(), 1);
    ASSERT_EQ(signatories->at(0), pubkey1);
  }

  // 2nd tx (add sig2 to user1)
  txn = Transaction();
  txn.creator_account_id = user1id;
  auto addSignatory = AddSignatory();
  addSignatory.account_id = user1id;
  addSignatory.pubkey = pubkey2;
  txn.commands.push_back(std::make_shared<AddSignatory>(addSignatory));

  block = Block();
  block.transactions.push_back(txn);
  block.height = 2;
  block.prev_hash = block1hash;
  auto block2hash = hashProvider.get_hash(block);
  block.hash = block2hash;
  block.txs_number = block.transactions.size();

  {
    auto ms = storage->createMutableStorage();
    ms->apply(block, [](const auto &, auto &, const auto &) { return true; });
    storage->commit(std::move(ms));
  }

  {
    auto account = wsv->getAccount(user1id);
    ASSERT_TRUE(account);

    auto signatories = wsv->getSignatories(user1id);
    ASSERT_TRUE(signatories);
    ASSERT_EQ(signatories->size(), 2);
    ASSERT_EQ(signatories->at(0), pubkey1);
    ASSERT_EQ(signatories->at(1), pubkey2);
  }

  // 3rd tx (create user2 with pubkey1 that is same as user1's key)
  txn = Transaction();
  txn.creator_account_id = "admin2";
  createAccount = CreateAccount();
  createAccount.account_name = "user2";
  createAccount.domain_id = "domain";
  createAccount.pubkey = pubkey1;  // same as user1's pubkey1
  txn.commands.push_back(std::make_shared<CreateAccount>(createAccount));

  block = Block();
  block.transactions.push_back(txn);
  block.height = 3;
  block.prev_hash = block2hash;
  auto block3hash = hashProvider.get_hash(block);
  block.hash = block3hash;
  block.txs_number = block.transactions.size();

  {
    auto ms = storage->createMutableStorage();
    ms->apply(block, [](const auto &, auto &, const auto &) { return true; });
    storage->commit(std::move(ms));
  }

  {
    auto account1 = wsv->getAccount(user1id);
    ASSERT_TRUE(account1);

    auto account2 = wsv->getAccount(user2id);
    ASSERT_TRUE(account2);

    auto signatories1 = wsv->getSignatories(user1id);
    ASSERT_TRUE(signatories1);
    ASSERT_EQ(signatories1->size(), 2);
    ASSERT_EQ(signatories1->at(0), pubkey1);
    ASSERT_EQ(signatories1->at(1), pubkey2);

    auto signatories2 = wsv->getSignatories(user2id);
    ASSERT_TRUE(signatories2);
    ASSERT_EQ(signatories2->size(), 1);
    ASSERT_EQ(signatories2->at(0), pubkey1);
  }

  // 4th tx (remove pubkey1 from user1)
  txn = Transaction();
  txn.creator_account_id = user1id;
  auto removeSignatory = RemoveSignatory();
  removeSignatory.account_id = user1id;
  removeSignatory.pubkey = pubkey1;
  txn.commands.push_back(std::make_shared<RemoveSignatory>(removeSignatory));

  block = Block();
  block.transactions.push_back(txn);
  block.height = 4;
  block.prev_hash = block3hash;
  auto block4hash = hashProvider.get_hash(block);
  block.hash = block4hash;
  block.txs_number = block.transactions.size();

  {
    auto ms = storage->createMutableStorage();
    ms->apply(block, [](const auto &, auto &, const auto &) { return true; });
    storage->commit(std::move(ms));
  }

  {
    auto account = wsv->getAccount(user1id);
    ASSERT_TRUE(account);

    // user1 has only pubkey2.
    auto signatories1 = wsv->getSignatories(user1id);
    ASSERT_TRUE(signatories1);
    ASSERT_EQ(signatories1->size(), 1);
    ASSERT_EQ(signatories1->at(0), pubkey2);

    // user2 still has pubkey1.Amount
    auto signatories2 = wsv->getSignatories(user2id);
    ASSERT_TRUE(signatories2);
    ASSERT_EQ(signatories2->size(), 1);
    ASSERT_EQ(signatories2->at(0), pubkey1);
  }

  // 5th tx (add sig2 to user2 and set quorum = 1)
  txn = Transaction();
  txn.creator_account_id = user2id;
  addSignatory = AddSignatory();
  addSignatory.account_id = user2id;
  addSignatory.pubkey = pubkey2;
  txn.commands.push_back(std::make_shared<AddSignatory>(addSignatory));
  auto seqQuorum = SetQuorum();
  seqQuorum.account_id = user2id;
  seqQuorum.new_quorum = 2;
  txn.commands.push_back(std::make_shared<SetQuorum>(seqQuorum));

  block = Block();
  block.transactions.push_back(txn);
  block.height = 5;
  block.prev_hash = block4hash;
  auto block5hash = hashProvider.get_hash(block);
  block.hash = block5hash;
  block.txs_number = block.transactions.size();

  {
    auto ms = storage->createMutableStorage();
    ms->apply(block, [](const auto &, auto &, const auto &) { return true; });
    storage->commit(std::move(ms));
  }

  {
    auto account = wsv->getAccount(user2id);
    ASSERT_TRUE(account);
    ASSERT_EQ(account->quorum, 2);

    // user2 has pubkey1 and pubkey2.
    auto signatories = wsv->getSignatories(user2id);
    ASSERT_TRUE(signatories);
    ASSERT_EQ(signatories->size(), 2);
    ASSERT_EQ(signatories->at(0), pubkey1);
    ASSERT_EQ(signatories->at(1), pubkey2);
  }

  // 6th tx (remove sig2 fro user2: This must success)
  txn = Transaction();
  txn.creator_account_id = user2id;
  removeSignatory = RemoveSignatory();
  removeSignatory.account_id = user2id;
  removeSignatory.pubkey = pubkey2;
  txn.commands.push_back(std::make_shared<RemoveSignatory>(removeSignatory));

  block = Block();
  block.transactions.push_back(txn);
  block.height = 6;
  block.prev_hash = block5hash;
  auto block6hash = hashProvider.get_hash(block);
  block.hash = block6hash;
  block.txs_number = block.transactions.size();

  {
    auto ms = storage->createMutableStorage();
    ms->apply(block, [](const auto &, auto &, const auto &) { return true; });
    storage->commit(std::move(ms));
  }

  {
    // user2 only has pubkey1.
    auto signatories = wsv->getSignatories(user2id);
    ASSERT_TRUE(signatories);
    ASSERT_EQ(signatories->size(), 1);
    ASSERT_EQ(signatories->at(0), pubkey1);
  }
}

void Output(Transaction tx) {
  //      std::vector<Signature> signatures;
  //      ts64_t created_ts;
  //      std::string creator_account_id;
  //      uint64_t tx_counter;
  //      hash256_t tx_hash;
  //      std::vector<std::shared_ptr<Command>> commands;
  std::cout << "tx: " << tx.creator_account_id << "\n";
  std::cout << "commands:\n";
  for (auto e : tx.commands) {
    std::cout << (std::dynamic_pointer_cast<CreateDomain>(e)
                      ? "CreateDomain"
                      : std::dynamic_pointer_cast<CreateAccount>(e)
                          ? "CreateAccount"
                          : std::dynamic_pointer_cast<CreateAsset>(e)
                              ? "CreateAsset"
                              : std::dynamic_pointer_cast<AddAssetQuantity>(e)
                                  ? "AddAssetQuantity"
                                  : std::dynamic_pointer_cast<CreateAsset>(e)
                                      ? "TransferAsset"
                                      : "Else")
              << "\n";
  }
}

TEST_F(AmetsuchiTest, GetAccountAssetsTransactionsWithPagerTest) {
  HashProviderImpl hashProvider;

  auto storage =
      StorageImpl::create(block_store_path, redishost_, redisport_, pgopt_);
  ASSERT_TRUE(storage);
  auto wsv = storage->getWsvQuery();
  auto blocks = storage->getBlockQuery();

  const std::string admin = "admin";
  const std::string maindomain = "maindomain";
  const std::string adminid = "admin@maindomain";
  const std::string domain1name = "domain1";
  const std::string domain2name = "domain2";
  const std::string user1name = "alice";
  const std::string user2name = "bob";
  const std::string user3name = "charlie";
  const std::string user4name = "eve";
  const std::string user1id = "alice@domain1";
  const std::string user2id = "bob@domain1";
  const std::string user3id = "charlie@domain1";

  auto assign_dummy_tx_info = [&hashProvider](Transaction &txn) {
    txn.created_ts = 0;
    txn.tx_counter = 1;
    txn.tx_hash = hashProvider.get_hash(txn);
  };

  auto assign_dummy_block_info = [&hashProvider](Block &block,
                                                 iroha::hash256_t prev_hash) {
    block.created_ts = 0;
    block.height = 1;
    block.prev_hash.fill(0);
    block.merkle_root.fill(0);
    block.txs_number = static_cast<uint16_t>(block.transactions.size());
    hashProvider.get_hash(block);
  };

  {  //////////////////////////////////////////////////////////////////////////////////
    Block block;
    {
      // Given Transaction 1 CreateDomain, CreateAccount alice@domain1,
      // CreateAccount bob@domain1
      Transaction txn;
      txn.creator_account_id = adminid;
      txn.commands = {std::make_shared<CreateDomain>(domain1name),
                      std::make_shared<CreateAccount>(
                          user1name, domain1name, iroha::ed25519::pubkey_t{}),
                      std::make_shared<CreateAccount>(
                          user2name, domain1name, iroha::ed25519::pubkey_t{})};
      assign_dummy_tx_info(txn);
      block.transactions.push_back(txn);
    }
    {
      // Given Transaction 2 CreateAccount charlie@domain1
      Transaction txn;
      txn.creator_account_id = adminid;
      txn.commands = {std::make_shared<CreateAccount>(
          user3name, domain1name, iroha::ed25519::pubkey_t{})};
      assign_dummy_tx_info(txn);
      block.transactions.push_back(txn);
    }
    assign_dummy_block_info(block, iroha::hash256_t{});

    // When storing block into MutableStorage
    auto ms = storage->createMutableStorage();
    ms->apply(block, [](const auto &blk, auto &query, const auto &top_hash) {
      return true;
    });
    storage->commit(std::move(ms));
  }

  // Then Account alice@domain1 should be load.
  auto account1 = wsv->getAccount(user1id);
  ASSERT_TRUE(account1);
  ASSERT_STREQ((user1name + "@" + domain1name).c_str(),
               account1->account_id.c_str());

  // Then Account bob@domain1 should be load.
  auto account2 = wsv->getAccount(user2id);
  ASSERT_TRUE(account2);
  ASSERT_STREQ((user2name + "@" + domain1name).c_str(),
               account2->account_id.c_str());

  // Then Account charlie@domain1 should be load.
  auto account3 = wsv->getAccount(user3id);
  ASSERT_TRUE(account3);
  ASSERT_STREQ((user3name + "@" + domain1name).c_str(),
               account3->account_id.c_str());

  const std::string asset1name = "irh";
  const std::string asset1id = "irh@domain1";
  const std::string asset2name = "moeka";
  const std::string asset2id = "moeka@domain2";

  iroha::hash256_t txh6;

  {
    Block block;
    // Given Transaction 1: Admin applies CreateAsset irh@domain1, CreateAsset
    // moeka@domain2
    Transaction tx1;
    tx1.creator_account_id = adminid;
    tx1.commands = {std::make_shared<CreateAsset>(asset1name, domain1name, 2),
                    std::make_shared<CreateAsset>(asset2name, domain2name, 0)};
    assign_dummy_tx_info(tx1);
    block.transactions.push_back(tx1);

    // Given Transaction 2: Admin applies AddAssetQuantity with Alice's
    // irh@domain1 and moeka@domain2 wallet.
    Transaction tx2;
    tx2.creator_account_id = adminid;
    tx2.commands = {std::make_shared<AddAssetQuantity>(user1name, asset1id,
                                                       iroha::Amount(200, 0)),
                    std::make_shared<AddAssetQuantity>(user2name, asset2id,
                                                       iroha::Amount(200, 0))};
    assign_dummy_tx_info(tx2);
    block.transactions.push_back(tx2);

    // Given Transaction 3: Alice applies TransferAsset irh@domain1 from Alice
    // to Bob
    Transaction tx3;
    tx3.creator_account_id = user1id;
    tx3.commands = {std::make_shared<TransferAsset>(user1id, user2id, asset1id,
                                                    iroha::Amount(200, 0),
                                                    "[Tx 3] Alice -> Bob")};
    assign_dummy_tx_info(tx3);
    block.transactions.push_back(tx3);

    // Given Transaction 4: Bob applies TransferAsset irh@domain1 from Bob to
    // Chalie
    Transaction tx4;
    tx4.creator_account_id = user2id;
    tx4.commands = {std::make_shared<TransferAsset>(user2id, user3id, asset1id,
                                                    iroha::Amount(150, 0),
                                                    "[Tx 4] Bob -> Charlie")};
    assign_dummy_tx_info(tx4);
    block.transactions.push_back(tx4);

    // Given Transaction 5: Charlie applies TransferAsset irh@domain1 from
    // Chalie to Alice.
    Transaction tx5;
    tx5.creator_account_id = user3id;
    tx5.commands = {std::make_shared<TransferAsset>(user3id, user1id, asset1id,
                                                    iroha::Amount(100, 0),
                                                    "[Tx 5] Charlie -> Alice")};
    assign_dummy_tx_info(tx5);
    block.transactions.push_back(tx5);

    // Given Transaction 6: Alice applies TransferAsset irh@domain1 from Alice
    // to Bob.
    Transaction tx6;
    tx6.creator_account_id = user3id;
    tx6.commands = {std::make_shared<TransferAsset>(user3id, user1id, asset1id,
                                                    iroha::Amount(50, 0),
                                                    "[Tx 6] Alice -> Bob")};
    assign_dummy_tx_info(tx6);
    block.transactions.push_back(tx6);
    txh6 = hashProvider.get_hash(tx6);

    blocks->getTopBlocks(1).subscribe([&](auto blk) {
      assign_dummy_block_info(block, hashProvider.get_hash(blk));
    });

    // When storing the block into MutableStorage
    auto ms = storage->createMutableStorage();
    ms->apply(block, [](const auto &blk, auto &query, const auto &top_hash) {
      return true;
    });
    storage->commit(std::move(ms));

    {
      bool passed = false;
      // When, query empty hash with id: Alice, limit: 0.
      blocks
          ->getAccountAssetsTransactionsWithPager(user1id, {asset1id},
                                                  iroha::hash256_t{}, 0)
          .subscribe([&passed](auto tx) {
            std::cout << "----------\n";
            Output(tx);
            std::cout << "----------\n";
            (void)tx;  // avoid from warning unused variable.
            passed = true;
          });
      // Then returns null.
      ASSERT_FALSE(passed);
    }

    {
      Transaction tx_response;
      // When, query empty hash with id: Alice, limit: 1.
      blocks
          ->getAccountAssetsTransactionsWithPager(user1id, {asset1id},
                                                  iroha::hash256_t{}, 1)
          .subscribe([&tx_response](auto tx) {
            std::cout << "ok\n";
            Output(tx);
            tx_response = tx;
          });
      // Then, returns the Top tx that meets an asset operation related to
      // Alice.
      Output(tx6);
      Output(tx_response);
      ASSERT_EQ("[Tx 6] Alice -> Bob",
                std::dynamic_pointer_cast<TransferAsset>(tx6.commands[0])
                    ->description);
      ASSERT_EQ(tx6, tx_response);
    }

    {
      std::vector<Transaction> tx_responses;
      // When querying empty hash with id: Alice, limit: 2.
      blocks
          ->getAccountAssetsTransactionsWithPager(user1id, {asset1id},
                                                  iroha::hash256_t{}, 2)
          .subscribe([&tx_responses](auto tx) { tx_responses.push_back(tx); });
      // Then, returns 2 Top txs that meet asset operations related to Alice.
      ASSERT_EQ(2, tx_responses.size());
      ASSERT_EQ("[Tx 6] Alice -> Bob",
                std::dynamic_pointer_cast<TransferAsset>(tx6.commands[0])
                    ->description);
      ASSERT_EQ(tx6, tx_responses[0]);
      ASSERT_EQ("[Tx 5] Charlie -> Alice",
                std::dynamic_pointer_cast<TransferAsset>(tx5.commands[0])
                    ->description);
      ASSERT_EQ(tx5, tx_responses[1]);
    }

    {
      std::vector<Transaction> tx_responses;
      // When querying empty hash with id: Alice, limit: 100 (over max acct
      // asset txs).
      blocks
          ->getAccountAssetsTransactionsWithPager(user1id, {asset1id},
                                                  iroha::hash256_t{}, 100)
          .subscribe([&tx_responses](auto tx) { tx_responses.push_back(tx); });
      // Then, returns all txs that meet asset operations related to Alice.
      ASSERT_EQ(4, tx_responses.size());
      ASSERT_EQ("[Tx 6] Alice -> Bob",
                std::dynamic_pointer_cast<TransferAsset>(tx6.commands[0])
                    ->description);
      ASSERT_EQ(tx6, tx_responses[0]);
      ASSERT_EQ("[Tx 5] Charlie -> Alice",
                std::dynamic_pointer_cast<TransferAsset>(tx5.commands[0])
                    ->description);
      ASSERT_EQ(tx5, tx_responses[1]);
      ASSERT_EQ("[Tx 3] Alice -> Bob",
                std::dynamic_pointer_cast<TransferAsset>(tx3.commands[0])
                    ->description);
      ASSERT_EQ(tx3, tx_responses[2]);
      ASSERT_TRUE(std::dynamic_pointer_cast<AddAssetQuantity>(tx2.commands[0]));
      ASSERT_EQ(tx2, tx_responses[3]);
    }

    {
      bool passed = false;
      // When querying top Transaction 6 hash with id: Alice, limit: 0.
      blocks
          ->getAccountAssetsTransactionsWithPager(user1id, {asset1id}, txh6, 0)
          .subscribe([&passed](auto tx) {
            (void)tx;  // avoid from warning unused variable.
            passed = true;
          });
      // Then returns null.
      ASSERT_FALSE(passed);
    }

    {
      Transaction tx_response;
      // When querying top Transaction 6 hash with id: Alice, limit: 1.
      blocks
          ->getAccountAssetsTransactionsWithPager(user1id, {asset1id}, txh6, 1)
          .subscribe([&tx_response](auto tx) { tx_response = tx; });
      // Then returns the second Top most tx that meet Alice's irh@domain1 (txh4
      // is excluded).
      ASSERT_EQ(tx5, tx_response);
    }
    {
      std::vector<Transaction> tx_responses;
      // When querying top Transaction 6 hash with id: Alice, limit: 100.
      blocks
          ->getAccountAssetsTransactionsWithPager(user1id, {asset1id}, txh6,
                                                  100)
          .subscribe([&tx_responses](auto tx) { tx_responses.push_back(tx); });
      // Then returns all txs without a tx which txh6 has excluded, that are
      // related to Alice's irh@domain1.
      ASSERT_EQ(3, tx_responses.size());
      ASSERT_EQ(tx5, tx_responses[0]);  // TransferAsset
      ASSERT_EQ(tx3, tx_responses[1]);  // TrasnferAsset
      ASSERT_EQ(tx2, tx_responses[2]);  // AddAssetQuantity
    }
  }

  {
    Block block;
    // Given Transaction 1: includes AddAssetQuantity and TransferAsset with
    // irrelevant commands.
    Transaction tx1;
    tx1.creator_account_id = adminid;
    tx1.commands = {
        // Given Transaction 2: includes AddAssetQuantity and TransferAsset
        // with irrelevant commands.
        std::make_shared<AddAssetQuantity>(asset1name, domain1name,
                                           iroha::Amount(1000, 0)),
        std::make_shared<AddAssetQuantity>(asset2name, domain2name,
                                           iroha::Amount(1000, 0)),
        std::make_shared<CreateAsset>("unfilteredasset", domain1name, 0),
        std::make_shared<TransferAsset>(user1id, user2id, asset1id,
                                        iroha::Amount(200, 0))};
    assign_dummy_tx_info(tx1);
    block.transactions.push_back(tx1);

    blocks->getTopBlocks(1).subscribe([&](auto blk) {
      assign_dummy_block_info(block, hashProvider.get_hash(blk));
    });

    // When storing block into MutableStorage
    auto ms = storage->createMutableStorage();
    ms->apply(block, [](const auto &blk, auto &query, const auto &top_hash) {
      return true;
    });
    storage->commit(std::move(ms));

    {
      // When query tx that has multiple assets from Top block.
      Transaction tx_response;
      blocks
          ->getAccountAssetsTransactionsWithPager(
              user1id, {asset1name, asset2name}, iroha::hash256_t{}, 3)
          .subscribe([&tx_response](auto tx) { tx_response = tx; });

      // Then, returns the tx.
      ASSERT_EQ(tx1, tx_response);
    }
  }
}
