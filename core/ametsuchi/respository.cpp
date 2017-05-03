/**
 * Copyright Soramitsu Co., Ltd. 2016 All Rights Reserved.
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

#include <infra/ametsuchi/include/ametsuchi/ametsuchi.h>
#include <service/flatbuffer_service.h>
#include <memory>
#include <service/connection.hpp>
#include <main_generated.h>
#include <endpoint_generated.h>
#include <asset_generated.h>

namespace repository {

    static std::unique_ptr<ametsuchi::Ametsuchi> db;

    void init() {
        db.reset(new ametsuchi::Ametsuchi("/tmp/"));
    }

    void append(const iroha::Transaction &tx) {
        if (db == nullptr) init();

        auto buf = flatbuffer_service::transaction::GetTxPointer(tx);
        db->append(&buf.value());
    }

    std::vector<const iroha::Asset *> findAssetByPublicKey(const flatbuffers::String &key) {
        if (db == nullptr) init();
        flatbuffers::FlatBufferBuilder fbb;
        return db->accountGetAllAssets(&key);
    }

    bool existAccountOf(const flatbuffers::String &key) {
        if (db == nullptr) init();
        return false;
    }

    bool checkUserCanPermission(const flatbuffers::String &key) {
        if (db == nullptr) init();

        return false;
    }

    const std::string getMerkleRoot() {
        if (db == nullptr) init();
        auto buf = db->getMerkleRoot();
        auto mr = flatbuffers::GetRoot<flatbuffers::String>(buf.data());
        return mr->str();
    }

    namespace permission {

        std::vector<const iroha::AccountPermissionLedger*> getPermissionLedgerOf(const flatbuffers::String &key) {
            if (db == nullptr) init();
            return db->assetGetPermissionLedger(&key);
        }

        std::vector<const iroha::AccountPermissionDomain*> getPermissionDomainOf(const flatbuffers::String &key) {
            if (db == nullptr) init();
            return db->assetGetPermissionDomain(&key);
        }

        std::vector<const iroha::AccountPermissionAsset*> getPermissionAssetOf(const flatbuffers::String &key){
            if (db == nullptr) init();
            return db->assetGetPermissionAsset(&key);
        }

    }

    namespace front_repository{
        void initialize_repository(){
            connection::iroha::AssetRepositoryImpl::AccountGetAsset::receive([=](
                const std::string & /* from */, flatbuffers::unique_ptr_t &&query_ptr) -> std::vector<const ::iroha::Asset *>{
                if(db == nullptr) init();
                const iroha::AssetQuery& query = *flatbuffers::GetRoot<iroha::AssetQuery>(query_ptr.get());
                auto ln = query.ledger_name();
                auto dn = query.domain_name();
                auto an = query.asset_name();
                if(ln == nullptr || dn == nullptr || an == nullptr) {
                    return db->accountGetAllAssets(query.pubKey(), query.uncommitted());
                }else{
                    std::vector<const ::iroha::Asset *> res{db->accountGetAsset(query.pubKey(), ln, dn, an, query.uncommitted())};
                    return res;
                }
            });
        }
    }
};