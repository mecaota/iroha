#ifndef CORE_DOMAIN_SIGNATORYDELETETRANSACTION_HPP_
#define CORE_DOMAIN_SIGNATORYDELETETRANSACTION_HPP_

#include <msgpack.hpp>

namespace SignatoryDeleteTransaction {
    MSGPACK_DEFINE(hash, type, accountPublicKey, signerToDeletePublicKey);
};

#endif  // CORE_DOMAIN_SIGNATORYDELETETRANSACTION_HPP_