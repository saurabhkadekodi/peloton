//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// btree_index.cpp
//
// Identification: src/backend/index/btree_index.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/logger.h"
#include "backend/index/bwtree_index.h"
#include "backend/index/index_key.h"
#include "backend/storage/tuple.h"
#include "malloc.h"
namespace peloton {
namespace index {
using namespace std;

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::BWTreeIndex(
    IndexMetadata *metadata)
    : Index(metadata),
      container(metadata, KeyComparator(metadata), KeyEqualityChecker(metadata),
                value_equals, 10),
      equals(metadata),
      comparator(metadata) {}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
BWTreeIndex<KeyType, ValueType, KeyComparator,
            KeyEqualityChecker>::~BWTreeIndex() {}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
bool BWTreeIndex<KeyType, ValueType, KeyComparator,
                 KeyEqualityChecker>::InsertEntry(const storage::Tuple *key,
                                                  const ItemPointer location) {
  KeyType index_key;
  index_key.SetFromKey(key);
  return container.InsertWrapper(index_key, location);
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
bool BWTreeIndex<KeyType, ValueType, KeyComparator,
                 KeyEqualityChecker>::DeleteEntry(const storage::Tuple *key,
                                                  const ItemPointer location) {
  KeyType index_key;
  index_key.SetFromKey(key);
  return container.DeleteWrapper(index_key, location);
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
vector<ItemPointer>
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Scan(
    const vector<Value> &values, const vector<oid_t> &key_column_ids,
    const vector<ExpressionType> &expr_types,
    const ScanDirectionType &scan_direction) {
  return container.ScanWrapper(values, key_column_ids, expr_types,
                               scan_direction);
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
vector<ItemPointer> BWTreeIndex<KeyType, ValueType, KeyComparator,
                                KeyEqualityChecker>::ScanAllKeys() {
  return container.ScanAllKeysWrapper();
}

/**
 * @brief Return all locations related to this key.
 */
template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
vector<ItemPointer>
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanKey(
    const storage::Tuple *key) {
  KeyType index_key;
  index_key.SetFromKey(key);
  return container.SearchKeyWrapper(index_key);
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
size_t BWTreeIndex<KeyType, ValueType, KeyComparator,
                   KeyEqualityChecker>::GetMemoryFootprint() {
  return container.memory_usage;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
string BWTreeIndex<KeyType, ValueType, KeyComparator,
                   KeyEqualityChecker>::GetTypeName() const {
  return "BWTree";
}

// // Explicit template instantiation
template class BWTreeIndex<IntsKey<1>, ItemPointer, IntsComparator<1>,
                           IntsEqualityChecker<1>>;
template class BWTreeIndex<IntsKey<2>, ItemPointer, IntsComparator<2>,
                           IntsEqualityChecker<2>>;
template class BWTreeIndex<IntsKey<3>, ItemPointer, IntsComparator<3>,
                           IntsEqualityChecker<3>>;
template class BWTreeIndex<IntsKey<4>, ItemPointer, IntsComparator<4>,
                           IntsEqualityChecker<4>>;

template class BWTreeIndex<GenericKey<4>, ItemPointer, GenericComparator<4>,
                           GenericEqualityChecker<4>>;
template class BWTreeIndex<GenericKey<8>, ItemPointer, GenericComparator<8>,
                           GenericEqualityChecker<8>>;
template class BWTreeIndex<GenericKey<12>, ItemPointer, GenericComparator<12>,
                           GenericEqualityChecker<12>>;
template class BWTreeIndex<GenericKey<16>, ItemPointer, GenericComparator<16>,
                           GenericEqualityChecker<16>>;
template class BWTreeIndex<GenericKey<24>, ItemPointer, GenericComparator<24>,
                           GenericEqualityChecker<24>>;
template class BWTreeIndex<GenericKey<32>, ItemPointer, GenericComparator<32>,
                           GenericEqualityChecker<32>>;
template class BWTreeIndex<GenericKey<48>, ItemPointer, GenericComparator<48>,
                           GenericEqualityChecker<48>>;
template class BWTreeIndex<GenericKey<64>, ItemPointer, GenericComparator<64>,
                           GenericEqualityChecker<64>>;
template class BWTreeIndex<GenericKey<96>, ItemPointer, GenericComparator<96>,
                           GenericEqualityChecker<96>>;
template class BWTreeIndex<GenericKey<128>, ItemPointer, GenericComparator<128>,
                           GenericEqualityChecker<128>>;
template class BWTreeIndex<GenericKey<256>, ItemPointer, GenericComparator<256>,
                           GenericEqualityChecker<256>>;
template class BWTreeIndex<GenericKey<512>, ItemPointer, GenericComparator<512>,
                           GenericEqualityChecker<512>>;

template class BWTreeIndex<TupleKey, ItemPointer, TupleKeyComparator,
                           TupleKeyEqualityChecker>;

}  // End index namespace
}  // End peloton namespace
