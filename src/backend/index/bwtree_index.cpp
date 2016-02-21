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

template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::BWTreeIndex(
    IndexMetadata *metadata)
    : Index(metadata),
      container(KeyComparator(metadata), KeyEqualityChecker(metadata), value_equals),
      equals(metadata),      
      comparator(metadata) {
  // Add your implementation here
}

template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::~BWTreeIndex() {
  // Add your implementation here
}

template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
bool BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::InsertEntry(
    // __attribute__((unused)) const storage::Tuple *key, __attribute__((unused)) const ItemPointer location) {
    const storage::Tuple *key,const ItemPointer location) {
  KeyType index_key;
  index_key.SetFromKey(key);
  container.Insert(index_key, location);
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
bool BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::DeleteEntry(
    __attribute__((unused)) const storage::Tuple *key, __attribute__((unused)) const ItemPointer location) {
  // Add your implementation here
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
vector<ItemPointer>
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::Scan(
    __attribute__((unused)) const vector<Value> &values,
    __attribute__((unused)) const vector<oid_t> &key_column_ids,
    __attribute__((unused)) const vector<ExpressionType> &expr_types,
    __attribute__((unused)) const ScanDirectionType& scan_direction) {
  vector<ItemPointer> result;
  // Add your implementation here
  return result;
}

template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
vector<ItemPointer>
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::ScanAllKeys() {
  vector<ItemPointer> result;
  // Add your implementation here
  return result;
}

/**
 * @brief Return all locations related to this key.
 */
template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
vector<ItemPointer>
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::ScanKey(
    __attribute__((unused)) const storage::Tuple *key) {
  vector<ItemPointer> result;
  // Add your implementation here
  return result;
}

template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
string
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker>::GetTypeName() const {
  return "BWTree";
}

// // Explicit template instantiation
template class BWTreeIndex<IntsKey<1>, ItemPointer, IntsComparator<1>,
IntsEqualityChecker<1>, ItemPointerEqualityChecker>;
template class BWTreeIndex<IntsKey<2>, ItemPointer, IntsComparator<2>,
IntsEqualityChecker<2>, ItemPointerEqualityChecker>;
template class BWTreeIndex<IntsKey<3>, ItemPointer, IntsComparator<3>,
IntsEqualityChecker<3>, ItemPointerEqualityChecker>;
template class BWTreeIndex<IntsKey<4>, ItemPointer, IntsComparator<4>,
IntsEqualityChecker<4>, ItemPointerEqualityChecker>;

template class BWTreeIndex<GenericKey<4>, ItemPointer, GenericComparator<4>,
GenericEqualityChecker<4>, ItemPointerEqualityChecker>;
template class BWTreeIndex<GenericKey<8>, ItemPointer, GenericComparator<8>,
GenericEqualityChecker<8>, ItemPointerEqualityChecker>;
template class BWTreeIndex<GenericKey<12>, ItemPointer, GenericComparator<12>,
GenericEqualityChecker<12>, ItemPointerEqualityChecker>;
template class BWTreeIndex<GenericKey<16>, ItemPointer, GenericComparator<16>,
GenericEqualityChecker<16>, ItemPointerEqualityChecker>;
template class BWTreeIndex<GenericKey<24>, ItemPointer, GenericComparator<24>,
GenericEqualityChecker<24>, ItemPointerEqualityChecker>;
template class BWTreeIndex<GenericKey<32>, ItemPointer, GenericComparator<32>,
GenericEqualityChecker<32>, ItemPointerEqualityChecker>;
template class BWTreeIndex<GenericKey<48>, ItemPointer, GenericComparator<48>,
GenericEqualityChecker<48>, ItemPointerEqualityChecker>;
template class BWTreeIndex<GenericKey<64>, ItemPointer, GenericComparator<64>,
GenericEqualityChecker<64>, ItemPointerEqualityChecker>;
template class BWTreeIndex<GenericKey<96>, ItemPointer, GenericComparator<96>,
GenericEqualityChecker<96>, ItemPointerEqualityChecker>;
template class BWTreeIndex<GenericKey<128>, ItemPointer, GenericComparator<128>,
GenericEqualityChecker<128>, ItemPointerEqualityChecker>;
template class BWTreeIndex<GenericKey<256>, ItemPointer, GenericComparator<256>,
GenericEqualityChecker<256>, ItemPointerEqualityChecker>;
template class BWTreeIndex<GenericKey<512>, ItemPointer, GenericComparator<512>,
GenericEqualityChecker<512>, ItemPointerEqualityChecker>;

template class BWTreeIndex<TupleKey, ItemPointer, TupleKeyComparator,
TupleKeyEqualityChecker, ItemPointerEqualityChecker>;

}  // End index namespace
}  // End peloton namespace
