//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// btree_index.h
//
// Identification: src/backend/index/btree_index.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <string>
#include <map>

#include "backend/catalog/manager.h"
#include "backend/common/platform.h"
#include "backend/common/types.h"
#include "backend/index/index.h"

#include "backend/index/bwtree.h"

namespace peloton {
namespace index {
using namespace std;
class ItemPointerEqualityChecker;

template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker, typename ValueEqualityChecker>
class BWTreeIndex : public Index {
  friend class IndexFactory;

  typedef BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueEqualityChecker> MapType;

 public:
  BWTreeIndex(IndexMetadata *metadata);

  ~BWTreeIndex();

  bool InsertEntry(const storage::Tuple *key, const ItemPointer location); // rajat

  bool DeleteEntry(const storage::Tuple *key, const ItemPointer location); // saurabh

  vector<ItemPointer> Scan(const vector<Value> &values,
                                const vector<oid_t> &key_column_ids,
                                const vector<ExpressionType> &expr_types,
                                const ScanDirectionType& scan_direction); // saurabh

  vector<ItemPointer> ScanAllKeys(); // saurabh

  vector<ItemPointer> ScanKey(const storage::Tuple *key); // saurabh

  string GetTypeName() const;

  uint64_t tree_height;

  // TODO: Implement this
  bool Cleanup() {
    return true;
  }

  // TODO: Implement this
  size_t GetMemoryFootprint() {
    return 0;
  }

 protected:
  // container
  MapType container;

  // equality checker and comparator
  KeyEqualityChecker equals;
  KeyComparator comparator;
  ValueEqualityChecker value_equals;
  // synch helper
  RWLock index_lock;
};

class ItemPointerEqualityChecker {
private:
  /* data */
public:
    inline bool operator()(const ItemPointer &lhs,
                         const ItemPointer &rhs) const {
      return (lhs.first == rhs.first) && (lhs.second == rhs.second);
  }
  ItemPointerEqualityChecker() = default;
};

}  // End index namespace
}  // End peloton namespace
