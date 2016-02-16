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
typedef uint64_t epoch_t;
/**
 * BW tree-based index implementation.
 *
 * @see Index
 */
class CASMappingTable {
  private:
  std::map<uint64_t, std::pair<void *, uint32_t>> cas_mapping_table; // should be capable of mapping to internal and leaf bw nodes and delta nodes of any type

  public:
  CASMappingTable();
  bool Install(uint64_t id, void *node_ptr, uint32_t chain_length); // install into mapping table via compare and swap
  std::pair<void *, uint32_t> Get(uint64_t id);
  uint64_t get_next_id();
};

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class BWTreeIndex : public Index {
  friend class IndexFactory;

  typedef BWTree<KeyType, ValueType, KeyComparator> MapType;

 public:
  BWTreeIndex(IndexMetadata *metadata);

  ~BWTreeIndex();

  bool InsertEntry(const storage::Tuple *key, const ItemPointer location); // rajat

  bool DeleteEntry(const storage::Tuple *key, const ItemPointer location); // saurabh

  std::vector<ItemPointer> Scan(const std::vector<Value> &values,
                                const std::vector<oid_t> &key_column_ids,
                                const std::vector<ExpressionType> &expr_types,
                                const ScanDirectionType& scan_direction); // saurabh

  std::vector<ItemPointer> ScanAllKeys(); // saurabh

  std::vector<ItemPointer> ScanKey(const storage::Tuple *key); // saurabh

  std::string GetTypeName() const;

  // TODO: Implement this
  bool Cleanup() {
    return true;
  }

  // TODO: Implement this
  size_t GetMemoryFootprint() {
    return 0;
  }

  CASMappingTable *table;
 protected:
  // container
  MapType container;

  // equality checker and comparator
  KeyEqualityChecker equals;
  KeyComparator comparator;

  // synch helper
  RWLock index_lock;

 private:
  uint32_t min_node_size;
  uint32_t max_node_size;
  uint64_t root; // root points to an id in the mapping table
  bool ConsolidateNode(uint64_t id); // id is that of the mapping table entry
  bool SplitNode(uint64_t id, KeyType k, ValueType v); // id of the node to split and the new key, value to be inserted - rajat
  bool MergeNodes(uint64_t n1, uint64_t n2); // saurabh
  void * CreateNode(uint64_t id, node_type_t t); // for creating when consolidating
  bool DeleteNode(uint64_t id);
  // tianyuan - GC and the epoch mechanism
};


}  // End index namespace
}  // End peloton namespace
