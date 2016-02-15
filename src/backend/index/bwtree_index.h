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
typedef enum node_type {
  INTERNAL_BW_NODE,
  LEAF_BW_NODE,
  INSERT,
  UPDATE,
  DELETE,
  SPLIT,
  MERGE,
  NODE_DELETE,
  SPLIT_INDEX,
  DELETE_INDEX
} node_type_t;

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
  CASMappingTable *table;
  uint64_t root; // root points to an id in the mapping table
  bool ConsolidateNode(uint64_t id); // id is that of the mapping table entry
  bool SplitNode(uint64_t id, key_t k); // id of the node to split at key k - rajat
  bool MergeNodes(uint64_t n1, uint64_t n2); // saurabh
  void * CreateNode(uint64_t id, node_type_t t); // for creating when consolidating
  bool DeleteNode(uint64_t id);
  // tianyuan - GC and the epoch mechanism
};

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class InternalBWNode {
  private:
  uint64_t id;
  epoch_t generation;
  uint64_t sibling_id;
  std::vector<std::pair<KeyType, uint64_t>> key_list; // all keys have children
  BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *my_tree; // reference of the tree I belong to

  public:
  InternalBWNode(BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *bwt, uint64_t id);
};

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class LeafBWNode {
  private:
  uint64_t id;
  epoch_t generation;
  uint64_t sibling_id;
  std::vector<std::pair<KeyType, ValueType>> kv_list; // all key value pairs
  BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *my_tree; // reference of the tree I belong to

  public:
  LeafBWNode(BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *bwt, uint64_t id);
};

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class SimpleDeltaNode {
  private:
  uint64_t id;
  epoch_t generation;
  int type; // delete / insert / update
  KeyType key;
  ValueType val;
  void *next; // can be delta node or internal_bw_node or leaf_bw_node
  BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *my_tree; // reference of the tree I belong to

  public:
  SimpleDeltaNode(BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *bwt, uint64_t id);
};

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class SplitIndexDeltaNode {
	private:
  uint64_t id;
  epoch_t generation;
  KeyType split_key;
  void *split_parent; // can be delta node or internal_bw_node
  uint64_t new_split_node_id;
  BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *my_tree; // reference of the tree I belong to

  public:
  SplitIndexDeltaNode(BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *bwt, uint64_t id);
};

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class DeleteIndexDeltaNode {
  private:
  uint64_t id;
  epoch_t generation;
  KeyType deleted_key;
  void *split_parent; // can be delta node or internal_bw_node
  BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *my_tree; // reference of the tree I belong to
 
  public:
  DeleteIndexDeltaNode(BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *bwt, uint64_t id);
};

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class SplitDeltaNode {
  uint64_t id;
  epoch_t generation;
  KeyType split_key;
  void *next; // can be delta node or internal_bw_node or leaf_bw_node
  uint64_t target_node_id; // pointer to target node for preventing blocking
  BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *my_tree; // reference of the tree I belong to

  public:
  SplitDeltaNode(BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *bwt, uint64_t id);
};

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class RemoveDeltaNode {
  uint64_t id;
  epoch_t generation;
  KeyType deleted_key; 
  void *node_to_be_removed; // can be delta node or internal_bw_node or leaf_bw_node
  BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *my_tree; // reference of the tree I belong to

  public:
  RemoveDeltaNode(BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *bwt, uint64_t id);
};

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class MergeDeltaNode {
  uint64_t id;
  epoch_t generation;
  void *next; // can be delta node or internal_bw_node or leaf_bw_node
  void *node_to_be_merged;
  BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *my_tree; // reference of the tree I belong to

  public:
  MergeDeltaNode(BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *bwt, uint64_t id);
};

}  // End index namespace
}  // End peloton namespace
