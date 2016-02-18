//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// BWTree.h
//
// Identification: src/backend/index/BWTree.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

namespace peloton {
namespace index {
using namespace std;

typedef uint64_t epoch_t;

typedef enum node_type {
  INTERNAL_BW_NODE,
  LEAF_BW_NODE,
  INSERT,
  DELETE,
  SPLIT,
  MERGE,
  REMOVE;
  SPLIT_INDEX,
  REMOVE_INDEX
} node_type_t;

class CASMappingTable {
  private:
  map<uint64_t, pair<Node*, uint32_t>> cas_mapping_table; // should be capable of mapping to internal and leaf bw nodes and delta nodes of any type

  public:
  CASMappingTable() {};
  bool Install(uint64_t id, Node* node_ptr, uint32_t chain_length); // install into mapping table via compare and swap
  pair<Node*, uint32_t> Get(uint64_t id);
  uint64_t Get_next_id();
};

// Look up the stx btree interface for background.
// peloton/third_party/stx/btree.h
/**
 * BW tree-based index implementation.
 *
 * @see Index
 */
template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class BWTree {
  friend class BWTreeIndex;
  CASMappingTable& table;
 private:
  uint32_t min_node_size;
  uint32_t max_node_size;
  uint64_t root; // root points to an id in the mapping table
  bool Consolidate(uint64_t id, bool force) {} // id is that of the mapping table entry
  bool SplitNode(uint64_t id, KeyType k, ValueType v){} // id of the node to split and the new key, value to be inserted - rajat
  bool MergeNodes(uint64_t n1, uint64_t n2){} // saurabh
  void * CreateNode(uint64_t id, node_type_t t){} // for creating when consolidating
  bool DeleteNode(uint64_t id){}
  // tianyuan - GC and the epoch mechanism
  bool Insert(KeyType key, ValueType value);


};

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class Node {
protected:
  epoch_t generation;
  uint64_t id;
  const BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>& my_tree; // reference of the tree I belong to
  Node *next;
  Node(const BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>& bwt, uint64_t id, node_type_t type) :
  my_tree(bwt), id(id), type(type) {
    next = nullptr;
  }
  ~Node() {
    next -> ~Node();
  }
public:
  node_type_t type;
};

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class InternalBWNode : Node {
  vector<pair<KeyType, uint64_t>> key_list; // all keys have children

  public:
  uint64_t sibling_id;
  InternalBWNode(const BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>& bwt, uint64_t id) :
  Node(bwt, id, INTERNAL_BW_NODE) {}
  bool Insert(uint64_t id, KeyType split_key, KeyType boundary_key, uint64_t new_node_id);
  bool Split(uint64_t id, uint64_t *path, uint64_t index, KeyType split_key, KeyType boundary_key, uint64_t new_node_id);
  uint64_t Get_child_id(KeyType key);
};

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class LeafBWNode : Node {
  vector<pair<KeyType, ValueType>> kv_list; // all key value pairs

  public:
  uint64_t sibling_id;
  LeafBWNode(const BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>& bwt, uint64_t id) :
  Node(bwt, id, LEAF_BW_NODE) {}
  uint64_t Get_size();
  bool Insert(KeyType key, ValueType value);
  bool Split_node(uint64_t id, uint64_t *path, uint64_t index, KeyType key, ValueType value);
};

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class DeltaNode : Node {
  KeyType key;
  ValueType val;
  public:
  DeltaNode(const BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>& bwt, uint64_t id) :
  Node(bwt, id, INSERT) {} // Default is INSERT type
};

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class SplitIndexDeltaNode : Node {
  public:
  SplitIndexDeltaNode(const BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>& bwt, uint64_t id) :
  Node(bwt, id, SPLIT_INDEX) {}
  KeyType split_key, boundary_key;
  uint64_t new_split_node_id;
};

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class RemoveIndexDeltaNode : Node { 
  public:
  uint64_t id;
  RemoveIndexDeltaNode(const BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>& bwt, uint64_t id) :
  Node(bwt, id, REMOVE_INDEX) {}
  KeyType deleted_key;
};

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class SplitDeltaNode : Node {
  public:
  SplitDeltaNode(const BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>& bwt, uint64_t id) :
  Node(bwt, id, SPLIT) {}
  KeyType split_key;
  uint64_t target_node_id; // pointer to target node for preventing blocking
};

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class RemoveDeltaNode : Node {
  public:
  KeyType deleted_key; 
  Node *node_to_be_removed; // can be delta node or internal_bw_node or leaf_bw_node
  RemoveDeltaNode(const BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>& bwt, uint64_t id) :
  Node(bwt, id, REMOVE) {}
};

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class MergeDeltaNode : Node {
  public:
  MergeDeltaNode(const BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>& bwt, uint64_t id) :
  Node(bwt, id, MERGE) {}
  KeyType MergeKey;
  Node *node_to_be_merged;
};
}  // End index namespace
}  // End peloton namespace
