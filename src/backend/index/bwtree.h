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
#include <cstdint>
#include <assert.h>
#include <vector>
#include <map>
#include <utility>

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
  REMOVE,
  SPLIT_INDEX,
  REMOVE_INDEX
} node_type_t;

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class BWTree;

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
class CASMappingTable {
  typedef Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker> NodeType;

  private:
  map<uint64_t, pair<void*, uint32_t>> cas_mapping_table; // should be capable of mapping to internal and leaf bw nodes and delta nodes of any type

  public:
  CASMappingTable() {};
  bool Install(uint64_t id, Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_ptr, uint32_t chain_length); // install into mapping table via compare and swap
  pair<Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*, uint32_t> Get(uint64_t id);
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
  // friend class BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>;
  CASMappingTable<KeyType, ValueType, KeyComparator, KeyEqualityChecker>& table;
 private:
  BWTree() {}
  // BWTree(CASMappingTable<KeyType, ValueType, KeyComparator, KeyEqualityChecker>& table) : table(table){}
  uint32_t min_node_size;
  uint32_t max_node_size;
  uint64_t root; // root points to an id in the mapping table
  bool Consolidate(uint64_t id, bool force) {return false;} // id is that of the mapping table entry
  bool SplitNode(uint64_t id, KeyType k, ValueType v){return false;} // id of the Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker> to split and the new key, value to be inserted - rajat
  bool MergeNodes(uint64_t n1, uint64_t n2){return false;} // saurabh
  void * CreateNode(uint64_t id, node_type_t t){return nullptr;} // for creating when consolidating
  bool DeleteNode(uint64_t id){return false;}
  // tianyuan - GC and the epoch mechanism
  bool Insert(KeyType key, ValueType value);


};

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class InternalBWNode : Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker> {
  vector<pair<KeyType, uint64_t>> key_list; // all keys have children

  public:
  uint64_t sibling_id;
  InternalBWNode(const BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>& bwt, uint64_t id) :
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(bwt, id, INTERNAL_BW_NODE) {}
  bool Insert(uint64_t id, KeyType split_key, KeyType boundary_key, uint64_t new_node_id);
  bool Split(uint64_t id, uint64_t *path, uint64_t index, KeyType split_key, KeyType boundary_key, uint64_t new_node_id);
  uint64_t Get_child_id(KeyType key);
};

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class LeafBWNode : Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker> {
  vector<pair<KeyType, ValueType>> kv_list; // all key value pairs

  public:
  uint64_t sibling_id;
  LeafBWNode(const BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>& bwt, uint64_t id) :
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(bwt, id, LEAF_BW_NODE) {}
  uint64_t Get_size();
  bool Insert(KeyType key, ValueType value);
  bool Split_node(uint64_t id, uint64_t *path, uint64_t index, KeyType key, ValueType value);
};

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class DeltaNode : Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker> {
  KeyType key;
  ValueType val;
  public:
  DeltaNode(const BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>& bwt, uint64_t id) :
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(bwt, id, INSERT) {} // Default is INSERT type
};

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class SplitIndexDeltaNode : Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker> {
  public:
  SplitIndexDeltaNode(const BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>& bwt, uint64_t id) :
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(bwt, id, SPLIT_INDEX) {}
  KeyType split_key, boundary_key;
  uint64_t new_split_node_id;
};

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class RemoveIndexDeltaNode : Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker> { 
  public:
  uint64_t id;
  RemoveIndexDeltaNode(const BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>& bwt, uint64_t id) :
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(bwt, id, REMOVE_INDEX) {}
  KeyType deleted_key;
};

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class SplitDeltaNode : Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker> {
  public:
  SplitDeltaNode(const BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>& bwt, uint64_t id) :
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(bwt, id, SPLIT) {}
  KeyType split_key;
  uint64_t target_node_id; // pointer to target Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker> for preventing blocking
};

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class RemoveDeltaNode : Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker> {
  public:
  KeyType deleted_key; 
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *node_to_be_removed; // can be delta Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker> or internal_bw_node or leaf_bw_node
  RemoveDeltaNode(const BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>& bwt, uint64_t id) :
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(bwt, id, REMOVE) {}
};

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class MergeDeltaNode : Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker> {
  public:
  MergeDeltaNode(const BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>& bwt, uint64_t id) :
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(bwt, id, MERGE) {}
  KeyType MergeKey;
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *node_to_be_merged;
};
}  // End index namespace
}  // End peloton namespace
