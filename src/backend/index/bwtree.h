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
#include <set>

namespace peloton {
namespace index {
using namespace std;

typedef uint64_t epoch_t;

typedef enum node_type {
  INTERNAL_BW_NODE,
  LEAF_BW_NODE,
  INSERT,
  UPDATE,
  DELETE,
  SPLIT,
  MERGE,
  REMOVE,
  SPLIT_INDEX,
  REMOVE_INDEX
} node_type_t;

template <typename KeyType, typename ValueType, class KeyComparator>
class BWTree;

template <typename KeyType, typename ValueType, class KeyComparator>
class Node {

public:
  epoch_t generation;
  uint64_t id;
  const BWTree<KeyType, ValueType, KeyComparator>& my_tree; // reference of the tree I belong to
  Node *next;
  Node(const BWTree<KeyType, ValueType, KeyComparator>& bwt, uint64_t id, node_type_t type) :
  my_tree(bwt), id(id), type(type) {
    next = nullptr;
  }
  ~Node() {
    next -> ~Node();
  }
  virtual bool Consolidate();
public:
  node_type_t type;
};

template <typename KeyType, typename ValueType, class KeyComparator>
class CASMappingTable {
  typedef Node<KeyType, ValueType, KeyComparator> NodeType;

  private:
  map<uint64_t, pair<NodeType*, uint32_t>> cas_mapping_table; // should be capable of mapping to internal and leaf bw nodes and delta nodes of any type
  uint64_t cur_max_id;
  public:
  CASMappingTable() : cur_max_id(1) {};
  bool Install(uint64_t id, Node<KeyType, ValueType, KeyComparator>* node_ptr, uint32_t chain_length); // install into mapping table via compare and swap
  pair<NodeType*, uint32_t> Get(uint64_t id);
  uint64_t Get_next_id();
};

// Look up the stx btree interface for background.
// peloton/third_party/stx/btree.h
/**
 * BW tree-based index implementation.
 *
 * @see Index
 */
template <typename KeyType, typename ValueType, class KeyComparator>
class BWTree {
  // friend class BWTreeIndex<KeyType, ValueType, KeyComparator>;
 public:
  CASMappingTable<KeyType, ValueType, KeyComparator> table;
 // BWTree() {CASMappingTable<KeyType, ValueType, KeyComparator> b;
BWTree() {}
BWTree(KeyComparator kc);
 // BWTree(CASMappingTable<KeyType, ValueType, KeyComparator> table) : table(table){}
  uint32_t min_node_size;
  uint32_t max_node_size;
  uint64_t tree_height;
  uint64_t root; // root points to an id in the mapping table
  bool Consolidate(uint64_t id, bool force) {return false;} // id is that of the mapping table entry
  bool MergeNodes(uint64_t n1, uint64_t n2){return false;} // saurabh
  //NodeType * CreateNode(uint64_t id, node_type_t t){return nullptr;} // for creating when consolidating
  //bool DeleteNode(uint64_t id){return false;}
  // tianyuan - GC and the epoch mechanism
  bool Insert(KeyType key, ValueType value);
  bool Delete(KeyType key, ValueType value);
  uint64_t Search(KeyType key, uint64_t *path, uint64_t &location);
  uint64_t Get_size(uint64_t id);
};

template <typename KeyType, typename ValueType, class KeyComparator>
class InternalBWNode : Node<KeyType, ValueType, KeyComparator> {
  public:
  multimap<KeyType, uint64_t, KeyComparator> key_list; // all keys have children
  uint64_t rightmost_pointer;
  uint64_t sibling_id;
  uint64_t low;
  uint64_t high;
  InternalBWNode(const BWTree<KeyType, ValueType, KeyComparator>& bwt, uint64_t id) :
  Node<KeyType, ValueType, KeyComparator>(bwt, id, INTERNAL_BW_NODE) {}
  bool Insert(KeyType split_key, KeyType boundary_key, uint64_t new_node_id);
  bool Split(uint64_t *path, uint64_t index, KeyType split_key, KeyType boundary_key, uint64_t new_node_id);
  bool Delete(KeyType merged_key); 
  bool Merge(uint64_t *path, uint64_t index, KeyType deleted_key);
  uint64_t Get_child_id(KeyType key);
};

template <typename KeyType, typename ValueType, class KeyComparator>
class LeafBWNode : Node<KeyType, ValueType, KeyComparator> {

  public:
  multimap<KeyType, ValueType, KeyComparator> kv_list; // all key value pairs
  uint64_t sibling_id;  
  uint64_t low;
  uint64_t high;
  LeafBWNode(const BWTree<KeyType, ValueType, KeyComparator>& bwt, uint64_t id) :
  Node<KeyType, ValueType, KeyComparator>(bwt, id, LEAF_BW_NODE) {}
  bool Insert(KeyType key, ValueType value);
  bool Delete(KeyType key, ValueType value);
  bool Split_node(uint64_t *path, uint64_t index, KeyType key, ValueType value);
  bool Merge_node(uint64_t *path, uint64_t index, KeyType key, ValueType value);
};

template <typename KeyType, typename ValueType, class KeyComparator>
class DeltaNode : Node<KeyType, ValueType, KeyComparator> {
  KeyType key;
  ValueType value;
  public:
  bool Consolidate();
  DeltaNode(const BWTree<KeyType, ValueType, KeyComparator>& bwt, uint64_t id, node_type_t type) :
  Node<KeyType, ValueType, KeyComparator>(bwt, id, type) {} // Default is INSERT type
};

template <typename KeyType, typename ValueType, class KeyComparator>
class SplitIndexDeltaNode : Node<KeyType, ValueType, KeyComparator> {
  public:
  SplitIndexDeltaNode(const BWTree<KeyType, ValueType, KeyComparator>& bwt, uint64_t id) :
  Node<KeyType, ValueType, KeyComparator>(bwt, id, SPLIT_INDEX) {}
  KeyType split_key, boundary_key;
  uint64_t new_split_node_id;
};

template <typename KeyType, typename ValueType, class KeyComparator>
class RemoveIndexDeltaNode : Node<KeyType, ValueType, KeyComparator> { 
  public:
  uint64_t id;
  RemoveIndexDeltaNode(const BWTree<KeyType, ValueType, KeyComparator>& bwt, uint64_t id) :
  Node<KeyType, ValueType, KeyComparator>(bwt, id, REMOVE_INDEX) {}
  KeyType deleted_key;
};

template <typename KeyType, typename ValueType, class KeyComparator>
class SplitDeltaNode : Node<KeyType, ValueType, KeyComparator> {
  public:
  SplitDeltaNode(const BWTree<KeyType, ValueType, KeyComparator>& bwt, uint64_t id) :
  Node<KeyType, ValueType, KeyComparator>(bwt, id, SPLIT) {}
  KeyType split_key;
  uint64_t target_node_id; 
};

template <typename KeyType, typename ValueType, class KeyComparator>
class RemoveDeltaNode : Node<KeyType, ValueType, KeyComparator> {
  public:
  //KeyType deleted_key; //The entire node is deleted and not a key, hence we don't need this
  //Node<KeyType, ValueType, KeyComparator> *node_to_be_removed; // can be delta Node<KeyType, ValueType, KeyComparator> or internal_bw_node or leaf_bw_node
  RemoveDeltaNode(const BWTree<KeyType, ValueType, KeyComparator>& bwt, uint64_t id) :
  Node<KeyType, ValueType, KeyComparator>(bwt, id, REMOVE) {}
};

template <typename KeyType, typename ValueType, class KeyComparator>
class MergeDeltaNode : Node<KeyType, ValueType, KeyComparator> {
  public:
  MergeDeltaNode(const BWTree<KeyType, ValueType, KeyComparator>& bwt, uint64_t id) :
  Node<KeyType, ValueType, KeyComparator>(bwt, id, MERGE) {}
  KeyType merge_key;
  Node<KeyType, ValueType, KeyComparator> *node_to_be_merged;
};
}  // End index namespace
}  // End peloton namespace
