//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// bwtree.cpp
//
// Identification: src/backend/index/bwtree.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/index/bwtree.h"

namespace peloton {
namespace index {

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
bool LeafBWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecked>::insert(const storage::Tuple *key, 
    const ItemPointer location){
  class SimpleDeltaNode delta(my_tree, id);
  delta.type = INSERT;
  delta.key = key;
  delta.value = location;
  delta.next = (void *)this;
  std::pair<void *, uint32_t> node_ = my_tree->table->Get(id);
  uint32_t chain_len = node_.second;
  return my_tree->table->Install(id, &delta, chain_len+1);
}
  

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
bool LeafBWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecked>::split_node(uint64_t id, uint64_t parent_id, 
    const storage::Tuple *key, const ItemPointer location){
  uint64_t new_node_id = my_tree->table->get_next_id();
  class LeafBWNode new_leaf_node(my_tree, new_node_id);
  new_leaf_node.type = LEAF_BW_NODE;
  new_leaf_node.sibling_id = sibling_id;
  std::pair<KeyType, ValueType> new_entry(key, location);
  new_leaf_node.kv_list.push_back(new_entry);

  class SplitDeltaNode split_node(my_tree, id);
  split_node.next = (void *)this;
  split_node.target_node_id = new_node_id;
  split_node.split_key = key;

  std::pair<void *, uint32_t> node_ = my_tree->table->Get(id);
  uint32_t chain_len = node_.second;
  bool ret_val = false;
  ret_val |= my_tree->table->Install(new_node_id, &new_leaf_node, 0);
  ret_val |= my_tree0>table->Install(id, &split_node, chain_len+1);

  return ret_val;
}


template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
uint64_t LeafBWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecked>::get_size(){
  std::pair<void *, uint32_t> node_ = my_tree->table->Get(id);
  void* node_pointer = node_.first;
  uint64_t count = 0;
  bool end = false;
  while(node_pointer != NULL)
  {
    node_type_t type = node_pointer->type;
    switch(type){
      case(INSERT):
        count++;
        break;
      case(DELETE):
        count--;
        break;
      case(UPDATE):
        break;
      case(LEAF_BW_NODE):
        count += kv_list.size();
        end = true;
        break;
      case(SPLIT):
        break;
      case(MERGE):
        count += node->node_to_be_merged->kv_list.size();
        break;
      case(NODE_DELETE):
        return 0;
        break;
      default:
        break;
    }
    if(!end)
      node_pointer = node_pointer->next;
    else
      break;
  }
  return count;
}
  
// Add your function definitions here

}  // End index namespace
}  // End peloton namespace
