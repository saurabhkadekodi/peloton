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
#include "assert.h"
namespace peloton {
namespace index {
using namespace std;

bool
CASMappingTable::Install(uint64_t id, Node* node_ptr, uint32_t chain_length) {
  return false;
}

pair<Node*, uint32_t> CASMappingTable::Get(uint64_t id) {
  pair<Node*, uint32_t> dummy;
  return dummy;
}

uint64_t CASMappingTable::Get_next_id() {
  return 0;
}

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
bool LeafBWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecked>::insert(KeyType key, ValueType value){
  DeltaNode* delta = new DeltaNode(my_tree, id);
  delta->key = key;
  delta->value = value;
  pair<Node*, uint32_t> node_ = my_tree.table->Get(id);
  delta->next = node_.first;
  uint32_t chain_len = node_.second;
  return my_tree.table->Install(id, delta, chain_len+1);
}

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
bool LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Split_node(uint64_t id, uint64_t *path, uint64_t index, KeyType key, ValueType value){
  my_tree.Consolidate(id, true);
  uint64_t new_node_id = my_tree.table->Get_next_id();
  LeafBWNode* new_leaf_node = new LeafBWNode(my_tree, new_node_id);
  new_leaf_node->sibling_id = sibling_id;
  sibling_id = new_node_id;

  uint64_t count = kv_list.size();
  KeyValue split_key = kv_list[count/2].first;
  KeyValue boundary_key = kv_list[count-1].first;
  vector<pair<KeyType, ValueType>> split_interator = kv_list.begin();
  advance(split_iterator, count/2);

  for(uint64_t i=count/2;i<count;i++)
  {
    pair<KeyType, ValueType> new_entry = kv_list[i];
    new_leaf_node->kv_list.push_back(new_entry);
  }

  // TODO: figure out relationship between bwtree and bwtreeidx then can use this comparator
  // if(my_tree.comparator(key, split_key)){
  //   kv_list.insert(upper_bound(kv_list.begin(), split_iterator, key, my_tree.comparator), key);
  // }
  // else{
  //   new_leaf_node->kv_list.insert(upper_bound(new_leaf_node->kv_list.begin(), new_leaf_node->kv_list.end(), key, my_tree.comparator), key);
  // }

  pair<Node*, uint32_t> node_ = my_tree.table->Get(id);
  SplitDeltaNode *split_node = new SplitDeltaNode(my_tree, id);
  split_node->next = node_.first;
  split_node->target_node_id = new_node_id;
  split_node->split_key = key;

  uint32_t chain_len = node_.second;
  bool ret_val = true;
  ret_val = my_tree.table->Install(new_node_id, new_leaf_node, 0);
  if(!ret_val)
    return false;
  ret_val = my_tree0>table->Install(id, split_node, chain_len+1);
  if(!ret_val)
    return false;

  uint64_t parent_id = path[index-1];
  pair<Node*, uint32_t> parent_node_ = my_tree.table->Get(parent_id);
  Node* node_pointer = parent_node_.first;
  while(1) // SUGGESTION: remove infinite loop condition
  {
    if(type == node_pointer->type)
      break;
    else
      node_pointer = node_pointer->next;
  }
  uint64_t parent_size = node_pointer->Get_size();
  if(parent_size < max_node_size || index == 1)
    ret_val = node_pointer->Insert(parent_id, split_key, boundary_key, new_node_id);
  else
    ret_val = node_pointer->Split(parent_id, path, index - 1, split_key, boundary_key, new_node_id);
  return ret_val;
}


template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
uint64_t LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Get_size(){
  pair<Node*, uint32_t> node_ = my_tree.table->Get(id);
  Node* node_pointer = node_.first;
  uint64_t count = 0;
  bool end = false;
  while(node_pointer != nullptr)
  {
    switch(node_pointer->type){
      case(INSERT):
        count++;
        break;
      case(DELETE):
        count--;
        break;
      // case(UPDATE):
      //   break;
      case(LEAF_BW_NODE):
        count += kv_list.size();
        end = true;
        break;
      case(SPLIT):
        break;
      case(MERGE):
        MergeDeltaNode* mdn = dynamic_cast<MergeDeltaNode*>(node_pointer);
        LeafBWNode* to_be_merged = dynamic_cast<LeafBWNode*>(mdn->node_to_be_merged);
        count += to_be_merged->kv_list.size();
        break;
      case(REMOVE):
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
  
template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
bool InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::insert(uint64_t id, KeyType split_key, KeyType boundary_key, uint64_t new_node_id){
  pair<Node*, uint32_t> node_ = my_tree.table->Get(id);
  Node* node_pointer = node_.first;
  SplitIndexDeltaNode *split_index = new SplitIndexDeltaNode(my_tree, id);
  split_index->split_key = split_key;
  split_index->boundary_key = boundary_key;
  split_index->next = node_pointer;
  split_index->next_split_node_id = new_node_id;
  uint32_t chain_len = node_.second;
  return my_tree.table->Install(id, split_inde, chain_len+1);
  
}

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
bool InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Split(uint64_t id, uint64_t *path, uint64_t index, 
    KeyType split_key, KeyType boundary_key, uint64_t new_node_id){

  bool ret_val = true;
  ret_val = my_tree.Consolidate(id, true);
  if(!ret_val)
    return false;
  pair<Node*, uint32_t> node_ = my_tree.table->Get(id);
  uint64_t count = key_list.size();

  uint64_t new_node_id = my_tree.table->Get_next_id(); 
  InternalBWNode new_internal_node = new InternalBWNode(my_tree, new_node_id);
  new_internal_node->sibling_id = sibling_id;
  sibling_id = new_node_id;

  KeyValue split_key = key_list[count/2].first;
  KeyValue boundary_key = key_list[count-1].first;
  vector<pair<KeyType, uint64_t>> split_interator = key_list.begin();
  advance(split_iterator, count/2);

  for(uint64_t i=count/2;i<count;i++)
  {
    pair<KeyType, ValueType> new_entry = key_list[i];
    new_leaf_node->key_list.push_back(new_entry);
  }

  // TODO: figure out relationship between bwtree and bwtreeidx then can use this comparator
  // if(my_tree.comparator(key, split_key)){
  //   key_list.insert(upper_bound(key_list.begin(), split_iterator, key, my_tree.comparator), key);
  // }
  // else{
  //   new_leaf_node->key_list.insert(upper_bound(new_leaf_node->key_list.begin(), new_leaf_node->key_list.end(), key, my_tree.comparator), key);
  // }

  SplitDeltaNode* split_node = new SplitDeltaNode(my_tree, id);
  split_node->next = node_.first;
  split_node->target_node_id = new_node_id;
  split_node->split_key = key;

  uint32_t chain_len = node_.second;
  ret_val = my_tree.table->Install(new_node_id, new_leaf_node, 0);
  if(!ret_val)
    return false;
  ret_val = my_tree0>table->Install(id, split_node, chain_len+1);
  if(!ret_val)
    return false;

  if(index != 0)
  {
    uint64_t parent_id = path[index - 1];
    pair<Node*, uint32_t> parent_node_ = my_tree.table->Get(parent_id);
    Node* node_pointer = parent_node_.first;
    while(1) // SUGGESTION: remove infinite loop condition
    {
      if(type == node_pointer->type)
        break;
      else
        node_pointer = node_pointer->next;
    }
    uint64_t parent_size = node_pointer->Get_size();
    if(parent_size < max_node_size || index == 1)
      ret_val = node_pointer->Insert(parent_id, split_key, boundary_key, new_node_id);
    else
      ret_val = node_pointer->Split(parent_id, path, index - 1, split_key, boundary_key, new_node_id);
  }
  else
  {
    assert(false);
    //This case should never happen 
  }

  return ret_val;
 
}
// Add your function definitions here

}  // End index namespace
}  // End peloton namespace
