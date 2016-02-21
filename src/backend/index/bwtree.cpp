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
using namespace std; //SUGGESTION: DON'T USE A GLOBAL USING NAMESPACE

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
Epoch<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Epoch(const BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>& bwt, uint64_t id, uint64_t oldest) {
  generation = id;
  oldest_epoch = oldest;
  ref_count.store(0);
  my_tree = bwt;
}

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
void Epoch<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::join() {
  /*
   * We call join from every operation that needs to be performed on the tree.
   */
  ref_count.fetch_add(1);
}

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
void Epoch<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::performGc() {
  /*
   * We have to iterate through the to_be_cleaned list one-by-one and delete
   * the nodes. The destructors of those nodes will take care of cleaning up
   * the necessary internal malloc'd objects.
   */
  to_be_cleaned.clear();
}

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
bool Epoch<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::leave() {
  ref_count.fetch_sub(1);
  if(to_be_cleaned.size() >= my_tree->max_epoch_size) {
    auto new_e = Epoch(this->generation + 1, my_tree->oldest_epoch);
    __sync_bool_compare_and_swap(&(my_tree->current_epoch), this, new_e);
  }
  if(__sync_bool_compare_and_swap(&(my_tree->current_epoch), this, this) == false) {
    /*
     * this means epoch changed before this thread reached leave.
     */
    if(ref_count == 0) {
      if(__sync_bool_compare_and_swap(&(my_tree->oldest_epoch), oldest_epoch, this->generation + 1)) {
        /*
         * perform cleaning of this epoch, because we don't need it anymore.
         *
         * Here it is guaranteed that only one thread will come here, since it
         * is guarded by compare-and-swap on oldest epoch.
         */
        performGc();
        return true; // this is for the upper layer to delete the epoch object
      }
    }
  }
}

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
bool
CASMappingTable<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Install(uint64_t id, Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_ptr, uint32_t chain_length) {
  return false;
}
template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
pair<Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*, uint32_t> CASMappingTable<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Get(uint64_t id) {
  pair<Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*, uint32_t> dummy;
  return dummy;
}
template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
uint64_t CASMappingTable<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Get_next_id() {
  return 0;
}

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
bool LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Insert(KeyType key, ValueType value){
  DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* delta = 
  new DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(this.my_tree, this.id);
  delta->key = key;
  delta->value = value;
  pair<Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*, uint32_t> node_ = this.my_tree.table->Get(this.id);
  delta->next = node_.first;
  uint32_t chain_len = node_.second;
  return this.my_tree.table->Install(this.id, delta, chain_len+1);
}

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
bool LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Delete(KeyType key, ValueType value){
  DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* delta = 
  new DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(this.my_tree, this.id);
  delta->key = key;
  delta->value = value;
  delta-> type = DELETE;
  pair<Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*, uint32_t> node_ = this.my_tree.table->Get(this.id);
  delta->next = node_.first;
  uint32_t chain_len = node_.second;
  return this.my_tree.table->Install(this.id, delta, chain_len+1);
}


template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
bool LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Split_node(uint64_t id, uint64_t *path, uint64_t index, KeyType key, ValueType value){
  bool result = this.my_tree.Consolidate(id, true);
  if (!result)
  {
    return result;
  }
  uint64_t new_node_id = this.my_tree.table->Get_next_id();
  LeafBWNode* new_leaf_node = new LeafBWNode(this.my_tree, new_node_id);
  new_leaf_node->sibling_id = sibling_id;
  sibling_id = new_node_id;

  uint64_t count = kv_list.size();
  KeyType split_key = kv_list[count/2].first;
  KeyType boundary_key = kv_list[count-1].first;
  vector<pair<KeyType, ValueType>> split_iterator = kv_list.begin();
  advance(split_iterator, count/2);

  for(uint64_t i=count/2;i<count;i++)
  {
    pair<KeyType, ValueType> new_entry = kv_list[i];
    new_leaf_node->kv_list.push_back(new_entry);
  }

  // TODO: figure out relationship between bwtree and bwtreeidx then can use this comparator
  // if(this.my_tree.comparator(key, split_key)){
  //   kv_list.insert(upper_bound(kv_list.begin(), split_iterator, key, this.my_tree.comparator), key);
  // }
  // else{
  //   new_leaf_node->kv_list.insert(upper_bound(new_leaf_node->kv_list.begin(), new_leaf_node->kv_list.end(), key, this.my_tree.comparator), key);
  // }

  pair<Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*, uint32_t> node_ = this.my_tree.table->Get(id);
  SplitDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* split_node = new SplitDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(this.my_tree, id);
  split_node->next = node_.first;
  split_node->target_node_id = new_node_id;
  split_node->split_key = key;

  uint32_t chain_len = node_.second;
  bool ret_val = true;
  ret_val = this.my_tree.table->Install(new_node_id, new_leaf_node, 0);
  if(!ret_val)
    return false;
  ret_val = this.my_tree.table->Install(id, split_node, chain_len+1);
  if(!ret_val)
    return false;

  uint64_t parent_id = path[index-1];
  pair<Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*, uint32_t> parent_node_ = this.my_tree.table->Get(parent_id);
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer = parent_node_.first;
  while(1) // SUGGESTION: remove infinite loop condition
  {
    if(INTERNAL_BW_NODE == node_pointer->type)
      break;
    else
      node_pointer = node_pointer->next;
  }
  uint64_t parent_size = node_pointer->Get_size();
  if(parent_size < this.my_tree.max_node_size || index == 1)
    ret_val = node_pointer->Insert(parent_id, split_key, boundary_key, new_node_id);
  else
    ret_val = node_pointer->Split(parent_id, path, index - 1, split_key, boundary_key, new_node_id);
  return ret_val;
}

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
bool DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Consolidate(){

  // uint64_t new_id = this.my_tree.table -> Get_next_id();
  // LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *new_leaf_node = new LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(this.my_tree, new_id);
  // multiset<KeyType, KeyComparator> insert_set;
  // multiset<KeyType, KeyComparator> delete_set;
  // DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *temp = this;
  // LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *leaf_node = nullptr;
  // bool stop = false;
  // while (!stop) {
  //   if (temp -> type == DELETE)
  //   {
  //     delete_set.insert(temp -> key);
  //   } else if (temp -> type == INSERT) {
  //     insert_set.insert(temp -> key);
  //   }
  //   if (temp -> next -> type == LEAF_BW_NODE) {
  //     leaf_node = dynamic_cast<LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(temp -> next);
  //     stop = true;
  //   } else {
  //     temp = dynamic_cast<DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *>(temp -> next);
  //   }
  // }
  // assert(leaf_node!= nullptr);
  // while (!insert_set.empty()) {
  //   multiset<KeyType, KeyComparator>::iterator it = insert_set.begin();
  //   KeyType key = *it;
  //   multiset<KeyType, KeyComparator>::iterator dit = delete_set.find(key);
  //   if (dit == delete_set.end())
  //   {
  //     leaf_node.kv_list.insert(key);
  //   } else {
  //     delete_set.erase(dit);
  //   }
  //   insert_set.erase(it);
  // }
  // while(!delete_set.empty()) {
  //   multiset<KeyType, KeyComparator>::iterator dit = delete_set.begin();
  //   KeyType key = *it;
  //   multiset<KeyType, KeyComparator>::iterator it = leaf_node.kv_list.find(key);
  //   if (it != kv_list.end())
  //   {
  //     kv_list.erase(it);
  //   } 
  //   delete_set.erase(dit);
  // }
  // while(!leaf_node.kv_list.empty()) {
  //   multiset<KeyType, KeyComparator>::iterator it = leaf_node.kv_list.begin();
  //   new_leaf_node.kv_list.insert(*it);
  //   leaf_node.kv_list.erase(it);
  // }
  // return this.my_tree.table->Install(new_id, new_leaf_node, 1);   // TODO: what is the correct chain length?
  return false;


}

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
uint64_t LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Get_size(){
  pair<Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*, uint32_t> node_ = this.my_tree.table->Get(this.id);
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer = node_.first;
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
        MergeDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* mdn = dynamic_cast<MergeDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(node_pointer);
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
bool InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Insert(uint64_t id, KeyType split_key, KeyType boundary_key, uint64_t new_node_id){
  pair<Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*, uint32_t> node_ = this.my_tree.table->Get(id);
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer = node_.first;
  SplitIndexDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *split_index = new SplitIndexDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(this.my_tree, id);
  split_index->split_key = split_key;
  split_index->boundary_key = boundary_key;
  split_index->next = node_pointer;
  split_index->next_split_node_id = new_node_id;
  uint32_t chain_len = node_.second;
  return this.my_tree.table->Install(id, split_index, chain_len+1);
  
}

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
bool InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Split(uint64_t id, uint64_t *path, uint64_t index, 
    KeyType split_key, KeyType boundary_key, uint64_t new_node_id){

  bool ret_val = true;
  ret_val = this.my_tree.Consolidate(id, true);
  if(!ret_val)
    return false;
  pair<Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*, uint32_t> node_ = this.my_tree.table->Get(id);
  uint64_t count = key_list.size();

  uint64_t new_internal_node_id = this.my_tree.table->Get_next_id(); 
  InternalBWNode new_internal_node = new InternalBWNode(this.my_tree, new_internal_node_id);
  new_internal_node->sibling_id = sibling_id;
  sibling_id = new_internal_node_id;

  auto internal_split_key = key_list[count/2].first;
  auto internal_boundary_key = key_list[count-1].first;
  vector<pair<KeyType, uint64_t>> split_iterator = key_list.begin();
  advance(split_iterator, count/2);

  for(uint64_t i=count/2;i<count;i++)
  {
    pair<KeyType, ValueType> new_entry = key_list[i];
    new_internal_node->key_list.push_back(new_entry); 
  }

  // TODO: figure out relationship between bwtree and bwtreeidx then can use this comparator
  // if(this.my_tree.comparator(split_key, internal_split_key)){
  //   key_list.insert(upper_bound(key_list.begin(), split_iterator, split_key, this.my_tree.comparator), split_key);
  // }
  // else{
  //   new_leaf_node->key_list.insert(upper_bound(new_leaf_node->key_list.begin(), new_leaf_node->key_list.end(), split_key, this.my_tree.comparator), split_key);
  // }

   SplitDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* split_node = new SplitDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(this.my_tree, id);
   split_node->next = node_.first;
   split_node->target_node_id = new_node_id;
   split_node->split_key = split_key; 

  uint32_t chain_len = node_.second;
  ret_val = this.my_tree.table->Install(new_internal_node_id, new_internal_node, 0);
  if(!ret_val)
    return false;
  ret_val = this.my_tree.table->Install(id, split_node, chain_len+1);
  if(!ret_val)
    return false;

  if(index != 0)
  {
    uint64_t parent_id = path[index - 1];
    pair<Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*, uint32_t> parent_node_ = this.my_tree.table->Get(parent_id);
    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer = parent_node_.first;
    while(1) // SUGGESTION: remove infinite loop condition
    {
      if(INTERNAL_BW_NODE == node_pointer->type)
        break;
      else
        node_pointer = node_pointer->next;
    }
    uint64_t parent_size = node_pointer->Get_size();
    if(parent_size < this.my_tree.max_node_size || index == 1)
      ret_val = node_pointer->Insert(parent_id, internal_split_key, internal_boundary_key, new_internal_node_id);
    else
      ret_val = node_pointer->Split(parent_id, path, index - 1, internal_split_key, internal_boundary_key, new_internal_node_id);
  }
  else
  {
    assert(false);
    //This case should never happen 
  }

  return ret_val;
 
}
template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
uint64_t InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Get_child_id(KeyType key) {
  return 0;  
} 

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
bool InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Delete(uint64_t id, KeyType merged_key){
  return false;
  
}


// Add your function definitions here

}  // End index namespace
}  // End peloton namespace
