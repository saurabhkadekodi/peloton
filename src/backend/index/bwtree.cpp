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
  pair<Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*, uint32_t> node_ = this.my_tree.table.Get(this.id);
  delta->next = node_.first;
  uint32_t chain_len = node_.second;
  return this.my_tree.table.Install(this.id, delta, chain_len+1);
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
bool LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Split_node(uint64_t *path, uint64_t index, KeyType key, ValueType value){
  bool result = this.my_tree.Consolidate(this.id, true);
  if (!result)
  {
    return result;
  }
  uint64_t new_node_id = this.my_tree.table.Get_next_id();
  LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* new_leaf_node = new LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(this.my_tree, new_node_id);
  new_leaf_node->sibling_id = sibling_id;
  sibling_id = new_node_id;

  uint64_t count = kv_list.size();
  KeyType split_key = kv_list[count/2].first;
  KeyType boundary_key = kv_list[count-1].first;
  typename multimap<KeyType, ValueType, KeyComparator>::iterator split_iterator = kv_list.begin();
  advance(split_iterator, count/2);

  for(;split_iterator != kv_list.end();split_iterator++)
  {
    pair<KeyType, ValueType> new_entry = *split_iterator;
    new_leaf_node->kv_list.insert(new_entry);
  }

  if(this.my_tree.comparator(key, split_key)){
    kv_list.insert(make_pair(key, value));
  }
  else{
    new_leaf_node->kv_list.insert(make_pair(key, value));
  }

  pair<Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*, uint32_t> node_ = this.my_tree.table.Get(this.id);
  SplitDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* split_node = new SplitDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(this.my_tree, this.id);
  split_node->next = node_.first;
  split_node->target_node_id = new_node_id;
  split_node->split_key = key;

  uint32_t chain_len = node_.second;
  bool ret_val = true;
  ret_val = this.my_tree.table.Install(new_node_id, new_leaf_node, 0);
  if(!ret_val)
    return false;
  ret_val = this.my_tree.table.Install(this.id, split_node, chain_len+1);
  if(!ret_val)
    return false;

  if(index > 0){
	  uint64_t parent_id = path[index-1];
	  pair<Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*, uint32_t> parent_node_ = this.my_tree.table.Get(parent_id);
	  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer = parent_node_.first;
	  
	  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *cur_pointer = node_pointer;
	  while(cur_pointer->next)
		cur_pointer = cur_pointer->next;

	  InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* internal_pointer = dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(cur_pointer);

	  uint64_t parent_size = internal_pointer->Get_size();
	  if(parent_size + 1 < this.my_tree.max_node_size)
		ret_val = node_pointer->Insert(split_key, boundary_key, new_node_id);
	  else
		ret_val = node_pointer->Split(path, index - 1, split_key, boundary_key, new_node_id);
	  return ret_val;
  }
  else{
	//TODO: SplitRoot
  }
}


template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
bool LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Merge_node(uint64_t *path, uint64_t index, KeyType key, ValueType value){
  bool ret_val = this.my_tree.Consolidate(this.id, true);
  if (!ret_val)
  {
    return ret_val;
  }

  pair<typename multimap<KeyType, ValueType>::iterator, typename multimap<KeyType, ValueType>::iterator> values= kv_list.equal_range(key);
  typename multimap<KeyType, ValueType, KeyComparator>::iterator iter;
  for(iter=values.first;iter!=values.second;iter++){
    if(iter->second == value){
      break;
    }
  }
  kv_list.erase(iter);

  //We are the root node
  if(index == 0)
    return true;

  uint64_t neighbour_node_id;
  if(this.sibling_id != 0){
    neighbour_node_id = this.sibling_id;
	this.my_tree.Consolidated(neighbour_node_id, true);

	pair<Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*, uint32_t> n_node_ = this.my_tree.table.Get(neighbour_node_id);
	Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* n_node_pointer = n_node_.first;

	uint64_t total_count = this.my_tree.Get_size(this.id) + this.my_tree.Get_size(neighbour_node_id);
    if(total_count > this.my_tree.max_node_size){
      //Redistribute
      uint64_t half_count = total_count/2;
      while(this.my_tree.Get_size(this.id) < half_count){
        typename multimap<KeyType, ValueType, KeyComparator>::iterator first_iterator = n_node_pointer->kv_list.begin();
        KeyType key = first_iterator->first;
        ValueType value = first_iterator->second;
        Insert(key, value);
        n_node_pointer->kv_list.erase(first_iterator);
      }
      //TODO: Find out the old split key, and how to update the parent 
      KeyType new_split_key = n_node_pointer->key_list.begin()->first;

    }
	else{
		RemoveDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* remove_node = new RemoveDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(this.my_tree, neighbour_node_id);
		remove_node->next = n_node_pointer;

		uint32_t chain_len = n_node_.second;
		ret_val = this.my_tree.table.Install(neighbour_node_id, remove_node, chain_len+1);
		if(!ret_val)
			return false;

		pair<Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*, uint32_t> node_ = this.my_tree.table.Get(this.id);
		MergeDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* merge_node = new MergeDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(this.my_tree, this.id);
		merge_node->node_to_be_merged = n_node_pointer;
		merge_node->next = node_.first;
		uint32_t my_chain_len = node_.second;
		merge_node->merge_key = n_node_pointer->kv_list.begin()->first;
		ret_val = this.my_tree.table.Install(this.id, merge_node, my_chain_len+1);
		if(!ret_val)
			return false;

		uint64_t parent_id = path[index-1];
		pair<Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*, uint32_t> parent_node_ = this.my_tree.table.Get(parent_id);
		Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* parent_node_pointer = parent_node_.first;

		Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *cur_pointer = parent_node_pointer;
		while(cur_pointer->next)
			cur_pointer = cur_pointer->next;

		InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* parent_pointer = dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(cur_pointer);

		uint64_t parent_size = parent_pointer->Get_size();
		if(parent_size - 1 > this.my_tree.min_node_size)
			ret_val = parent_pointer->Delete(merge_node->merge_key);
		else
			ret_val = parent_pointer->Merge(path, index - 1, merge_node->merge_key);
		return ret_val;

	}
  }
  else{
    //TODO get left sibling id from parent
	uint64_t left_sibling = 0;
    neighbour_node_id = left_sibling;
	this.my_tree.Consolidated(neighbour_node_id, true);

	pair<Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*, uint32_t> n_node_ = this.my_tree.table.Get(neighbour_node_id);
	Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* n_node_pointer = n_node_.first;

	uint64_t total_count = this.my_tree.Get_size(this.id) + this.my_tree.Get_size(neighbour_node_id);
    if(total_count > this.my_tree.max_node_size){
      //Redistribute
      uint64_t half_count = total_count/2;
      while(this.my_tree.Get_size(this.id) < half_count){
        typename multimap<KeyType, ValueType, KeyComparator>::reverse_iterator first_iterator = n_node_pointer->kv_list.rbegin();
        KeyType key = first_iterator->first;
        ValueType value = first_iterator->second;
        Insert(key, value);
        n_node_pointer->kv_list.erase(first_iterator);
      }
      //TODO: Find out the old split key, and how to update the parent 
      KeyType new_split_key = this.key_list.begin()->first;

    }
	else{
		pair<Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*, uint32_t> node_ = this.my_tree.table.Get(this.id);
		RemoveDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* remove_node = new RemoveDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(this.my_tree, this.id);
		remove_node->next = node_.first;

		uint32_t chain_len = node_.second;
		ret_val = this.my_tree.table.Install(this.id, remove_node, chain_len+1);
		if(!ret_val)
			return false;

		Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer = node_.first;
		MergeDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* merge_node = new MergeDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(this.my_tree, neighbour_node_id);
		merge_node->node_to_be_merged = node_pointer;
		merge_node->next = n_node_pointer;
		uint32_t neighbour_chain_len = n_node_.second;
		merge_node->merge_key = node_pointer->kv_list.begin()->first;
		ret_val = this.my_tree.table.Install(neighbour_node_id, merge_node, neighbour_chain_len+1);
		if(!ret_val)
			return false;

		uint64_t parent_id = path[index-1];
		pair<Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*, uint32_t> parent_node_ = this.my_tree.table.Get(parent_id);
		Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* parent_node_pointer = parent_node_.first;

		Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *cur_pointer = parent_node_pointer;
		while(cur_pointer->next)
			cur_pointer = cur_pointer->next;

		InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* parent_pointer = dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(cur_pointer);

		uint64_t parent_size = parent_pointer->Get_size();
		if(parent_size - 1 > this.my_tree.min_node_size)
			ret_val = parent_pointer->Delete(merge_node->merge_key);
		else
			ret_val = parent_pointer->Merge(path, index - 1, merge_node->merge_key);
		return ret_val;
	}
  }


  
  

/*
  ret_val = this.my_tree.table.Install(this.id, split_node, chain_len+1);
  if(!ret_val)
    return false;

  if(index > 0){
	  uint64_t parent_id = path[index-1];
	  pair<Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*, uint32_t> parent_node_ = this.my_tree.table.Get(parent_id);
	  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer = parent_node_.first;
	  
	  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *cur_pointer = node_pointer;
	  while(cur_pointer->next)
		cur_pointer = cur_pointer->next;

	  InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* internal_pointer = dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(cur_pointer);

	  uint64_t parent_size = internal_pointer->Get_size();
	  if(parent_size + 1 < this.my_tree.max_node_size)
		ret_val = node_pointer->Insert(split_key, boundary_key, new_node_id);
	  else
		ret_val = node_pointer->Split(path, index - 1, split_key, boundary_key, new_node_id);
	  return ret_val;
  }
  else{
	//SplitRoot
  }
  */
}

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
bool DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Consolidate(){

  uint64_t new_id = this.my_tree.table -> Get_next_id();
  LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *new_leaf_node = new LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(this.my_tree, new_id);
  multiset<KeyType, KeyComparator> insert_set;
  multiset<KeyType, KeyComparator> delete_set;
  DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *temp = this;
  LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *leaf_node = nullptr;
  bool stop = false;
  while (!stop) {
    if (temp -> type == DELETE)
    {
      delete_set.insert(temp -> key);
    } else if (temp -> type == INSERT) {
      insert_set.insert(temp -> key);
    }
    if (temp -> next -> type == LEAF_BW_NODE) {
      leaf_node = dynamic_cast<LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(temp -> next);
      stop = true;
    } else {
      temp = dynamic_cast<DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *>(temp -> next);
    }
  }
  assert(leaf_node!= nullptr);
  while (!insert_set.empty()) {
    typename multiset<KeyType, KeyComparator>::iterator it = insert_set.begin();
    KeyType key = *it;
    typename multiset<KeyType, KeyComparator>::iterator dit = delete_set.find(key);
    if (dit == delete_set.end())
    {
      leaf_node.kv_list.insert(key);
    } else {
      delete_set.erase(dit);
    }
    insert_set.erase(it);
  }
  while(!delete_set.empty()) {
    typename multiset<KeyType, KeyComparator>::iterator dit = delete_set.begin();
    KeyType key = *dit;
    typename multiset<KeyType, KeyComparator>::iterator it = leaf_node.kv_list.find(key);
    if (it != this.kv_list.end())
    {
      this.kv_list.erase(it);
    } 
    delete_set.erase(dit);
  }
  while(!leaf_node.kv_list.empty()) {
    typename multiset<KeyType, KeyComparator>::iterator it = leaf_node.kv_list.begin();
    new_leaf_node.kv_list.insert(*it);
    leaf_node.kv_list.erase(it);
  }
  return this.my_tree.table->Install(new_id, new_leaf_node, 1);   // TODO: what is the correct chain length?


}

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
uint64_t BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Get_size(uint64_t id){
  pair<Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*, uint32_t> node_ = table.Get(id);
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer = node_.first;
  uint64_t count = 0;
  bool end = false;

  // switch(node_pointer->type){
  //   case(LEAF_BW_NODE):
  //     return leaf_pointer->id;
  //     break;
  //   case(INTERNAL_BW_NODE):
  //     InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* internal_pointer = dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(node_pointer);
  //     prev_id = cur_id;
  //     cur_id = internal_pointer->Get_child_id(key);  
  //     try_consolidation = true;
  //     break;
  while(node_pointer != nullptr)
  {
    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* simple_pointer = nullptr;
    switch(node_pointer->type){
      case(INSERT):
        count++;
        break;
      case(DELETE):
        //TODO: Assume for now that there are no multiple instances
        count--;
        break;
      // case(UPDATE):
      //   break;
      case(LEAF_BW_NODE):
        LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* leaf_pointer = dynamic_cast<LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(node_pointer);
        count += leaf_pointer->kv_list.size();
        end = true;
        break;
      case(SPLIT):
        break;
      case(MERGE):
        MergeDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* mdn = dynamic_cast<MergeDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(node_pointer);
        LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* to_be_merged = dynamic_cast<LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(mdn->node_to_be_merged);
        count += to_be_merged->kv_list.size();
        break;
      case(REMOVE):
        return 0;
        break;
      case(SPLIT_INDEX):
        count++;
        break;
      case(REMOVE_INDEX):
        count--;
        break;
      case(INTERNAL_BW_NODE):
        InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* internal_pointer = dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(node_pointer);
        count += internal_pointer->key_list.size();
        end = true;
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
bool InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Insert(KeyType split_key, KeyType boundary_key, uint64_t new_node_id){
  pair<Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*, uint32_t> node_ = this.my_tree.table.Get(this.id);
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer = node_.first;
  SplitIndexDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *split_index = new SplitIndexDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(this.my_tree, this.id);
  split_index->split_key = split_key;
  split_index->boundary_key = boundary_key;
  split_index->next = node_pointer;
  split_index->next_split_node_id = new_node_id;
  uint32_t chain_len = node_.second;
  return this.my_tree.table.Install(this.id, split_index, chain_len+1);
  
}

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
bool InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Split(uint64_t *path, uint64_t index, 
    KeyType split_key, KeyType boundary_key, uint64_t new_node_id){

  bool ret_val = true;
  ret_val = this.my_tree.Consolidate(this.id, true);
  if(!ret_val)
    return false;
  pair<Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*, uint32_t> node_ = this.my_tree.table.Get(this.id);

  uint64_t new_internal_node_id = this.my_tree.table.Get_next_id(); 
  InternalBWNode new_internal_node = new InternalBWNode(this.my_tree, new_internal_node_id);
  new_internal_node->sibling_id = sibling_id;
  sibling_id = new_internal_node_id;



  uint64_t count = key_list.size();
  // KeyType split_key = key_list[count/2].first;
  // KeyType boundary_key = key_list[count-1].first;
  typename multimap<KeyType, uint64_t, KeyComparator>::iterator split_iterator = key_list.begin();
  advance(split_iterator, count/2);

  for(;split_iterator != key_list.end();split_iterator++)
  {
    pair<KeyType, ValueType> new_entry = *split_iterator;
    new_internal_node->key_list.insert(new_entry);
  }

  // if(this.my_tree.comparator(key, split_key)){
  //   key_list.insert(make_pair(key, new_node_id));
  // }
  // else{
  //   new_internal_node->key_list.insert(make_pair(key, new_node_id));
  // }

   SplitDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* split_node = new SplitDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(this.my_tree, this.id);
   split_node->next = node_.first;
   split_node->target_node_id = new_internal_node_id;
   split_node->split_key = split_key; 

  uint32_t chain_len = node_.second;
  ret_val = this.my_tree.table->Install(new_internal_node_id, new_internal_node, 0);
  if(!ret_val)
    return false;
  ret_val = this.my_tree.table->Install(this.id, split_node, chain_len+1);
  if(!ret_val)
    return false;

  if(index != 0)
  {
    uint64_t parent_id = path[index - 1];
    pair<Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*, uint32_t> parent_node_ = this.my_tree.table.Get(parent_id);
    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer = parent_node_.first;

	Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *cur_pointer = node_pointer;
	while(cur_pointer->next)
		cur_pointer = cur_pointer->next;

	InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* internal_pointer = dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(cur_pointer);

	uint64_t parent_size = internal_pointer->Get_size();
	if(parent_size + 1< this.my_tree.max_node_size)
		ret_val = node_pointer->Insert(split_key, boundary_key, new_node_id);
	else
		ret_val = node_pointer->Split(path, index - 1, split_key, boundary_key, new_node_id);
	return ret_val;
  }
  else
  {
    //TODO: SplitRoot
  }

  return ret_val;
 
}
template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
uint64_t InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Get_child_id(KeyType key) {
	//TODO: We need to find the correct child id
	typename multimap<KeyType, uint64_t, KeyComparator>::iterator iter = key_list.find(key);
	if(iter == key_list.end()){
		return rightmost_pointer;
	}
	return iter->second;
} 

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
bool InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Delete(KeyType merged_key){

	pair<Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*, uint32_t> node_ = this.my_tree.table.Get(this.id);
	Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer = node_.first;
	RemoveIndexDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *remove_index = new RemoveIndexDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(this.my_tree, this.id);
	remove_index->deleted_key = merged_key;
	remove_index->next = node_pointer;
	uint32_t chain_len = node_.second;
	return this.my_tree.table.Install(this.id, remove_index, chain_len+1);
}


// Add your function definitions here

}  // End index namespace
}  // End peloton namespace
