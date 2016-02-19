//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// btree_index.cpp
//
// Identification: src/backend/index/btree_index.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/logger.h"
#include "backend/index/bwtree_index.h"
#include "backend/index/index_key.h"
#include "backend/storage/tuple.h"
#include "malloc.h"

namespace peloton {
namespace index {
using namespace std;

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::BWTreeIndex(
    IndexMetadata *metadata)
    : Index(metadata),
      equals(metadata),
      comparator(metadata) {
  // Add your implementation here
}

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::~BWTreeIndex() {
  // Add your implementation here
}

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Insert(
    KeyType key, ValueType value) {

Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer = Search(key);
assert(leaf_pointer -> type == LEAF_BW_NODE);
  LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* leaf_pointer = dynamic_cast<LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(node_pointer);


        uint64_t cur_node_size = leaf_pointer->Get_size();
        if(cur_node_size < max_node_size){
          return leaf_pointer->Insert(key, value);
        }
        else{
          return leaf_pointer->Split_node(cur_id, path, index, prev_id, key, value); 
        }
 
  // Add your implementation here
  return false;
}

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Delete(KeyType key, ValueType value) {
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer = Search(key);
assert(leaf_pointer -> type == LEAF_BW_NODE);
  LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* leaf_pointer = dynamic_cast<LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(node_pointer);


        uint64_t cur_node_size = leaf_pointer->Get_size();
                if(cur_node_size > min_node_size){
          return leaf_pointer->Delete(key, value);
        }
        else{
          // return leaf_pointer->Merge_node(cur_id, path, index, prev_id, key, value); 
        }
}


template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* 
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Search(KeyType key) {
  uint64_t cur_id = root;
  uint64_t prev_id = cur_id;
  bool stop = false;
  bool try_consolidation = true;
  vector<KeyType> deleted_keys, deleted_indexes;
  uint64_t *path = (uint64_t *)malloc(sizeof(uint64_t) * tree_height);
  uint64_t index = 0;
  while(!stop){
    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer = nullptr;
    if(try_consolidation){
      Consolidate(cur_id, false);
      pair<Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*, uint32_t> node_ = table.get(cur_id); 
      node_pointer = node_.first;
      uint32_t chain_length = node_.second;
      deleted_keys.clear();
      deleted_indexes.clear();
      index++;
      path[index] = cur_id;
    }
    // TODO: nodepointer could be null
Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* simple_pointer = nullptr;
    switch(node_pointer->type){
      case(LEAF_BW_NODE):
        LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* leaf_pointer = dynamic_cast<LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(node_pointer);
        return leaf_pointer;
        break;
      case(INTERNAL_BW_NODE):
        InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* internal_pointer = dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(node_pointer);
        prev_id = cur_id;
        cur_id = internal_pointer->Get_child_id(key);  
        try_consolidation = true;
        break;
      case(INSERT):
        simple_pointer = dynamic_cast<DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(node_pointer);
        if(equals(*key, simple_pointer->key) && 
            find(deleted_keys.begin(), deleted_keys.end(), *key) == deleted_keys.end())
          return nullptr;
        node_pointer = simple_pointer->next();
        try_consolidation = false;
        break;
      // case(UPDATE):
      //   DeltaNode *simple_pointer = (DeltaNode *)node_pointer;
      //   if( equals(*key, simple_pointer->key) && 
      //       find(deleted_keys.begin(), deleted_keys.end(), *key) == deleted_keys.end())
      //     return false;
      //   node_pointer = simple_pointer->next();
      //   try_consolidation = false;
      //   break;
      case(DELETE):
       simple_pointer = dynamic_cast<DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(node_pointer);
        if(equals(*key, simple_pointer->key))
          deleted_keys.push_back(*key);
        node_pointer = simple_pointer->next();
        try_consolidation = false;
        break;
      case(SPLIT):
        SplitDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *split_pointer = dynamic_cast<SplitDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(node_pointer);
        // TODO: bwtree may not have access to comparator
        // if(comparator(*key, split_pointer->key)){
        //   node_pointer = split_pointer->next;
        //   try_consolidation = false;
        // }
        // else{
        //   prev_id = cur_id;
        //   cur_id = split_pointer->target_node_id;
        //   try_consolidation = true;
        // }
        break;
      case(MERGE):
        MergeDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *merge_pointer = dynamic_cast<MergeDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(node_pointer);
        // if(comparator(*key, merge_pointer->MergeKey)){
        //   node_pointer = merge_pointer->next;
        // }
        // else{
        //   node_pointer = merge_pointer->node_to_be_merged;
        // }
        // try_consolidation = false;
        break;
      case(REMOVE):
        cur_id = prev_id;
        try_consolidation = true;
        break;
      case(SPLIT_INDEX):
        SplitIndexDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *split_index_pointer = dynamic_cast<SplitIndexDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(node_pointer); 
        if(find(deleted_indexes.begin(), deleted_indexes.end(), split_index_pointer->split_key) == deleted_indexes.end()){
          // if(comparator(*key, split_index_pointer->split_key) || !comparator(*key, split_index_pointer->boundary_key)){
          //   node_pointer = split_index_pointer->next;
          //   try_consolidation = false;
          // }
          // else{
          //   prev_id = cur_id;
          //   cur_id = split_index_pointer->new_split_node_id;
          //   try_consolidation = true;
          // }
        }
        else{
          node_pointer = split_index_pointer->next;
          try_consolidation = false;
        }
        break;
      case(REMOVE_INDEX):
        RemoveIndexDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *delete_index_pointer = dynamic_cast<RemoveIndexDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(node_pointer);
        deleted_indexes.push_back(delete_index_pointer->deleted_key);
        node_pointer = delete_index_pointer->next;
        break;
    }
  }

  // Add your implementation here
  return nullptr;
}

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
bool BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::InsertEntry(
    __attribute__((unused)) const storage::Tuple *key, __attribute__((unused)) const ItemPointer location) {
  // Add your implementation here
  return false;
}

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
bool BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::DeleteEntry(
    __attribute__((unused)) const storage::Tuple *key, __attribute__((unused)) const ItemPointer location) {
  // Add your implementation here
  return false;
}

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
vector<ItemPointer>
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Scan(
    __attribute__((unused)) const vector<Value> &values,
    __attribute__((unused)) const vector<oid_t> &key_column_ids,
    __attribute__((unused)) const vector<ExpressionType> &expr_types,
    __attribute__((unused)) const ScanDirectionType& scan_direction) {
  vector<ItemPointer> result;
  // Add your implementation here
  return result;
}

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
vector<ItemPointer>
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanAllKeys() {
  vector<ItemPointer> result;
  // Add your implementation here
  return result;
}

/**
 * @brief Return all locations related to this key.
 */
template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
vector<ItemPointer>
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanKey(
    __attribute__((unused)) const storage::Tuple *key) {
  vector<ItemPointer> result;
  // Add your implementation here
  return result;
}

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
string
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::GetTypeName() const {
  return "BWTree";
}

// Explicit template instantiation
template class BWTreeIndex<IntsKey<1>, ItemPointer, IntsComparator<1>,
IntsEqualityChecker<1>>;
template class BWTreeIndex<IntsKey<2>, ItemPointer, IntsComparator<2>,
IntsEqualityChecker<2>>;
template class BWTreeIndex<IntsKey<3>, ItemPointer, IntsComparator<3>,
IntsEqualityChecker<3>>;
template class BWTreeIndex<IntsKey<4>, ItemPointer, IntsComparator<4>,
IntsEqualityChecker<4>>;

template class BWTreeIndex<GenericKey<4>, ItemPointer, GenericComparator<4>,
GenericEqualityChecker<4>>;
template class BWTreeIndex<GenericKey<8>, ItemPointer, GenericComparator<8>,
GenericEqualityChecker<8>>;
template class BWTreeIndex<GenericKey<12>, ItemPointer, GenericComparator<12>,
GenericEqualityChecker<12>>;
template class BWTreeIndex<GenericKey<16>, ItemPointer, GenericComparator<16>,
GenericEqualityChecker<16>>;
template class BWTreeIndex<GenericKey<24>, ItemPointer, GenericComparator<24>,
GenericEqualityChecker<24>>;
template class BWTreeIndex<GenericKey<32>, ItemPointer, GenericComparator<32>,
GenericEqualityChecker<32>>;
template class BWTreeIndex<GenericKey<48>, ItemPointer, GenericComparator<48>,
GenericEqualityChecker<48>>;
template class BWTreeIndex<GenericKey<64>, ItemPointer, GenericComparator<64>,
GenericEqualityChecker<64>>;
template class BWTreeIndex<GenericKey<96>, ItemPointer, GenericComparator<96>,
GenericEqualityChecker<96>>;
template class BWTreeIndex<GenericKey<128>, ItemPointer, GenericComparator<128>,
GenericEqualityChecker<128>>;
template class BWTreeIndex<GenericKey<256>, ItemPointer, GenericComparator<256>,
GenericEqualityChecker<256>>;
template class BWTreeIndex<GenericKey<512>, ItemPointer, GenericComparator<512>,
GenericEqualityChecker<512>>;

template class BWTreeIndex<TupleKey, ItemPointer, TupleKeyComparator,
TupleKeyEqualityChecker>;

}  // End index namespace
}  // End peloton namespace
