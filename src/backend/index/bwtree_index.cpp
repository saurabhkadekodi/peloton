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

namespace peloton {
namespace index {

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
bool BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::InsertEntry(
    const storage::Tuple *key, const ItemPointer location) {
  uint64_t cur_id = root;
  uint64_t prev_id = cur_id;
  bool stop = false;
  bool try_consolidation = true;
  std::vector<KeyType> deleted_keys, deleted_indexes;
  while(!stop){
    if(try_consolidation){
      ConsolidateNode(cur_id);
      std::pair<void *, uint32_t> node_ = table.get(cur_id); 
      void *node_pointer = node_.first;
      uint32_t chain_length = node_.second;
      deleted_keys.clear();
      deleted_indexes.clear();
    }
    node_type_t cur_node_type = node_pointer->type;
    switch(cur_node_type){
      case(LEAF_BW_NODE):
        class LeafBWNode *leaf_pointer = (class LeafBWNode *)node_pointer;
        uint64_t cur_node_size = leaf_pointer->get_size();
        if(cur_node_size < max_node_size){
          return leaf_pointer->insert(key, location);
        }
        else{
          return leaf_pointer->split_node(cur_id, prev_id, key, location); 
        }
        stop = true;
        break;
      case(INTERNAL_BW_NODE):
        class InternalBWNode *internal_pointer = (class InternalBWNode *)node_pointer;
        prev_id = cur_id;
        cur_id = internal_pointer->get_child_id(key);  
        try_consolidation = true;
        break;
      case(INSERT):
        class SimpleDeltaNode *simple_pointer = (class SimpleDeltaNode *)node_pointer;
        if( equals(*key, simple_pointer->key) && 
            std::find(deleted_keys.begin(), deleted_keys.end(), *key) == deleted_keys.end())
          return false;
        node_pointer = simple_pointer->next();
        try_consolidation = false;
        break;
      case(UPDATE):
        class SimpleDeltaNode *simple_pointer = (class SimpleDeltaNode *)node_pointer;
        if( equals(*key, simple_pointer->key) && 
            std::find(deleted_keys.begin(), deleted_keys.end(), *key) == deleted_keys.end())
          return false;
        node_pointer = simple_pointer->next();
        try_consolidation = false;
        break;
      case(DELETE):
        class SimpleDeltaNode *simple_pointer = (class SimpleDeltaNode *)node_pointer;
        if(equals(*key, simple_pointer->key))
          deleted_keys.push_back(*key);
        node_pointer = simple_pointer->next();
        try_consolidation = false;
        break;
      case(SPLIT):
        class SplitDeltaNode *split_pointer = (class SplitDeltaNode *)node_pointer;
        if(comparator(*key, split_pointer->key)){
          node_pointer = split_pointer->next;
          try_consolidation = false;
        }
        else{
          prev_id = cur_id;
          cur_id = split_pointer->target_node_id;
          try_consolidation = true;
        }
        break;
      case(MERGE):
        class MergeDeltaNode *merge_pointer = (class MergeDeltaNode *)node_pointer;
        if(comparator(*key, merge_pointer->MergeKey)){
          node_pointer = merge_pointer->next;
        }
        else{
          node_pointer = merge_pointer->node_to_be_merged;
        }
        try_consolidation = false;
        break;
      case(NODE_DELETE):
        cur_id = prev_id;
        try_consolidation = true;
        break;
      case(SPLIT_INDEX):
        class SplitIndexDeltaNode *split_index_pointer = (class SplitIndexDeltaNode *)node_pointer; 
        if(std::find(deleted_indexes.begin(), deleted_indexes.end(), split_index_pointer->split_key) == deleted_indexes.end()){
          if(comparator(*key, split_index_pointer->split_key) || !comparator(*key, split_index_pointer->boundary_key)){
            node_pointer = split_index_pointer->split_parent;
            try_consolidation = false;
          }
          else{
            prev_id = cur_id;
            cur_id = split_index_pointer->new_split_node_id;
            try_consolidation = true;
          }
        }
        else{
          node_pointer = split_index_pointer->split_parent;
          try_consolidation = false;
        }
        break;
      case(DELETE_INDEX):
        class DeleteIndexDeltaNode *delete_index_pointer = (class DeleteIndexDeltaNode *)node_pointer;
        deleted_indexes.push_back(delete_index_pointer->deleted_key;
        node_pointer = delete_index_pointer->split_parent;
        break;
    }
  }

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
std::vector<ItemPointer>
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Scan(
    __attribute__((unused)) const std::vector<Value> &values,
    __attribute__((unused)) const std::vector<oid_t> &key_column_ids,
    __attribute__((unused)) const std::vector<ExpressionType> &expr_types,
    __attribute__((unused)) const ScanDirectionType& scan_direction) {
  std::vector<ItemPointer> result;
  // Add your implementation here
  return result;
}

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
std::vector<ItemPointer>
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanAllKeys() {
  std::vector<ItemPointer> result;
  // Add your implementation here
  return result;
}

/**
 * @brief Return all locations related to this key.
 */
template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
std::vector<ItemPointer>
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanKey(
    __attribute__((unused)) const storage::Tuple *key) {
  std::vector<ItemPointer> result;
  // Add your implementation here
  return result;
}

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
std::string
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
