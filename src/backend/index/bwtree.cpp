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
#include "backend/common/types.h"
#include "backend/index/index_key.h"
#include "backend/storage/tuple.h"
#include "backend/index/index.h"

#include "assert.h"
namespace peloton {
namespace index {
using namespace std;  // SUGGESTION: DON'T USE A GLOBAL USING NAMESPACE

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::BWTree(
    IndexMetadata *metadata, KeyComparator comparator, KeyEqualityChecker equals,
    ItemPointerEqualityChecker value_equals, bool allow_duplicates = false)
    : metadata(metadata), comparator(comparator), equals(equals), value_equals(value_equals), allow_duplicates(allow_duplicates) {
  min_node_size = 1;
  max_node_size = 2;
  tree_height = 1;
  root = table.Get_next_id();
  LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* root_node =
      new LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
          this->metadata, *this, root);
  table.Install(root, root_node);
  LOG_DEBUG("Successfully created a tree of min_node_size %d, max_node_size %d", min_node_size, max_node_size);
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
bool CASMappingTable<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
    Install(
        uint64_t id,
        Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_ptr) {
  if (node_ptr->chain_len == 0) {
    cas_mapping_table.insert(
        pair<uint64_t,
             Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
            id, node_ptr));
    return true;
  }

  while (true) {
    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* old_node =
        node_ptr->next;
    typename map<uint64_t, Node<KeyType, ValueType, KeyComparator,
                                KeyEqualityChecker>*>::iterator iter =
        cas_mapping_table.find(id);
    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* address =
        iter->second;
    if (__sync_bool_compare_and_swap(&address, old_node, node_ptr)) {
      break;
    }
    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* cur_node =
        cas_mapping_table[id];
    node_ptr->next = cur_node;
    node_ptr->chain_len = cur_node->chain_len + 1;
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* CASMappingTable<
    KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Get(uint64_t id) {
  typename map<uint64_t, Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>::iterator it = cas_mapping_table.find(id);
  assert(it != cas_mapping_table.end());
  return it -> second;
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
uint64_t CASMappingTable<KeyType, ValueType, KeyComparator,
                         KeyEqualityChecker>::Get_next_id() {
  uint64_t old_val = cur_max_id;
  uint64_t new_val = old_val + 1;
  while (true) {
    if (__sync_bool_compare_and_swap(&cur_max_id, old_val, new_val))
      return new_val;
    old_val = cur_max_id;
    new_val = old_val + 1;
  }
  return 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Consolidate(
    uint64_t id, bool force) {
  if (id == 0) {
    if (force) {
      id++;
      force = false;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Split_root(
    KeyType split_key, uint64_t left_pointer, uint64_t right_pointer) {
  uint64_t new_root_id = this->table.Get_next_id();
  uint64_t old_root_id = root;
  InternalBWNode<KeyType, ValueType, KeyComparator,
                 KeyEqualityChecker>* internal_pointer =
      new InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
          this->metadata, *this, new_root_id);
  internal_pointer->leftmost_pointer = left_pointer;
  internal_pointer->key_list.insert(
      pair<KeyType, uint64_t>(split_key, right_pointer));
  internal_pointer->left_sibling = 0;
  internal_pointer->right_sibling = 0;
  internal_pointer->chain_len = 0;
  table.Install(new_root_id, internal_pointer);
  // TODO: Need to handle race conditions here
  tree_height++;
  return __sync_bool_compare_and_swap(&root, old_root_id, new_root_id);
}


template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
vector<ValueType> BWTree<KeyType, ValueType, KeyComparator,
                         KeyEqualityChecker>::Search_key(KeyType key) {
  vector<ValueType> ret_vector;
  uint64_t* path = (uint64_t*)malloc(tree_height * sizeof(uint64_t));
  uint64_t location;
  uint64_t node_id = Search(key, path, location);
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer =
      table.Get(node_id);

  multimap<KeyType, ValueType, KeyComparator> deleted_keys(KeyComparator(this->metadata));
  while (node_pointer) {
    switch (node_pointer->type) {
      case (INSERT): {
        DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            simple_pointer = dynamic_cast<
            DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
            node_pointer);

        if (equals(key, simple_pointer->key)) {
          bool push = true;
          pair<typename multimap<KeyType, ValueType>::iterator,
               typename multimap<KeyType, ValueType>::iterator> values =
              deleted_keys.equal_range(simple_pointer->key);
          typename multimap<KeyType, ValueType, KeyComparator>::iterator iter;
          for (iter = values.first; iter != values.second; iter++) {
            if(value_equals(iter->second, simple_pointer->value)){
            	push = false;
            }
          }
          if (push) {
            ret_vector.push_back(simple_pointer->value);
          }
        }
      } break;
      case (DELETE): {
        DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            simple_pointer = nullptr;
        simple_pointer = dynamic_cast<
            DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
            node_pointer);
        deleted_keys.insert(pair<KeyType, ValueType>(simple_pointer->key,
                                                     simple_pointer->value));
      } break;
      case (SPLIT): {
        // SplitDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>
        // *split_pointer = nullptr;
        // split_pointer = dynamic_cast<SplitDeltaNode<KeyType, ValueType,
        // KeyComparator, KeyEqualityChecker>*>(node_pointer);
        // It should never be the case that we need to go to the right side of a
        // split pointer
        // That would have been done be the search function which would give us
        // the correct id
        // Hence we just need to continue to the next node in this delta chain
      } break;
      case (MERGE): {
        MergeDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            merge_pointer = nullptr;
        merge_pointer =
            dynamic_cast<MergeDeltaNode<KeyType, ValueType, KeyComparator,
                                        KeyEqualityChecker>*>(node_pointer);
        if (!comparator(key, merge_pointer->merge_key)) {
          node_pointer = merge_pointer->node_to_be_merged;
          continue;
        }
      } break;
      case (LEAF_BW_NODE): {
        LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            leaf_pointer = nullptr;
        leaf_pointer = dynamic_cast<
            LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
            node_pointer);

        pair<typename multimap<KeyType, ValueType>::iterator,
             typename multimap<KeyType, ValueType>::iterator> values =
            leaf_pointer->kv_list.equal_range(key);
        typename multimap<KeyType, ValueType, KeyComparator>::iterator iter;
        for (iter = values.first; iter != values.second; iter++) {
          bool push = true;
          pair<typename multimap<KeyType, ValueType>::iterator,
               typename multimap<KeyType, ValueType>::iterator> deleted_values =
              deleted_keys.equal_range(key);
          typename multimap<KeyType, ValueType, KeyComparator>::iterator
              deleted_iter;
          for (deleted_iter = deleted_values.first;
               deleted_iter != deleted_values.second; deleted_iter++) {
            if(value_equals(deleted_iter->second, iter->second)){
            	push = false;
            	break;
            }
          }
          if (push) {
            ret_vector.push_back(iter->second);
          }
        }
      } break;
      default:
        // This should never occur
      assert(false);
        break;
    }

    node_pointer = node_pointer->next;
  }
  free(path);
  return ret_vector;
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Insert(
    KeyType key, ValueType value) {
  uint64_t* path = (uint64_t*)malloc(sizeof(uint64_t) * tree_height);
  uint64_t location;
  uint64_t node_id = Search(key, path, location);

  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer =
      table.Get(node_id);
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* cur_pointer =
      node_pointer;

  bool can_insert = false;
  while (cur_pointer->next) {
    if (cur_pointer -> type == INSERT || cur_pointer -> type == DELETE)
    {
      DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* delta_node = dynamic_cast<
            DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
            cur_pointer);
      if (equals(delta_node -> key, key) && value_equals(delta_node -> value, value) && !allow_duplicates)
      {
        if (!can_insert && cur_pointer -> type == INSERT)
        {
          free(path);
          return false;
        } else if (cur_pointer -> type == DELETE) {
          // If we ever found a delete record, we can insert
          can_insert = true;
        }
      }
    }
    cur_pointer = cur_pointer->next;
  }

  LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
      leaf_pointer = dynamic_cast<
          LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
          cur_pointer);
  if (leaf_pointer -> kv_list.find(key) != leaf_pointer -> kv_list.end() && !allow_duplicates)
  {
    free(path);
    return false;
  }

  uint64_t cur_node_size = Get_size(node_id);
  if (cur_node_size < max_node_size) {
    free(path);
    auto retval = leaf_pointer->Leaf_insert(key, value);
    auto root_node = table.Get(root);
    Traverse(root_node);
    return retval;
  } else {
    auto retval = leaf_pointer->Leaf_split(path, location, key, value);
    auto root_node = table.Get(root);
    Traverse(root_node);
    return retval;
  }
  assert(false);
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Delete(
    KeyType key, ValueType value) {
  uint64_t* path = (uint64_t*)malloc(sizeof(uint64_t) * tree_height);
  uint64_t location;
  uint64_t node_id = Search(key, path, location);

  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer =
      table.Get(node_id);
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* cur_pointer =
      node_pointer;

  while (cur_pointer->next) {
    if (cur_pointer -> type == INSERT || cur_pointer -> type == DELETE)
    {
            DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* delta_node = dynamic_cast<
            DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
            cur_pointer);
      if (equals(delta_node -> key, key) && value_equals(delta_node -> value, value)) {
        // Can't delete a key,value twice, but we return false anyway
        free(path);
        return false;
      }
    }
    cur_pointer = cur_pointer->next;
  }

  LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
      leaf_pointer = dynamic_cast<
          LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
          cur_pointer);
  if (leaf_pointer -> kv_list.find(key) == leaf_pointer -> kv_list.end())
  {
    // attempting to delete a non-existing key
    free(path);
    return false;
  }

  uint64_t cur_node_size = Get_size(node_id);
  if (cur_node_size > min_node_size) {
    free(path);
    auto retval = leaf_pointer->Leaf_delete(key, value);
    auto root_node = table.Get(root);
    Traverse(root_node);
    return retval;
  } else {
    auto retval = leaf_pointer->Leaf_merge(path, location, key, value);
    auto root_node = table.Get(root);
    Traverse(root_node);
    return retval;
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
uint64_t BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Search(
    KeyType key, uint64_t* path, uint64_t& location) {
  uint64_t cur_id = root;
  // TODO: why prev_id not used?
  uint64_t prev_id = cur_id;
  bool stop = false;
  bool try_consolidation = true;
  prev_id = prev_id; // TODO: this is a work around for not being used
  int merge_dir = -2;
  bool need_redirection = false;
  // multimap<KeyType, ValueType, KeyComparator> deleted_keys(KeyComparator(this->metadata));
  set<KeyType, KeyComparator> deleted_indexes(KeyComparator(this->metadata));
  uint64_t index = 0;
  location = 0;
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer =
      nullptr;
  while (!stop) {
    if (try_consolidation) {
      Consolidate(cur_id, false);
      node_pointer = table.Get(cur_id);
      // deleted_keys.clear();
      deleted_indexes.clear();
      path[index] = cur_id;
      index++;
      location++;
    }
    assert(node_pointer!= nullptr);
    switch (node_pointer->type) {
      case (LEAF_BW_NODE): {
        LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            leaf_pointer =
                dynamic_cast<LeafBWNode<KeyType, ValueType, KeyComparator,
                                        KeyEqualityChecker>*>(node_pointer);
        if (!need_redirection)
        {
          return leaf_pointer->id;
        } else {
          // The search to R is thus redirected
          if (merge_dir == LEFT)
          {
            return leaf_pointer -> right_sibling;
          } else if (merge_dir == RIGHT) {
            return leaf_pointer -> left_sibling;
          } else
            assert(false);
        }
      } break;
      case (INTERNAL_BW_NODE): {
        InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            internal_pointer =
                dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator,
                                            KeyEqualityChecker>*>(node_pointer);
        prev_id = cur_id;
        cur_id = internal_pointer->Get_child_id(key);
        try_consolidation = true;
      } break;
      case (INSERT): {
        DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            simple_pointer = dynamic_cast<
            DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
            node_pointer);
        if(equals(key, simple_pointer->key))
            return simple_pointer -> id;
        node_pointer = simple_pointer->next;
        try_consolidation = false;
      } break;
      // case(UPDATE):
      //   DeltaNode *simple_pointer = (DeltaNode *)node_pointer;
      //   if( equals(key, simple_pointer->key) &&
      //       find(deleted_keys.begin(), deleted_keys.end(), *key) ==
      //       deleted_keys.end())
      //     return false;
      //   node_pointer = simple_pointer->next();
      //   try_consolidation = false;
      //   break;
      case (DELETE): {
        DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            simple_pointer = dynamic_cast<
            DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
            node_pointer);
        if(equals(key, simple_pointer->key))
          return simple_pointer->id;
        // deleted_keys.push_back(key);
        node_pointer = simple_pointer->next;
        try_consolidation = false;
      } break;
      case (SPLIT): {
        SplitDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            split_pointer =
                dynamic_cast<SplitDeltaNode<KeyType, ValueType, KeyComparator,
                                            KeyEqualityChecker>*>(node_pointer);
        if (comparator(key, split_pointer->split_key)) {
          node_pointer = split_pointer->next;
          try_consolidation = false;
        } else {
          prev_id = cur_id;
          cur_id = split_pointer->target_node_id;
          try_consolidation = true;
        }
      } break;
      case (MERGE): {
        MergeDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            merge_pointer =
                dynamic_cast<MergeDeltaNode<KeyType, ValueType, KeyComparator,
                                            KeyEqualityChecker>*>(node_pointer);
        if (comparator(key, merge_pointer->merge_key)) {
          node_pointer = merge_pointer->next;
        } else {
          node_pointer = merge_pointer->node_to_be_merged;
        }
        try_consolidation = false;
      } break;
      case (REMOVE): {
        RemoveDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* remove_pointer = dynamic_cast<RemoveDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(node_pointer);
        merge_dir = remove_pointer -> direction;
        if (merge_dir == UP) {
          // I will let you access it anyway
          node_pointer = remove_pointer->next;
          try_consolidation = false;
        } else {
          prev_id = cur_id;
          cur_id = remove_pointer -> merged_to_id;
          need_redirection = true;
          try_consolidation = true;
        }
      }
      // The following has been resolved in another manner
        // Should try to complete the SMO instead of going to the parent and
        // waiting for the merge thread to complete it
        break;
      case (SPLIT_INDEX): {
        SplitIndexDeltaNode<KeyType, ValueType, KeyComparator,
                            KeyEqualityChecker>* split_index_pointer =
            dynamic_cast<SplitIndexDeltaNode<KeyType, ValueType, KeyComparator,
                                             KeyEqualityChecker>*>(
                node_pointer);
        if (deleted_indexes.find(split_index_pointer->split_key) ==
            deleted_indexes.end()) {
          if (comparator(key, split_index_pointer->split_key) ||
              (!comparator(key, split_index_pointer->boundary_key) &&
                equals(key, split_index_pointer->boundary_key))) {
            node_pointer = split_index_pointer->next;
            try_consolidation = false;
          } else {
            prev_id = cur_id;
            cur_id = split_index_pointer->new_split_node_id;
            try_consolidation = true;
          }
        } else {
          node_pointer = split_index_pointer->next;
          try_consolidation = false;
        }
      } break;
      case (REMOVE_INDEX): {
        RemoveIndexDeltaNode<KeyType, ValueType, KeyComparator,
                             KeyEqualityChecker>* delete_index_pointer =
            dynamic_cast<RemoveIndexDeltaNode<KeyType, ValueType, KeyComparator,
                                              KeyEqualityChecker>*>(
                node_pointer);
        deleted_indexes.insert(delete_index_pointer->deleted_key);
        node_pointer = delete_index_pointer->next;
      } break;
    }
  }
  assert(false);
  return 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
bool LeafBWNode<KeyType, ValueType, KeyComparator,
                KeyEqualityChecker>::Leaf_insert(KeyType key, ValueType value) {
  DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* delta =
      new DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
          this->my_tree, this->id, INSERT);
  delta->key = key;
  delta->value = value;
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_ =
      (this->my_tree).table.Get(this->id);
  delta->next = node_;
  uint32_t chain_len = node_->chain_len;
  delta->chain_len = chain_len + 1;
  bool result = this->my_tree.table.Install(this->id, delta);
  return result;
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
bool LeafBWNode<KeyType, ValueType, KeyComparator,
                KeyEqualityChecker>::Leaf_delete(KeyType key, ValueType value) {
  DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* delta =
      new DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
          this->my_tree, this->id, DELETE);
  delta->key = key;
  delta->value = value;
  delta->type = DELETE;
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_ =
      this->my_tree.table.Get(this->id);
  delta->next = node_;
  uint32_t chain_len = node_->chain_len;
  delta->chain_len = chain_len + 1;
  return this->my_tree.table.Install(this->id, delta);
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
bool LeafBWNode<KeyType, ValueType, KeyComparator,
                KeyEqualityChecker>::Leaf_split(uint64_t* path, uint64_t index,
                                                KeyType key, ValueType value) {
  bool result = this->my_tree.Consolidate(this->id, true);
  if (!result) {
    return result;
  }
  uint64_t new_node_id = this->my_tree.table.Get_next_id();
  // This is Q
  LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
      new_leaf_node =
          new LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
              this->my_tree.metadata, this->my_tree, new_node_id);
  new_leaf_node->left_sibling = this -> id;
  new_leaf_node -> right_sibling = this -> right_sibling;

  uint64_t count = kv_list.size();
  typename multimap<KeyType, ValueType, KeyComparator>::iterator key_it = kv_list.begin();
  advance(key_it, count / 2);
  KeyType split_key = key_it -> first;
  key_it = kv_list.end();
  KeyType boundary_key = key_it -> first;

  typename multimap<KeyType, ValueType, KeyComparator>::iterator
      split_iterator = kv_list.begin();
  advance(split_iterator, count / 2);

  for (; split_iterator != kv_list.end(); split_iterator++) {
    pair<KeyType, ValueType> new_entry = *split_iterator;
    new_leaf_node -> kv_list.insert(new_entry);
  }

  if (this->my_tree.comparator(key, split_key)) {
    this -> Leaf_insert(key, value);    // Prevent race condition
  } else {
    // This is ok since new_leaf_node is just created
    new_leaf_node-> kv_list.insert(pair<KeyType, ValueType>(key, value));
  }

  // Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_ =
  //     (this->my_tree).table.Get(this->id);
  SplitDeltaNode<KeyType, ValueType, KeyComparator,
                 KeyEqualityChecker>* split_node =
      new SplitDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
          this->my_tree, this->id);
  split_node->next = this;
  split_node->target_node_id = new_node_id;
  split_node->split_key = key;

  uint32_t chain_len = this ->chain_len;
  bool ret_val = true;
  ret_val = this->my_tree.table.Install(new_node_id, new_leaf_node);
  if (!ret_val) return false;
  split_node->chain_len = chain_len + 1;
  ret_val = this->my_tree.table.Install(this->id, split_node);
  if (!ret_val) return false;

  if (index > 0) {
    uint64_t parent_id = path[index - 1];
    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* parent_node_ =
        this->my_tree.table.Get(parent_id);
    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* cur_pointer =
        parent_node_;
    while (cur_pointer->next) cur_pointer = cur_pointer->next;

    InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
        internal_pointer =
            dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator,
                                        KeyEqualityChecker>*>(cur_pointer);

    uint64_t parent_size = this->my_tree.Get_size(parent_id);
    if (parent_size + 1 < this->my_tree.max_node_size)
      ret_val = internal_pointer->Internal_insert(split_key, boundary_key,
                                                  new_node_id);
    else
      ret_val = internal_pointer->Internal_split(path, index - 1, split_key,
                                                 boundary_key, new_node_id);
    return ret_val;
  } else {
    ret_val = this->my_tree.Split_root(split_key, this->id, new_node_id);
    /* If the split root fails, return failure and let the user decide what to
       do next. This might be the only case in which we return on failure */
    free(path);
    return ret_val;
  }
  assert(false);
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
bool InternalBWNode<KeyType, ValueType, KeyComparator,
                    KeyEqualityChecker>::Internal_merge(uint64_t* path,
                                                        uint64_t index,
                                                        KeyType merge_key) {

  // typename multimap<KeyType, uint64_t, KeyComparator>::iterator iter =
  //     key_list.find(merge_key);
  // key_list.erase(iter);
  this -> Internal_delete(merge_key);
  bool ret_val = this->my_tree.Consolidate(this->id, true);
  if (!ret_val) {
    return ret_val;
  }
  // We are the root node
  if (index == 0) return true;
  uint64_t neighbour_node_id = this -> left_sibling;
  int direction = LEFT;
  if (this->left_sibling != 0) {
    neighbour_node_id = this->left_sibling;
  } else if (this -> right_sibling != 0) {
    neighbour_node_id = this -> right_sibling;
    direction = RIGHT;
  } else
    direction = UP;

  uint64_t parent_id = path[index - 1];
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
      parent_node_pointer = this->my_tree.table.Get(parent_id);
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* cur_pointer =
      parent_node_pointer;
  while (cur_pointer->next) cur_pointer = cur_pointer->next;

  InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
      parent_pointer =
          dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator,
                                      KeyEqualityChecker>*>(cur_pointer);
  if (direction == UP)
  {
      // TODO: how to merge up??

  }
    this->my_tree.Consolidate(neighbour_node_id, true);

  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* n_node_ =
      this->my_tree.table.Get(neighbour_node_id);
  InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
      n_node_pointer = dynamic_cast<
          InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
          n_node_);
    // (a) Posting remove node delta
    RemoveDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
        remove_node = new RemoveDeltaNode<KeyType, ValueType, KeyComparator,
                                          KeyEqualityChecker>(
            this->my_tree, this -> id);
    remove_node->next = this;
    remove_node->merged_to_id = neighbour_node_id;
    remove_node ->direction = direction;
    uint32_t chain_len = this->chain_len;
    remove_node->chain_len = chain_len + 1;
    ret_val = this->my_tree.table.Install(this -> id, remove_node);
    if (!ret_val) return false;

    // (b) posting merge delta
    MergeDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
          merge_node =
              new MergeDeltaNode<KeyType, ValueType, KeyComparator,
                                 KeyEqualityChecker>(this->my_tree, neighbour_node_id);
      merge_node->node_to_be_merged = this;
      merge_node->next = n_node_pointer;
      merge_node->chain_len = n_node_pointer -> chain_len + 1;
      merge_node->merge_key = n_node_pointer->key_list.end()->first;
      ret_val = this->my_tree.table.Install(neighbour_node_id, merge_node);
      if (!ret_val) return false;

      // (c) posting index term delete delta or key redistribution
      uint64_t total_count = this->my_tree.Get_size(this->id) +
                           this->my_tree.Get_size(neighbour_node_id);


      // Given we are now in (b) state, we want either complete the merge
      // or redistribute the keys. Make sure to consolidate before proceed
      // TODO: Consolidate() is called
      if (total_count > this->my_tree.max_node_size) {
        // Redistribute keys
      uint64_t new_node_id = this->my_tree.table.Get_next_id();
      // TODO: should we pick a new id, no, maybe keep the old ones
        InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            L =
                new InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
                    this->my_tree.metadata, this->my_tree, new_node_id);
        new_node_id = this->my_tree.table.Get_next_id();
                InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            R =
                new InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
                    this->my_tree.metadata, this->my_tree, new_node_id);

        L -> left_sibling = n_node_pointer -> left_sibling;
        L -> right_sibling = R -> id;
        R -> left_sibling = L -> id;
        R -> right_sibling = this -> right_sibling;

        multimap<KeyType, uint64_t, KeyComparator> temp(KeyComparator(this->my_tree.metadata));
        uint64_t half_count = total_count / 2;
        if (direction == LEFT)
        {
                  typename multimap<KeyType, uint64_t, KeyComparator>::iterator
            left_iterator = n_node_pointer->key_list.begin();
        typename multimap<KeyType, uint64_t, KeyComparator>::iterator
            right_iterator = this->key_list.begin();

        for (; left_iterator != n_node_pointer->key_list.end(); ++left_iterator)
          {
            KeyType key = left_iterator->first;
             uint64_t value = left_iterator->second;
             temp.insert(pair<KeyType, uint64_t>(key, value));
          }
          for(; right_iterator != this->key_list.end();++right_iterator) {
                        KeyType key = right_iterator->first;
             uint64_t value = right_iterator->second;
             temp.insert(pair<KeyType, uint64_t>(key, value));
          }
        } else if (direction==RIGHT) {
              typename multimap<KeyType, uint64_t, KeyComparator>::iterator
            left_iterator = this->key_list.begin();
        typename multimap<KeyType, uint64_t, KeyComparator>::iterator
            right_iterator = n_node_pointer->key_list.begin();

        for (; left_iterator != this->key_list.end(); ++left_iterator)
          {
            KeyType key = left_iterator->first;
             uint64_t value = left_iterator->second;
             temp.insert(pair<KeyType, uint64_t>(key, value));
          }
          for(; right_iterator != n_node_pointer->key_list.end();++right_iterator) {
                        KeyType key = right_iterator->first;
             uint64_t value = right_iterator->second;
             temp.insert(pair<KeyType, uint64_t>(key, value));
          }

        }
        assert(temp.size()==total_count);
typename multimap<KeyType, uint64_t, KeyComparator>::iterator temp_iterator= temp.begin();
for (int i = 0; i < half_count; ++i)
{
                     KeyType key = temp_iterator->first;
             uint64_t value = temp_iterator->second;
L -> key_list.insert(pair<KeyType, uint64_t>(key, value));
temp_iterator++;
}
for (int i = half_count;i<=total_count;++i) {
                       KeyType key = temp_iterator->first;
             uint64_t value = temp_iterator->second;
R -> key_list.insert(pair<KeyType, uint64_t>(key, value));
temp_iterator++;
}
      KeyType new_split_key = L->key_list.end()->first;
     ret_val = this->my_tree.table.Install(L -> id, L);
      if (!ret_val) return false;
           ret_val = this->my_tree.table.Install(R -> id, R);
      if (!ret_val) return false;

      // TODO: Free two old leaf nodes


        ret_val = parent_pointer->Internal_delete(merge_node->merge_key);
      if (!ret_val) return false;
      // we treat it as a split; sp means special purpose
      KeyType sp_boundary_key = R -> key_list.end()->first;

        ret_val = parent_pointer->Internal_insert(new_split_key, sp_boundary_key, R -> id);
      if (!ret_val) return false;

      return ret_val;
    } else { // Normal merge case
  typename multimap<KeyType, uint64_t, KeyComparator>::iterator this_iterator= this -> key_list.begin();
  for (;this_iterator != this -> key_list.end(); this_iterator++) {
                            KeyType key = this_iterator->first;
             uint64_t value = this_iterator->second;
             // This relies on the split_key <= search_key && search_key <= boundary_key
    n_node_pointer -> Internal_insert(key, key, value);
  }
      uint64_t parent_size = this->my_tree.Get_size(parent_id);
      if (parent_size - 1 > this->my_tree.min_node_size || index == 0)
        ret_val = parent_pointer->Internal_delete(merge_node->merge_key);
      else
        ret_val = parent_pointer->Internal_merge(path, index - 1,
                                                 merge_node->merge_key);
      return ret_val;
  }

  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
bool LeafBWNode<KeyType, ValueType, KeyComparator,
                KeyEqualityChecker>::Leaf_merge(uint64_t* path, uint64_t index,
                                                KeyType key,
                                                ValueType value) {
  // pair<typename multimap<KeyType, ValueType>::iterator,
  //      typename multimap<KeyType, ValueType>::iterator> values =
  //     kv_list.equal_range(key);
  // typename multimap<KeyType, ValueType, KeyComparator>::iterator iter =
  //     values.first;
  // for(iter=values.first;iter!=values.second;iter++){
  //  if(this -> my_tree.value_equals(iter->second, value)){
  //    kv_list.erase(iter);
  //    break;
  //  }
  // }
  this -> Leaf_delete(key, value);
  bool ret_val = this->my_tree.Consolidate(this->id, true);
  if (!ret_val) {
    return ret_val;
  }
  // We are the root node
  if (index == 0) return true;

  uint64_t neighbour_node_id = this -> left_sibling;
  int direction = LEFT;
  if (this->left_sibling != 0) {
    neighbour_node_id = this->left_sibling;
  } else if (this -> right_sibling != 0) {
    neighbour_node_id = this -> right_sibling;
    direction = RIGHT;
  } else
    direction = UP;

      uint64_t parent_id = path[index - 1];
      Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
          parent_node_pointer = this->my_tree.table.Get(parent_id);
      Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* cur_pointer =
          parent_node_pointer;
      while (cur_pointer->next) cur_pointer = cur_pointer->next;

      InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
          parent_pointer =
              dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator,
                                          KeyEqualityChecker>*>(cur_pointer);
    if (direction == UP)
    {
        // TODO: how to merge up??

    }

    this->my_tree.Consolidate(neighbour_node_id, true);
    // (a) Posting remove node delta

    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* n_node_ =
        this->my_tree.table.Get(neighbour_node_id);
    LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
        n_node_pointer = dynamic_cast<
            LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
            n_node_);

      RemoveDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
          remove_node = new RemoveDeltaNode<KeyType, ValueType, KeyComparator,
                                            KeyEqualityChecker>(
              this->my_tree, this -> id);
      remove_node->next = this;
      remove_node->merged_to_id = neighbour_node_id;
      remove_node ->direction = direction;
      uint32_t chain_len = this->chain_len;
      remove_node->chain_len = chain_len + 1;
      ret_val = this->my_tree.table.Install(this -> id, remove_node);
      if (!ret_val) return false;

    // (b) posting merge delta
      MergeDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
          merge_node =
              new MergeDeltaNode<KeyType, ValueType, KeyComparator,
                                 KeyEqualityChecker>(this->my_tree, neighbour_node_id);
      merge_node->node_to_be_merged = this;
      merge_node->next = n_node_pointer;
      merge_node->chain_len = n_node_pointer -> chain_len + 1;
      merge_node->merge_key = n_node_pointer->kv_list.end()->first;
      ret_val = this->my_tree.table.Install(neighbour_node_id, merge_node);
      if (!ret_val) return false;

      // (c) posting index term delete delta or key redistribution
      uint64_t total_count = this->my_tree.Get_size(this->id) +
                           this->my_tree.Get_size(neighbour_node_id);


      // Given we are now in (b) state, we want either complete the merge
      // or redistribute the keys. Make sure to consolidate before proceed
      // TODO: Consolidate() is called
      if (total_count > this->my_tree.max_node_size) {
        // Redistribute keys
      uint64_t new_node_id = this->my_tree.table.Get_next_id();
      // TODO: should we pick a new id, no, maybe keep the old ones
        LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            L =
                new LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
                    this->my_tree.metadata, this->my_tree, new_node_id);
        new_node_id = this->my_tree.table.Get_next_id();
                LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            R =
                new LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
                    this->my_tree.metadata, this->my_tree, new_node_id);

        L -> left_sibling = n_node_pointer -> left_sibling;
        L -> right_sibling = R -> id;
        R -> left_sibling = L -> id;
        R -> right_sibling = this -> right_sibling;

        multimap<KeyType, ValueType, KeyComparator> temp(KeyComparator(this->my_tree.metadata));
        uint64_t half_count = total_count / 2;
        if (direction == LEFT)
        {
                  typename multimap<KeyType, ValueType, KeyComparator>::iterator
            left_iterator = n_node_pointer->kv_list.begin();
        typename multimap<KeyType, ValueType, KeyComparator>::iterator
            right_iterator = this->kv_list.begin();

        for (; left_iterator != n_node_pointer->kv_list.end(); ++left_iterator)
          {
            KeyType key = left_iterator->first;
             ValueType value = left_iterator->second;
             temp.insert(pair<KeyType, ValueType>(key, value));
          }
          for(; right_iterator != this->kv_list.end();++right_iterator) {
                        KeyType key = right_iterator->first;
             ValueType value = right_iterator->second;
             temp.insert(pair<KeyType, ValueType>(key, value));
          }
        } else if (direction==RIGHT) {
              typename multimap<KeyType, ValueType, KeyComparator>::iterator
            left_iterator = this->kv_list.begin();
        typename multimap<KeyType, ValueType, KeyComparator>::iterator
            right_iterator = n_node_pointer->kv_list.begin();

        for (; left_iterator != this->kv_list.end(); ++left_iterator)
          {
            KeyType key = left_iterator->first;
             ValueType value = left_iterator->second;
             temp.insert(pair<KeyType, ValueType>(key, value));
          }
          for(; right_iterator != n_node_pointer->kv_list.end();++right_iterator) {
                        KeyType key = right_iterator->first;
             ValueType value = right_iterator->second;
             temp.insert(pair<KeyType, ValueType>(key, value));
          }

        }
        assert(temp.size()==total_count);
typename multimap<KeyType, ValueType, KeyComparator>::iterator temp_iterator= temp.begin();
for (int i = 0; i < half_count; ++i)
{
                     KeyType key = temp_iterator->first;
             ValueType value = temp_iterator->second;
L -> kv_list.insert(pair<KeyType, ValueType>(key, value));
temp_iterator++;
}
for (int i = half_count;i<=total_count;++i) {
                       KeyType key = temp_iterator->first;
             ValueType value = temp_iterator->second;
R -> kv_list.insert(pair<KeyType, ValueType>(key, value));
temp_iterator++;
}
      KeyType new_split_key = L->kv_list.end()->first;
     ret_val = this->my_tree.table.Install(L -> id, L);
      if (!ret_val) return false;
           ret_val = this->my_tree.table.Install(R -> id, R);
      if (!ret_val) return false;

      // TODO: Free two old leaf nodes


        ret_val = parent_pointer->Internal_delete(merge_node->merge_key);
      if (!ret_val) return false;

      // we treat it as a split; sp means special purpose
      KeyType sp_boundary_key = R -> kv_list.end()->first;
        ret_val = parent_pointer->Internal_insert(new_split_key, sp_boundary_key, R -> id);
      if (!ret_val) return false;

      return ret_val;
    } else { // Normal merge case
  typename multimap<KeyType, ValueType, KeyComparator>::iterator this_iterator= this -> kv_list.begin();
  for (;this_iterator != this -> kv_list.end(); this_iterator++) {
                            KeyType key = this_iterator->first;
             ValueType value = this_iterator->second;
    n_node_pointer -> Leaf_insert(key, value);
  }
      uint64_t parent_size = this->my_tree.Get_size(parent_id);
      if (parent_size - 1 > this->my_tree.min_node_size || index == 0)
        ret_val = parent_pointer->Internal_delete(merge_node->merge_key);
      else
        ret_val = parent_pointer->Internal_merge(path, index - 1,
                                                 merge_node->merge_key);
      return ret_val;
  }

  return false;

  /*
    ret_val = this->my_tree.table.Install(this->id, split_node, chain_len+1);
    if(!ret_val)
      return false;

    if(index > 0){
        uint64_t parent_id = path[index-1];
        pair<Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*,
    uint32_t> parent_node_ = this->my_tree.table.Get(parent_id);
        Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
    node_pointer = parent_node_.first;

        Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker> *cur_pointer
    = node_pointer;
        while(cur_pointer->next)
          cur_pointer = cur_pointer->next;

        InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
    internal_pointer = dynamic_cast<InternalBWNode<KeyType, ValueType,
    KeyComparator, KeyEqualityChecker>*>(cur_pointer);

        uint64_t parent_size = internal_pointer->Get_size();
        if(parent_size + 1 < this->my_tree.max_node_size)
          ret_val = node_pointer->Insert(split_key, boundary_key, new_node_id);
        else
          ret_val = node_pointer->Split(path, index - 1, split_key,
    boundary_key, new_node_id);
        return ret_val;
    }
    else{
      //SplitRoot
    }
    */
}


template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
bool LeafBWNode<KeyType, ValueType, KeyComparator,
                KeyEqualityChecker>::Consolidate() {
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_ =
      this->my_tree.table.Get(this->id);

  uint64_t new_id = this->my_tree.table.Get_next_id();
  LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
      new_leaf_node =
          new LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
              this->my_tree.metadata, this->my_tree, new_id);
  multimap<KeyType, ValueType, KeyComparator> insert_set(KeyComparator(this->my_tree.metadata));
  multimap<KeyType, ValueType, KeyComparator> delete_set(KeyComparator(this->my_tree.metadata));
  DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* temp =
      dynamic_cast<
          DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
          node_);
  LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* leaf_node =
      this;
  bool stop = false;
  while (!stop) {
    if (temp->type == DELETE) {
      delete_set.insert(pair<KeyType, ValueType>(temp->key, temp->value));
    } else if (temp->type == INSERT) {
      insert_set.insert(pair<KeyType, ValueType>(temp->key, temp->value));
    }
    if (temp->next->type == LEAF_BW_NODE) {
      stop = true;
    } else {
      temp = dynamic_cast<
          DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
          temp->next);
    }
  }
  assert(leaf_node != nullptr);
  while (!insert_set.empty()) {
    typename multimap<KeyType, ValueType, KeyComparator>::iterator it =
        insert_set.begin();
    KeyType key = it->first;
    typename multimap<KeyType, ValueType, KeyComparator>::iterator dit =
        delete_set.find(key);
    if (dit == delete_set.end()) {
      leaf_node->kv_list.insert(
          pair<KeyType, ValueType>(it->first, it->second));
    } else {
      delete_set.erase(dit);
    }
    insert_set.erase(it);
  }
  // there can be multiple delete key deltas
  while (!delete_set.empty()) {
    typename multimap<KeyType, ValueType, KeyComparator>::iterator dit =
        delete_set.begin();
    KeyType key = dit->first;
    typename multimap<KeyType, ValueType, KeyComparator>::iterator it =
        leaf_node->kv_list.find(key);
    if (it != this->kv_list.end()) {
      this->kv_list.erase(it);
    }
    delete_set.erase(dit);
  }
  while (!leaf_node->kv_list.empty()) {
    typename multimap<KeyType, ValueType, KeyComparator>::iterator it =
        leaf_node->kv_list.begin();
    new_leaf_node->kv_list.insert(
        pair<KeyType, ValueType>(it->first, it->second));
    leaf_node->kv_list.erase(it);
  }
  bool result = this->my_tree.table.Install(
      new_id, new_leaf_node);  // TODO: what is the correct chain length?
  return result;
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
uint64_t BWTree<KeyType, ValueType, KeyComparator,
                KeyEqualityChecker>::Get_size(uint64_t node_id) {
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer =
      table.Get(node_id);
  uint64_t count = 0;
  bool end = false;

  // switch(node_pointer->type){
  //   case(LEAF_BW_NODE):
  //     return leaf_pointer->id;
  //     break;
  //   case(INTERNAL_BW_NODE):
  //     InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
  //     internal_pointer = dynamic_cast<InternalBWNode<KeyType, ValueType,
  //     KeyComparator, KeyEqualityChecker>*>(node_pointer);
  //     prev_id = cur_id;
  //     cur_id = internal_pointer->Get_child_id(key);
  //     try_consolidation = true;
  //     break;
  while (node_pointer != nullptr) {
    // Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
    // simple_pointer = nullptr;
    switch (node_pointer->type) {
      case (INSERT):
        count++;
        break;
      case (DELETE):
        count--;
        break;
      // case(UPDATE):
      //   break;
      case (LEAF_BW_NODE): {
        LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            leaf_pointer =
                dynamic_cast<LeafBWNode<KeyType, ValueType, KeyComparator,
                                        KeyEqualityChecker>*>(node_pointer);
        count += leaf_pointer->kv_list.size();
        end = true;
      } break;
      case (SPLIT):
        break;
      case (MERGE): {
        MergeDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            mdn =
                dynamic_cast<MergeDeltaNode<KeyType, ValueType, KeyComparator,
                                            KeyEqualityChecker>*>(node_pointer);
        LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            to_be_merged =
                dynamic_cast<LeafBWNode<KeyType, ValueType, KeyComparator,
                                        KeyEqualityChecker>*>(
                    mdn->node_to_be_merged);
        count += to_be_merged->kv_list.size();
      } break;
      case (REMOVE):
        // return 0;  // Remove node is still accessable
        break;
      case (SPLIT_INDEX):
        count++;
        break;
      case (REMOVE_INDEX):
        count--;
        break;
      case (INTERNAL_BW_NODE): {
        InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            internal_pointer =
                dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator,
                                            KeyEqualityChecker>*>(node_pointer);
        count += internal_pointer->key_list.size();
        end = true;
      } break;
      default:
        break;
    }
    if (!end)
      node_pointer = node_pointer->next;
    else
      break;
  }
  return count;
}



template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
vector<ItemPointer>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Scan(
     __attribute__((unused)) const vector<Value> &values,
     __attribute__((unused)) const vector<oid_t> &key_column_ids,
     __attribute__((unused)) const vector<ExpressionType> &expr_types,
     __attribute__((unused)) const ScanDirectionType &scan_direction) {

  vector<ItemPointer> result;
  KeyType index_key;
  /*
  oid_t leading_column_id = 0;
  auto key_column_ids_itr = std::find(
    key_column_ids.begin(), key_column_ids.end(), leading_column_id);

  // SPECIAL CASE : leading column id is one of the key column ids
  // and is involved in a equality constraint
  bool special_case = false;
  if (key_column_ids_itr != key_column_ids.end()) {
      auto offset = std::distance(key_column_ids.begin(), key_column_ids_itr);
      if (expr_types[offset] == EXPRESSION_TYPE_COMPARE_EQUAL) {
        special_case = true;
    }
  }
  */
  std::unique_ptr<storage::Tuple> start_key;

  // If it is a special case, we can figure out the range to scan in the index
  //if (special_case == true) {

    start_key.reset(new storage::Tuple(metadata->GetKeySchema(), true));
    index_key.SetFromKey(start_key.get());

    // Construct the lower bound key tuple

    // Set scan begin iterator
    //scan_begin_itr = container.equal_range(index_key).first;
  //}
  //
  
  uint64_t *path = (uint64_t *)malloc(tree_height *sizeof(uint64_t));
  uint64_t location;
  uint64_t leaf_id = Search(index_key, path, location);

  bool reached_end = false;
  while(!reached_end)
  {
    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer =
      this->table.Get(leaf_id);
    while(node_pointer->next)
      node_pointer = node_pointer->next;
    LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
        leaf_pointer = nullptr;
    leaf_pointer = dynamic_cast<
        LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
        node_pointer);
    switch(scan_direction)
    {
      case SCAN_DIRECTION_TYPE_FORWARD:
        if(leaf_pointer->left_sibling != 0)
          leaf_id = leaf_pointer->left_sibling;
        else
          reached_end = true;
        break;
      case SCAN_DIRECTION_TYPE_BACKWARD:
        if(leaf_pointer->right_sibling != 0)
          leaf_id = leaf_pointer->right_sibling;
        else
          reached_end = true;
        break;
      case SCAN_DIRECTION_TYPE_INVALID:
      default:
        throw Exception("Invalid scan direction \n");
        break;
    }
  }

  bool move_forward = true;

  switch(scan_direction)
  {
    case SCAN_DIRECTION_TYPE_FORWARD:
      move_forward = true;
      break;
    case SCAN_DIRECTION_TYPE_BACKWARD:
      move_forward = false;
      break;
    case SCAN_DIRECTION_TYPE_INVALID:
    default:
      throw Exception("Invalid scan direction \n");
      break;
  }
  reached_end = false;
  while(!reached_end)
  {
    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer =
      this->table.Get(leaf_id);
    while(node_pointer)
    {
      switch(node_pointer->type)
      {   
        case(INSERT):
        {
          DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            simple_pointer = dynamic_cast<
            DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
            node_pointer);
            auto inserted_key = simple_pointer->key;
            auto tuple = inserted_key.GetTupleForComparison(metadata->GetKeySchema());
            if (Index::Compare(tuple, key_column_ids, expr_types, values) == true) {
              ItemPointer location = simple_pointer->value;
              result.push_back(location);
            }
          break;
        }
        case(DELETE):
          break;
        case(SPLIT):
          break;
        case(MERGE):
          break;
        case(REMOVE):
          break;
        case(LEAF_BW_NODE):
        {
          LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            leaf_pointer = nullptr;
          leaf_pointer = dynamic_cast<
              LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
              node_pointer);
          typename multimap<KeyType, ValueType>::iterator iter = leaf_pointer->kv_list.begin();
          for(;iter!=leaf_pointer->kv_list.end();iter++)
          {   
            auto leaf_key = iter->first;
            auto tuple = leaf_key.GetTupleForComparison(metadata->GetKeySchema());
            if (Index::Compare(tuple, key_column_ids, expr_types, values) == true) {
              ItemPointer location = iter->second;
              result.push_back(location);
            }
          }
          break;
        }
      } 
      node_pointer = node_pointer->next;
    }
    LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
        leaf_pointer = nullptr;
    leaf_pointer = dynamic_cast<
        LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
        node_pointer);
    if(move_forward)
    {
      if(leaf_pointer->right_sibling)
        leaf_id = leaf_pointer->right_sibling;
      else
        reached_end = true;
    }
    else
    {
      if(leaf_pointer->left_sibling)
        leaf_id = leaf_pointer->left_sibling;
      else
        reached_end = true;
    }
  }

  return result;
}


template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
bool InternalBWNode<KeyType, ValueType, KeyComparator,
                    KeyEqualityChecker>::Internal_insert(KeyType split_key,
                                                         KeyType boundary_key,
                                                         uint64_t new_node_id) {
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer =
      this->my_tree.table.Get(this->id);
  SplitIndexDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
      split_index =
          new SplitIndexDeltaNode<KeyType, ValueType, KeyComparator,
                                  KeyEqualityChecker>(this->my_tree, this->id);
  split_index->split_key = split_key;
  split_index->boundary_key = boundary_key;
  split_index->next = node_pointer;
  split_index->new_split_node_id = new_node_id;
  uint32_t chain_len = node_pointer->chain_len;
  split_index->chain_len = chain_len + 1;
  return this->my_tree.table.Install(this->id, split_index);
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
bool InternalBWNode<KeyType, ValueType, KeyComparator,
                    KeyEqualityChecker>::Internal_split(uint64_t* path,
                                                        uint64_t index,
                                                        KeyType requested_key,
                                                        KeyType requested_boundary_key,
                                                        uint64_t new_node_id) {
  bool ret_val = true;
  ret_val = this->my_tree.Consolidate(this->id, true);
  if (!ret_val) return false;

  uint64_t new_internal_node_id = this->my_tree.table.Get_next_id();
  InternalBWNode<KeyType, ValueType, KeyComparator,
                 KeyEqualityChecker>* new_internal_node =
      new InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
          this->my_tree.metadata, this->my_tree, new_internal_node_id);
  new_internal_node->left_sibling = this -> id;
  new_internal_node-> right_sibling = this -> right_sibling;

  uint64_t count = key_list.size();
  typename multimap<KeyType, uint64_t, KeyComparator>::iterator key_it = key_list.begin();
  advance(key_it, count / 2);
  KeyType split_key = key_it -> first;
  key_it = key_list.end();
  KeyType boundary_key = key_it -> first;

  typename multimap<KeyType, uint64_t, KeyComparator>::iterator split_iterator =
      key_list.begin();
  advance(split_iterator, count / 2);

  // TODO: what about the left_most key in new_internal_node??
  for (; split_iterator != key_list.end(); split_iterator++) {
    pair<KeyType, uint64_t> new_entry = *split_iterator;
    new_internal_node->key_list.insert(new_entry);
  }

  // This is very tricky
  if(this->my_tree.comparator(requested_key, split_key)){
    // If both req_split_key and req_boundary_key fall in my range, I add
    // split_index on myself
    if(this->my_tree.comparator(requested_boundary_key, split_key)){
      this -> Internal_insert(requested_key, requested_boundary_key, this -> id);
    } else {
      // Just set requested_boundary_key = 0 but only rely on the assuption that
      // no Search function will ever use this boundary key since it's on the
      // other side
      // UPDATE: it turns out that this could not happen since the req_split/boundary key
      // won't be apart
      // this -> Internal_insert(requested_key, 0, this -> id);
      assert(false);
    }

  }
  else{
    new_internal_node->Internal_insert(requested_key, requested_boundary_key, new_internal_node_id);
  }

  // I just finished what the lower level tolds me to do, now it's for my
  // own request
  SplitDeltaNode<KeyType, ValueType, KeyComparator,
                 KeyEqualityChecker>* split_node =
      new SplitDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
          this->my_tree, this->id);
  split_node->next = this;
  split_node->target_node_id = new_internal_node_id;
  split_node->split_key = split_key;

  uint32_t chain_len = this->chain_len;
  new_internal_node->chain_len = 0;
  ret_val =
      this->my_tree.table.Install(new_internal_node_id, new_internal_node);
  if (!ret_val) return false;
  split_node->chain_len = chain_len + 1;
  ret_val = this->my_tree.table.Install(this->id, split_node);
  if (!ret_val) return false;

  if (index != 0) {
    uint64_t parent_id = path[index - 1];
    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
        parent_node_pointer = this->my_tree.table.Get(parent_id);

    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* cur_pointer =
        parent_node_pointer;
    while (cur_pointer->next) cur_pointer = cur_pointer->next;

    InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
        internal_pointer =
            dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator,
                                        KeyEqualityChecker>*>(cur_pointer);

    uint64_t parent_size = this->my_tree.Get_size(parent_id);
    if (parent_size + 1 < this->my_tree.max_node_size)
      ret_val = internal_pointer->Internal_insert(split_key, boundary_key,
                                                  new_node_id);
    else
      ret_val = internal_pointer->Internal_split(path, index - 1, split_key,
                                                 boundary_key, new_node_id);
    return ret_val;
  } else {
    ret_val = this->my_tree.Split_root(split_key, this->id, new_node_id);
    /* If the split root fails, return failure and let the user decide what to
       do next. This might be the only case in which we return on failure */
    free(path);
    return ret_val;
  }
  assert(false);
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
uint64_t InternalBWNode<KeyType, ValueType, KeyComparator,
                        KeyEqualityChecker>::Get_child_id(KeyType key) {
  typename multimap<KeyType, uint64_t, KeyComparator>::reverse_iterator iter =
      key_list.rbegin();
  for (; iter != key_list.rend(); iter++) {
    if (this->my_tree.comparator(key, iter->first)) {
      return iter->second;
    }
  }
  if (iter == key_list.rend()) return leftmost_pointer;
  assert(false);
  return 0;
}

// template <typename KeyType, typename ValueType, typename KeyComparator,
//           typename KeyEqualityChecker>
// uint64_t InternalBWNode<KeyType, ValueType, KeyComparator,
//                         KeyEqualityChecker>::Get_parent_id(uint64_t id) {
//   typename multimap<KeyType, uint64_t, KeyComparator>::reverse_iterator iter =
//       key_list.rbegin();
//   for (; iter != key_list.rend(); iter++) {
//     if (this->my_tree.comparator(key, iter->first)) {
//       return iter->second;
//     }
//   }
//   if (iter == key_list.rend()) return leftmost_pointer;
//   assert(false);
//   return 0;
// }


template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
bool InternalBWNode<KeyType, ValueType, KeyComparator,
                    KeyEqualityChecker>::Internal_delete(KeyType merged_key) {
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer =
      this->my_tree.table.Get(this->id);
  RemoveIndexDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
      remove_index =
          new RemoveIndexDeltaNode<KeyType, ValueType, KeyComparator,
                                   KeyEqualityChecker>(this->my_tree, this->id);
  remove_index->deleted_key = merged_key;
  remove_index->next = node_pointer;
  uint32_t chain_len = node_pointer->chain_len;
  remove_index->chain_len = chain_len + 1;
  return this->my_tree.table.Install(this->id, remove_index);
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator,
                KeyEqualityChecker>::Traverse(Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* n) {
    /*
     * Inorder traversal of the BW Tree for debugging purposes.
     */
    switch(n->type) {
      case INTERNAL_BW_NODE: {
        LOG_DEBUG("INTERNAL_BW_NODE id = %lu", n->id);
        InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            internal_pointer =
                dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator,
                                            KeyEqualityChecker>*>(n);
        Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
          leftmost = n->my_tree.table.Get(internal_pointer->leftmost_pointer);
        Traverse(leftmost);
        for(auto it=internal_pointer->key_list.begin(); it != internal_pointer->key_list.end(); it++) {
          auto child = n->my_tree.table.Get(it->second);
          Traverse(child);
        }
        }
        break;
      case LEAF_BW_NODE: {
        LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            leaf_pointer = dynamic_cast<LeafBWNode<KeyType, ValueType, KeyComparator,
                                            KeyEqualityChecker>*>(n);
        LOG_DEBUG("LEAF_BW_NODE id = %lu | count = %lu", n->id, leaf_pointer->kv_list.size());
        for(auto it=leaf_pointer->kv_list.begin(); it != leaf_pointer->kv_list.end(); it++) {
          auto key = it->first;
          //auto value = it->second;
          //std::cout << "key = " << key << " | value = " << value << std::endl;
          //LOG_DEBUG("key = %s", key.GetTupleForComparison(key.key_tuple_schema).GetData());
          LOG_DEBUG("key = %s", key.key_tuple);
        }
        }
        break;
      case INSERT: {
        LOG_DEBUG("INSERT id = %lu", n->id);
        n = n->next;
        }
        break;
      case UPDATE: {
        LOG_DEBUG("UPDATE id = %lu", n->id);
        n = n->next;
        }
        break;
      case DELETE: {
        LOG_DEBUG("DELETE id = %lu", n->id);
        n = n->next;
        }
        break;
      case SPLIT: {
        LOG_DEBUG("SPLIT id = %lu", n->id);
        n = n->next;
        }
        break;
      case MERGE: {
        LOG_DEBUG("MERGE id = %lu", n->id);
        n = n->next;
        }
        break;
      case REMOVE: {
        LOG_DEBUG("REMOVE id = %lu", n->id);
        }
        break;
      case SPLIT_INDEX: {
        LOG_DEBUG("SPLIT_INDEX id = %lu", n->id);
        n = n->next;
        }
        break;
      case REMOVE_INDEX: {
        LOG_DEBUG("REMOVE_INDEX id = %lu", n->id);
        n = n->next;
        }
        break;
    }
}

template class BWTree<IntsKey<1>, ItemPointer, IntsComparator<1>,
                      IntsEqualityChecker<1>>;
template class BWTree<IntsKey<2>, ItemPointer, IntsComparator<2>,
                      IntsEqualityChecker<2>>;
template class BWTree<IntsKey<3>, ItemPointer, IntsComparator<3>,
                      IntsEqualityChecker<3>>;
template class BWTree<IntsKey<4>, ItemPointer, IntsComparator<4>,
                      IntsEqualityChecker<4>>;

template class BWTree<GenericKey<4>, ItemPointer, GenericComparator<4>,
                      GenericEqualityChecker<4>>;
template class BWTree<GenericKey<8>, ItemPointer, GenericComparator<8>,
                      GenericEqualityChecker<8>>;
template class BWTree<GenericKey<12>, ItemPointer, GenericComparator<12>,
                      GenericEqualityChecker<12>>;
template class BWTree<GenericKey<16>, ItemPointer, GenericComparator<16>,
                      GenericEqualityChecker<16>>;
template class BWTree<GenericKey<24>, ItemPointer, GenericComparator<24>,
                      GenericEqualityChecker<24>>;
template class BWTree<GenericKey<32>, ItemPointer, GenericComparator<32>,
                      GenericEqualityChecker<32>>;
template class BWTree<GenericKey<48>, ItemPointer, GenericComparator<48>,
                      GenericEqualityChecker<48>>;
template class BWTree<GenericKey<64>, ItemPointer, GenericComparator<64>,
                      GenericEqualityChecker<64>>;
template class BWTree<GenericKey<96>, ItemPointer, GenericComparator<96>,
                      GenericEqualityChecker<96>>;
template class BWTree<GenericKey<128>, ItemPointer, GenericComparator<128>,
                      GenericEqualityChecker<128>>;
template class BWTree<GenericKey<256>, ItemPointer, GenericComparator<256>,
                      GenericEqualityChecker<256>>;
template class BWTree<GenericKey<512>, ItemPointer, GenericComparator<512>,
                      GenericEqualityChecker<512>>;

template class BWTree<TupleKey, ItemPointer, TupleKeyComparator,
                      TupleKeyEqualityChecker>;

// Add your function definitions here

}  // End index namespace
}  // End peloton namespace
