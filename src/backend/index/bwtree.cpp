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

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
Epoch<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Epoch(
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>& bwt,
    uint64_t id, uint64_t oldest)
    : generation(id), oldest_epoch(oldest), my_tree(bwt) {
  ref_count.store(0);
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void Epoch<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::join() {
  /*
   * We call join from every operation that needs to be performed on the tree.
   */
  ref_count.fetch_add(1);
  LOG_DEBUG("** joining epoch ref count %lu **", ref_count.load());
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void Epoch<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::performGc() {
  /*
   * We have to iterate through the to_be_cleaned list one-by-one and delete
   * the nodes. The destructors of those nodes will take care of cleaning up
   * the necessary internal malloc'd objects.
   */
  LOG_DEBUG("** garbage collecting epoch: cleaning %lu nodes **",
            to_be_cleaned.size());
  LOG_DEBUG("** memory usage before cleaning = %lu",
            this->my_tree.memory_usage);
  for (auto it = to_be_cleaned.begin(); it != to_be_cleaned.end(); it++) {
    this->my_tree.memory_usage -= sizeof(*(*it));
    delete *it;
  }
  to_be_cleaned.clear();
  assert(to_be_cleaned.size() == 0);
  LOG_DEBUG("** memory usage after cleaning = %lu", this->my_tree.memory_usage);
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
bool Epoch<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::leave() {
  ref_count.fetch_sub(1);
  LOG_DEBUG("** leaving epoch ref count = %lu **", ref_count.load());
  if (to_be_cleaned.size() >= my_tree.max_epoch_size) {
    LOG_DEBUG("** creating epoch %lu **", generation);
    auto new_e = new Epoch(this->my_tree, this->generation + 1,
                           this->my_tree.oldest_epoch);
    if (__sync_bool_compare_and_swap(&(my_tree.current_epoch), this, new_e) ==
        false) {
      delete new_e;
    } else {
      this->my_tree.memory_usage += sizeof(*new_e);
      LOG_DEBUG("** installed new epoch %lu **", new_e->generation);
    }
  }
  if (__sync_bool_compare_and_swap(&(my_tree.current_epoch), this, this) ==
      false) {
    /*
     * this means epoch changed before this thread reached leave.
     */
    if (ref_count == 0) {
      if (__sync_bool_compare_and_swap(&(my_tree.oldest_epoch),
                                       this->generation,
                                       this->generation + 1)) {
        /*
         * perform cleaning of this epoch, because we don't need it anymore.
         *
         * Here it is guaranteed that only one thread will come here, since it
         * is guarded by compare-and-swap on oldest epoch.
         */
        performGc();
        return true;  // this is for the upper layer to delete the epoch object
      }
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::BWTree(
    IndexMetadata* metadata, KeyComparator comparator,
    KeyEqualityChecker equals, ItemPointerEqualityChecker value_equals,
    bool allow_duplicates = true, uint32_t policy = 10)
    : metadata(metadata),
      comparator(comparator),
      equals(equals),
      value_equals(value_equals),
      allow_duplicates(allow_duplicates),
      policy(policy) {
  min_node_size = 2;
  max_node_size = 4;
  tree_height = 1;
  root = table.GetNextId();
  memory_usage = 0;
  LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* root_node =
      new LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
          this->metadata, *this, root);
  this->memory_usage += sizeof(*root_node);
  table.Install(root, root_node);
  LOG_DEBUG("Successfully created a tree of min_node_size %d, max_node_size %d",
            min_node_size, max_node_size);
  oldest_epoch = 1;
  current_epoch =
      new Epoch<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
          *this, oldest_epoch, oldest_epoch);
  this->memory_usage += sizeof(*current_epoch);
  max_epoch_size = 1;  // FIXME: change this to be less aggressive (say 64?)
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::~BWTree() {
  auto tw =
      new ThreadWrapper<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
          current_epoch);
  this->memory_usage += sizeof(*tw);
  tw->e->join();
  CleanupTreeRecursively(root, tw);
  if (tw->e->leave()) {
    this->memory_usage -= sizeof(*(tw->e));
    delete tw->e;
  }
  this->memory_usage -= sizeof(*tw);
  delete tw;

  if (current_epoch != nullptr) {
    current_epoch->performGc();
    this->memory_usage -= sizeof(*(current_epoch));
    delete current_epoch;
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
    CleanupTreeRecursively(uint64_t id,
                           ThreadWrapper<KeyType, ValueType, KeyComparator,
                                         KeyEqualityChecker>* tw) {
  LOG_DEBUG("Cleaning up node %lu", id);
  Consolidate(id, true, tw);
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node =
      this->table.Get(id);
  if (node == nullptr) {
    return;
  }
  switch (node->type) {
    case (INTERNAL_BW_NODE): {
      LOG_DEBUG("point 2");
      InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
          node_ = dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator,
                                              KeyEqualityChecker>*>(node);
      if (node_->leftmost_pointer) {
        CleanupTreeRecursively(node_->leftmost_pointer, tw);
      }
      auto it = node_->key_list.begin();
      for (; it != node_->key_list.end(); it++) {
        CleanupTreeRecursively(it->second, tw);
      }
    } break;
    default:
      break;
  }
  if (node) {
    delete node;
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
bool CASMappingTable<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
    Install(
        uint64_t id,
        Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_ptr) {
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* table_val =
      nullptr;
  if (!cas_mapping_table.find(id, table_val)) {
    cas_mapping_table.insert(id, node_ptr);
    return true;
  }

  while (true) {
    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* old_node =
        node_ptr->next;
    table_val = nullptr;
    uint64_t table_val_addr = 0;
    if (!cas_mapping_table.find(id, table_val, table_val_addr)) {
      assert(0);
    }
    if (__sync_bool_compare_and_swap((void**)table_val_addr, old_node,
                                     node_ptr)) {
      table_val = nullptr;
      if (!cas_mapping_table.find(id, table_val)) {
        assert(0);
      }
      if (table_val->type == INTERNAL_BW_NODE ||
          table_val->type == LEAF_BW_NODE) {
        table_val->chain_len = 0;
        table_val->next = NULL;
      }
      break;
    }
    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* cur_node =
        nullptr;
    if (!cas_mapping_table.find(id, cur_node)) {
      assert(0);
    }
    node_ptr->next = cur_node;
    node_ptr->chain_len = cur_node->chain_len + 1;
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* CASMappingTable<
    KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Get(uint64_t id) {
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* retval = nullptr;
  if (!cas_mapping_table.find(id, retval)) {
    assert(0);
  }
  return retval;
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
uint64_t CASMappingTable<KeyType, ValueType, KeyComparator,
                         KeyEqualityChecker>::GetNextId() {
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
    uint64_t id, bool force,
    ThreadWrapper<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* tw) {
  bool ret_val = true;
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_ =
      this->table.Get(id);
  // Not reaching the threshold
  if (node_->chain_len < this->policy && !force) {
    return true;
  }
  LOG_DEBUG(
      "\t\t\t(^_^)Consolidate is called on %lu due to %d, chain_len %u(^_^)",
      id, force, node_->chain_len);
  deque<Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*> stack;
  // Collect delta chains
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* temp = node_;

  bool encounter_split_delta = false;
  bool encounter_merge_delta = false;
  uint64_t merge_delta_id;
  KeyType split_key;
  while (temp->next != nullptr) {
    stack.push_back(temp);
    if (temp->type == SPLIT) {
      SplitDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
          split_delta =
              dynamic_cast<SplitDeltaNode<KeyType, ValueType, KeyComparator,
                                          KeyEqualityChecker>*>(temp);
      split_key = split_delta->split_key;
      encounter_split_delta = true;
    }
    temp = temp->next;
  }

  LOG_DEBUG("\t\t**** Consolidate temp id = %lu, type %d", temp->id,
            temp->type);
  if (temp->type == LEAF_BW_NODE) {
    LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* base =
        dynamic_cast<
            LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
            temp);
    LeafBWNode<KeyType, ValueType, KeyComparator,
               KeyEqualityChecker>* new_base =
        new LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
            this->metadata, *this, base->id);
    this->memory_usage += sizeof(*new_base);
    // This is a workaround, Install will set the next to NULL again by checking
    // the type
    // new_base->next = node_;
    new_base->next = nullptr;

    typename multimap<KeyType, ValueType, KeyComparator>::iterator base_it =
        base->kv_list.begin();
    for (; base_it != base->kv_list.end(); base_it++) {
      if (encounter_split_delta && !comparator(base_it->first, split_key)) {
        continue;
      } else {
        new_base->kv_list.insert(*base_it);
      }
    }
    bool gc_new_base = false;
    while (!stack.empty()) {
      temp = stack.back();
      stack.pop_back();
      if (temp->type == INSERT) {
        DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            insert_delta =
                dynamic_cast<DeltaNode<KeyType, ValueType, KeyComparator,
                                       KeyEqualityChecker>*>(temp);
        new_base->kv_list.insert(
            pair<KeyType, ValueType>(insert_delta->key, insert_delta->value));
        tw->e->to_be_cleaned.push_back(temp);
      } else if (temp->type == DELETE) {
        DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            delete_delta =
                dynamic_cast<DeltaNode<KeyType, ValueType, KeyComparator,
                                       KeyEqualityChecker>*>(temp);
        pair<typename multimap<KeyType, ValueType>::iterator,
             typename multimap<KeyType, ValueType>::iterator> values =
            new_base->kv_list.equal_range(delete_delta->key);
        typename multimap<KeyType, ValueType, KeyEqualityChecker>::iterator it =
            values.first;
        for (; it != values.second;) {
          if (value_equals(it->second, delete_delta->value))
            it = new_base->kv_list.erase(it);
          else
            it++;
        }
        tw->e->to_be_cleaned.push_back(temp);
      } else if (temp->type == SPLIT) {
        LOG_DEBUG("Bypass the split delta");
        tw->e->to_be_cleaned.push_back(temp);
      } else if (temp->type == REMOVE) {
        // This node will be removed, the node it's merging to does GC
        // gc_new_base = true;
        // tw->e->to_be_cleaned.push_back(temp);
      } else if (temp->type == MERGE) {
        encounter_merge_delta = true;
        MergeDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            merge_pointer =
                dynamic_cast<MergeDeltaNode<KeyType, ValueType, KeyComparator,
                                            KeyEqualityChecker>*>(temp);

        merge_delta_id = merge_pointer->id;
        LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            merged_node_pointer =
                dynamic_cast<LeafBWNode<KeyType, ValueType, KeyComparator,
                                        KeyEqualityChecker>*>(
                    merge_pointer->node_to_be_merged);
        typename multimap<KeyType, ValueType, KeyComparator>::iterator iter =
            merged_node_pointer->kv_list.begin();
        for (; iter != merged_node_pointer->kv_list.end(); iter++) {
          new_base->kv_list.insert(*iter);
        }
        if (merged_node_pointer->left_sibling == id) {
          new_base->right_sibling = merged_node_pointer->right_sibling;
          uint64_t right_sibling_node_id = merged_node_pointer->right_sibling;

          if (right_sibling_node_id) {
            Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
                right_sibling_temp = this->table.Get(right_sibling_node_id);

            while (right_sibling_temp->next != nullptr) {
              right_sibling_temp = right_sibling_temp->next;
            }
            LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
                right_sibling_pointer =
                    dynamic_cast<LeafBWNode<KeyType, ValueType, KeyComparator,
                                            KeyEqualityChecker>*>(
                        right_sibling_temp);
            right_sibling_pointer->left_sibling = id;
          }
        } else if (merged_node_pointer->right_sibling == id) {
          new_base->left_sibling = merged_node_pointer->left_sibling;
          uint64_t left_sibling_node_id = merged_node_pointer->left_sibling;
          if (left_sibling_node_id) {
            Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
                left_sibling_temp = this->table.Get(left_sibling_node_id);

            while (left_sibling_temp->next != nullptr) {
              left_sibling_temp = left_sibling_temp->next;
            }
            LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
                left_sibling_pointer =
                    dynamic_cast<LeafBWNode<KeyType, ValueType, KeyComparator,
                                            KeyEqualityChecker>*>(
                        left_sibling_temp);
            left_sibling_pointer->right_sibling = id;
          }
        }

        // this->freelist.insert(merge_pointer->node_to_be_merged);
        tw->e->to_be_cleaned.push_back(merge_pointer->node_to_be_merged);
        // tw->e->to_be_cleaned.push_back(temp);
      } else {
        assert(false);
      }
    }
    if (!(encounter_merge_delta && base->left_sibling == merge_delta_id))
      new_base->left_sibling = base->left_sibling;
    if (!(encounter_merge_delta && base->right_sibling == merge_delta_id))
      new_base->right_sibling = base->right_sibling;
    if (!gc_new_base) {
      ret_val = this->table.Install(base->id, new_base);
      // LOG_DEBUG("New base size is %ld", new_base->kv_list.size());
    } else {
      // this->freelist.insert(new_base);
      tw->e->to_be_cleaned.push_back(new_base);
    }
    // this->freelist.insert(base);
    tw->e->to_be_cleaned.push_back(base);
    return ret_val;
  } else {
    InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
        base = dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator,
                                           KeyEqualityChecker>*>(temp);

    LOG_DEBUG("Consolidating the internal node with id %ld", id);
    LOG_DEBUG("########## new internal node id = %lu", id);
    InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
        new_base =
            new InternalBWNode<KeyType, ValueType, KeyComparator,
                               KeyEqualityChecker>(this->metadata, *this, id);
    this->memory_usage += sizeof(*new_base);
    typename multimap<KeyType, uint64_t, KeyComparator>::iterator iter =
        base->key_list.begin();
    for (; iter != base->key_list.end(); iter++) {
      if (encounter_split_delta && !comparator(iter->first, split_key))
        continue;
      new_base->key_list.insert(*iter);
    }
    new_base->leftmost_pointer = base->leftmost_pointer;
    LOG_DEBUG("Set the leftmost pointer of %ld to %ld", new_base->id,
              new_base->leftmost_pointer);
    new_base->next = node_;

    bool gc_new_base = false;
    while (!stack.empty()) {
      temp = stack.back();
      switch (temp->type) {
        case (SPLIT_INDEX): {
          SplitIndexDeltaNode<KeyType, ValueType, KeyComparator,
                              KeyEqualityChecker>* split_pointer =
              dynamic_cast<SplitIndexDeltaNode<
                  KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
                  temp);
          LOG_DEBUG("Adding key to the new base for the internal node");
          new_base->key_list.insert(pair<KeyType, uint64_t>(
              split_pointer->split_key, split_pointer->new_split_node_id));
          break;
        }
        case (UPDATE): {
          UpdateDeltaNode<KeyType, ValueType, KeyComparator,
                          KeyEqualityChecker>* update_pointer =
              dynamic_cast<UpdateDeltaNode<KeyType, ValueType, KeyComparator,
                                           KeyEqualityChecker>*>(temp);
          typename multimap<KeyType, uint64_t, KeyEqualityChecker>::iterator
              iter = new_base->key_list.begin();
          for (; iter != new_base->key_list.end();) {
            if (this->equals(iter->first, update_pointer->old_key)) {
              uint64_t pointer_val = iter->second;
              iter = new_base->key_list.erase(iter);
              new_base->key_list.insert(pair<KeyType, uint64_t>(
                  update_pointer->new_key, pointer_val));
              // relying on the fact that key, pointer pairs are unique
              break;
            } else
              iter++;
          }

          break;
        }
        case (REMOVE_INDEX): {
          RemoveIndexDeltaNode<KeyType, ValueType, KeyComparator,
                               KeyEqualityChecker>* remove_pointer =
              dynamic_cast<RemoveIndexDeltaNode<
                  KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
                  temp);
          // new_base->key_list.erase(remove_pointer->deleted_key);
          pair<typename multimap<KeyType, uint64_t>::iterator,
               typename multimap<KeyType, uint64_t>::iterator> values =
              new_base->key_list.equal_range(remove_pointer->deleted_key);
          typename multimap<KeyType, uint64_t, KeyEqualityChecker>::iterator
              it = values.first;
          for (; it != values.second;) {
            LOG_DEBUG("This might be strange %d",
                      this->equals(it->first, remove_pointer->deleted_key));
            LOG_DEBUG("Erasing a value while consolidating an internal node");
            if (equals(it->first, new_base->key_list.begin()->first)) {
              LOG_DEBUG("Also setting its leftmost pointer");
              new_base->leftmost_pointer = new_base->key_list.begin()->second;
            }
            it = new_base->key_list.erase(it);
          }
          break;
        }
        case (SPLIT):
          break;
        case (REMOVE):
          // This node will be removed, the node it's merging to does GC
          // gc_new_base = true;
          break;
        case (MERGE): {
          encounter_merge_delta = true;
          MergeDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
              merge_pointer =
                  dynamic_cast<MergeDeltaNode<KeyType, ValueType, KeyComparator,
                                              KeyEqualityChecker>*>(temp);

          merge_delta_id = merge_pointer->id;
          InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
              merged_node_pointer =
                  dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator,
                                              KeyEqualityChecker>*>(
                      merge_pointer->node_to_be_merged);
          iter = merged_node_pointer->key_list.begin();
          for (; iter != merged_node_pointer->key_list.end(); iter++) {
            new_base->key_list.insert(*iter);
          }
          if (merged_node_pointer->left_sibling == id) {
            new_base->right_sibling = merged_node_pointer->right_sibling;
            uint64_t right_sibling_node_id = merged_node_pointer->right_sibling;

            if (right_sibling_node_id) {
              Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
                  right_sibling_temp = this->table.Get(right_sibling_node_id);

              while (right_sibling_temp->next != nullptr) {
                right_sibling_temp = right_sibling_temp->next;
              }
              InternalBWNode<KeyType, ValueType, KeyComparator,
                             KeyEqualityChecker>* right_sibling_pointer =
                  dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator,
                                              KeyEqualityChecker>*>(
                      right_sibling_temp);
              right_sibling_pointer->left_sibling = id;
            }
          } else if (merged_node_pointer->right_sibling == id) {
            new_base->left_sibling = merged_node_pointer->left_sibling;
            uint64_t left_sibling_node_id = merged_node_pointer->left_sibling;
            if (left_sibling_node_id) {
              Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
                  left_sibling_temp = this->table.Get(left_sibling_node_id);

              while (left_sibling_temp->next != nullptr) {
                left_sibling_temp = left_sibling_temp->next;
              }
              InternalBWNode<KeyType, ValueType, KeyComparator,
                             KeyEqualityChecker>* left_sibling_pointer =
                  dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator,
                                              KeyEqualityChecker>*>(
                      left_sibling_temp);
              left_sibling_pointer->right_sibling = id;
            }
            new_base->leftmost_pointer = merged_node_pointer->leftmost_pointer;
          }

          // this->freelist.insert(merge_pointer->node_to_be_merged);
          tw->e->to_be_cleaned.push_back(merge_pointer->node_to_be_merged);
        }
      }
      // this->freelist.insert(temp);
      tw->e->to_be_cleaned.push_back(temp);
      stack.pop_back();
    }
    if (!(encounter_merge_delta && base->left_sibling == merge_delta_id))
      new_base->left_sibling = base->left_sibling;
    if (!(encounter_merge_delta && base->right_sibling == merge_delta_id))
      new_base->right_sibling = base->right_sibling;
    if (!gc_new_base) {
      LOG_DEBUG("######### installing id %lu", id);
      ret_val = this->table.Install(id, new_base);
    } else {
      // this->freelist.insert(new_base);
      tw->e->to_be_cleaned.push_back(new_base);
    }
    // this->freelist.insert(base);
    tw->e->to_be_cleaned.push_back(base);
    return ret_val;
  }

  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::SplitRoot(
    KeyType split_key, uint64_t left_pointer, uint64_t right_pointer) {
  uint64_t new_root_id = this->table.GetNextId();
  uint64_t old_root_id = root;
  InternalBWNode<KeyType, ValueType, KeyComparator,
                 KeyEqualityChecker>* internal_pointer =
      new InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
          this->metadata, *this, new_root_id);
  this->memory_usage += sizeof(*internal_pointer);
  internal_pointer->leftmost_pointer = left_pointer;
  internal_pointer->key_list.insert(
      pair<KeyType, uint64_t>(split_key, right_pointer));
  internal_pointer->left_sibling = 0;
  internal_pointer->right_sibling = 0;
  internal_pointer->chain_len = 0;
  table.Install(new_root_id, internal_pointer);
  LOG_DEBUG("Installed the new root");
  // TODO: Need to handle race conditions here
  tree_height++;
  bool ret_val = __sync_bool_compare_and_swap(&root, old_root_id, new_root_id);
  LOG_DEBUG("CAS on the tree root retunred %d", ret_val);
  return ret_val;
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
vector<ValueType> BWTree<KeyType, ValueType, KeyComparator,
                         KeyEqualityChecker>::SearchKeyWrapper(KeyType key) {
  auto tw =
      new ThreadWrapper<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
          current_epoch);
  this->memory_usage += sizeof(*tw);
  // auto e = current_epoch;
  tw->e->join();
  vector<ItemPointer> result = SearchKey(key, tw);
  if (tw->e->leave()) {
    this->memory_usage -= sizeof(*(tw->e));
    delete tw->e;
  }
  this->memory_usage -= sizeof(*tw);
  delete tw;
  return result;
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
vector<ValueType>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::SearchKey(
    KeyType key,
    ThreadWrapper<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* tw) {
  vector<ValueType> ret_vector;
  uint64_t* path = (uint64_t*)malloc(tree_height * sizeof(uint64_t));
  uint64_t location;
  uint64_t node_id = Search(key, path, location, tw);
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer =
      table.Get(node_id);

  multimap<KeyType, ValueType, KeyComparator> deleted_keys(
      KeyComparator(this->metadata));
  while (node_pointer) {
    switch (node_pointer->type) {
      case (INSERT): {
        DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            simple_pointer =
                dynamic_cast<DeltaNode<KeyType, ValueType, KeyComparator,
                                       KeyEqualityChecker>*>(node_pointer);

        if (equals(key, simple_pointer->key)) {
          bool push = true;
          pair<typename multimap<KeyType, ValueType>::iterator,
               typename multimap<KeyType, ValueType>::iterator> values =
              deleted_keys.equal_range(simple_pointer->key);
          typename multimap<KeyType, ValueType, KeyComparator>::iterator iter;
          for (iter = values.first; iter != values.second; iter++) {
            if (value_equals(iter->second, simple_pointer->value)) {
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
      case (REMOVE): {
        // LOG_DEBUG("Remove node from search_key = %p", node_pointer);
        // assert(node_pointer->next == nullptr);
        // node_pointer = node_pointer->next;
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
            if (value_equals(deleted_iter->second, iter->second)) {
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
bool BWTree<KeyType, ValueType, KeyComparator,
            KeyEqualityChecker>::InsertWrapper(KeyType key,
                                               ValueType location) {
  auto tw =
      new ThreadWrapper<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
          current_epoch);
  this->memory_usage += sizeof(*tw);
  // auto e = current_epoch;
  tw->e->join();
  auto retval = Insert(key, location, tw);
  if (tw->e->leave()) {
    this->memory_usage -= sizeof(*(tw->e));
    delete tw->e;
  }
  this->memory_usage -= sizeof(*tw);
  delete tw;
  return retval;
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Insert(
    KeyType key, ValueType value,
    ThreadWrapper<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* tw) {
  uint64_t* path = (uint64_t*)malloc(sizeof(uint64_t) * tree_height);
  uint64_t location;
  uint64_t node_id = Search(key, path, location, tw);

  LOG_DEBUG("Insert node id %ld", node_id);

  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer =
      table.Get(node_id);
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* cur_pointer =
      node_pointer;

  vector<ValueType> values_for_key = SearchKey(key, tw);
  typename vector<ValueType>::iterator i;

  if (!allow_duplicates && values_for_key.size() != 0) {
    free(path);
    return false;
  }

  /*
  bool duplicate_found = false;
  for(i=values_for_key.begin();i!=values_for_key.end();i++)
  {
    if(value_equals(*i, value))
    {
      duplicate_found = true;
      break;
    }
  }



  if(duplicate_found)
    return false;
  */
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
      seen_remove_delta_node = nullptr;

  while (cur_pointer->next) {
    if (cur_pointer->type == REMOVE) {
      LOG_DEBUG("I've seen a remove delta");
      seen_remove_delta_node = cur_pointer;
    }
    cur_pointer = cur_pointer->next;
  }

  if (seen_remove_delta_node != nullptr) {
    RemoveDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
        seen_remove_delta =
            dynamic_cast<RemoveDeltaNode<KeyType, ValueType, KeyComparator,
                                         KeyEqualityChecker>*>(
                seen_remove_delta_node);
    LOG_DEBUG("Redriect the insert to my neighbor");
    uint64_t neighbor_id = seen_remove_delta->merged_to_id;
    uint64_t neighbor_node_size = Get_size(neighbor_id);
    if (neighbor_node_size == max_node_size - 1) {
      // Tricky case, we consolidate because the node is about to overflow
      LOG_DEBUG("Finish the consolidation");
      Consolidate(neighbor_id, true, tw);
    }
    // Since there could be consolidating happening, seen_remove_delta is no
    // longer accessible!!!
    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* target =
        this->table.Get(neighbor_id);
    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* temptemp =
        target;
    while (temptemp->next != nullptr) {
      temptemp = temptemp->next;
    }
    LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
        leaf_pointer = dynamic_cast<
            LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
            temptemp);
    LOG_DEBUG("Leaf insert on my merged neighbor");
    free(path);
    auto retval = leaf_pointer->LeafInsert(key, value);
    auto root_node = table.Get(root);
    Traverse(root_node);
    return retval;
  }

  LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
      leaf_pointer = dynamic_cast<
          LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
          cur_pointer);

  uint64_t cur_node_size = Get_size(node_id);
  if (cur_node_size < max_node_size) {
    free(path);
    LOG_DEBUG("Leaf Insert");
    auto retval = leaf_pointer->LeafInsert(key, value);
    auto root_node = table.Get(root);
    Traverse(root_node);
    return retval;
  } else {
    LOG_DEBUG("Leaf Split");
    auto retval = leaf_pointer->LeafSplit(path, location, key, value, tw);
    auto root_node = table.Get(root);
    Traverse(root_node);
    return retval;
  }
  assert(false);
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator,
            KeyEqualityChecker>::DeleteWrapper(KeyType key,
                                               ValueType location) {
  auto tw =
      new ThreadWrapper<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
          current_epoch);
  this->memory_usage += sizeof(*tw);
  // auto e = current_epoch;
  tw->e->join();
  bool ret_val = Delete(key, location, tw);
  if (tw->e->leave()) {
    this->memory_usage -= sizeof(*(tw->e));
    delete tw->e;
  }
  this->memory_usage -= sizeof(*tw);
  delete tw;
  return ret_val;
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Delete(
    KeyType key, ValueType value,
    ThreadWrapper<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* tw) {
  uint64_t* path = (uint64_t*)malloc(sizeof(uint64_t) * tree_height);
  uint64_t location;
  uint64_t node_id = Search(key, path, location, tw);

  LOG_DEBUG("Delete on node id %ld", node_id);
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer =
      table.Get(node_id);
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* cur_pointer =
      node_pointer;

  vector<ValueType> values_for_key = SearchKey(key, tw);
  typename vector<ValueType>::iterator i;
  bool delete_possible = false;
  for (i = values_for_key.begin(); i != values_for_key.end(); i++) {
    if (value_equals(*i, value)) {
      delete_possible = true;
      break;
    }
  }

  if (!delete_possible) {
    LOG_DEBUG("Key Value pair does not exist");
    free(path);
    return false;
  }
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
      seen_remove_delta_node = nullptr;
  while (cur_pointer->next) {
    if (cur_pointer->type == REMOVE) {
      LOG_DEBUG("I've seen a remove delta");
      seen_remove_delta_node = cur_pointer;
    }
    cur_pointer = cur_pointer->next;
  }
  if (seen_remove_delta_node != nullptr) {
    RemoveDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
        seen_remove_delta =
            dynamic_cast<RemoveDeltaNode<KeyType, ValueType, KeyComparator,
                                         KeyEqualityChecker>*>(
                seen_remove_delta_node);
    LOG_DEBUG("Redriect the delete to my neighbor");
    uint64_t neighbor_id = seen_remove_delta->merged_to_id;
    uint64_t neighbor_node_size = Get_size(neighbor_id);
    if (neighbor_node_size == min_node_size + 1) {
      // Tricky case, we consolidate because the node is about to underflow
      LOG_DEBUG("Finish the consolidation");
      Consolidate(neighbor_id, true, tw);
    }
    // Since there could be consolidating happening, seen_remove_delta is no
    // longer accessible!!!
    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* target =
        this->table.Get(neighbor_id);
    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* temptemp =
        target;
    while (temptemp->next != nullptr) {
      temptemp = temptemp->next;
    }
    LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
        leaf_pointer = dynamic_cast<
            LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
            temptemp);
    LOG_DEBUG("Leaf delete on my merged neighbor");
    free(path);
    auto retval = leaf_pointer->LeafDelete(key, value);
    auto root_node = table.Get(root);
    Traverse(root_node);
    return retval;
  }

  LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
      leaf_pointer = dynamic_cast<
          LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
          cur_pointer);
  uint64_t cur_node_size = Get_size(node_id);
  LOG_DEBUG("Cur node size of %ld is %ld", node_id, cur_node_size);
  if (cur_node_size > min_node_size) {
    LOG_DEBUG("Leaf delete");
    free(path);
    auto retval = leaf_pointer->LeafDelete(key, value);
    auto root_node = table.Get(root);
    Traverse(root_node);
    return retval;
  } else {
    LOG_DEBUG("Leaf merge");
    auto retval = leaf_pointer->LeafMerge(path, location, key, value, tw);
    free(path);
    auto root_node = table.Get(root);
    Traverse(root_node);
    return retval;
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
uint64_t BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Search(
    KeyType key, uint64_t* path, uint64_t& location,
    ThreadWrapper<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* tw) {
  uint64_t cur_id = root;
  // TODO: why prev_id not used?
  uint64_t prev_id = cur_id;
  bool stop = false;
  bool try_consolidation = true;
  prev_id = prev_id;  // TODO: this is a work around for not being used
  int merge_dir = -2;
  bool need_redirection = false;
  // multimap<KeyType, ValueType, KeyComparator>
  // deleted_keys(KeyComparator(this->metadata));
  set<KeyType, KeyComparator> deleted_indexes(KeyComparator(this->metadata));
  vector<pair<KeyType, KeyType>> updated_keys;
  uint64_t index = -1;
  location = -1;
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer =
      nullptr;
  while (!stop) {
    if (try_consolidation) {
      Consolidate(cur_id, false, tw);
      node_pointer = table.Get(cur_id);
      LOG_DEBUG("Search is looking at node id %ld with top type %d", cur_id,
                node_pointer->type);
      // deleted_keys.clear();
      deleted_indexes.clear();
      index++;
      location++;
      path[index] = cur_id;
    }
    assert(node_pointer != nullptr);
    switch (node_pointer->type) {
      case (LEAF_BW_NODE): {
        LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            leaf_pointer =
                dynamic_cast<LeafBWNode<KeyType, ValueType, KeyComparator,
                                        KeyEqualityChecker>*>(node_pointer);
        if (!need_redirection) {
          return leaf_pointer->id;
        } else {
          // The search to R is thus redirected
          if (merge_dir == LEFT) {
            return leaf_pointer->right_sibling;
          } else if (merge_dir == RIGHT) {
            return leaf_pointer->left_sibling;
          } else
            assert(false);
        }
        break;
      }
      case (UPDATE): {
        UpdateDeltaNode<KeyType, ValueType, KeyComparator,
                        KeyEqualityChecker>* update_pointer =
            dynamic_cast<UpdateDeltaNode<KeyType, ValueType, KeyComparator,
                                         KeyEqualityChecker>*>(node_pointer);
        updated_keys.push_back(pair<KeyType, KeyType>(update_pointer->old_key,
                                                      update_pointer->new_key));
        node_pointer = node_pointer->next;
        LOG_DEBUG("Hello");
        try_consolidation = false;
        break;
      }
      case (INTERNAL_BW_NODE): {
        InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            internal_pointer =
                dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator,
                                            KeyEqualityChecker>*>(node_pointer);
        prev_id = cur_id;
        cur_id = internal_pointer->GetChildId(key, updated_keys);
        LOG_DEBUG("Internal pointer with id %ld returned child id as %ld",
                  internal_pointer->id, cur_id);
        try_consolidation = true;
        break;
      }
      case (INSERT): {
        DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            simple_pointer =
                dynamic_cast<DeltaNode<KeyType, ValueType, KeyComparator,
                                       KeyEqualityChecker>*>(node_pointer);

        KeyType cur_key = simple_pointer->key;
        typename vector<pair<KeyType, KeyType>>::reverse_iterator
            updated_keys_iter = updated_keys.rbegin();
        for (; updated_keys_iter != updated_keys.rend(); updated_keys_iter++) {
          if (this->equals(cur_key, updated_keys_iter->first)) {
            cur_key = updated_keys_iter->second;
          }
        }
        if (equals(key, cur_key)) return simple_pointer->id;
        node_pointer = simple_pointer->next;
        try_consolidation = false;
        break;
      }
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
            simple_pointer =
                dynamic_cast<DeltaNode<KeyType, ValueType, KeyComparator,
                                       KeyEqualityChecker>*>(node_pointer);
        if (equals(key, simple_pointer->key)) return simple_pointer->id;
        // deleted_keys.push_back(key);
        node_pointer = simple_pointer->next;
        try_consolidation = false;
        break;
      }
      case (SPLIT): {
        SplitDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            split_pointer =
                dynamic_cast<SplitDeltaNode<KeyType, ValueType, KeyComparator,
                                            KeyEqualityChecker>*>(node_pointer);
        KeyType cur_key = split_pointer->split_key;
        typename vector<pair<KeyType, KeyType>>::reverse_iterator
            updated_keys_iter = updated_keys.rbegin();
        for (; updated_keys_iter != updated_keys.rend(); updated_keys_iter++) {
          if (this->equals(cur_key, updated_keys_iter->first)) {
            cur_key = updated_keys_iter->second;
          }
        }
        if (comparator(key, cur_key)) {
          node_pointer = split_pointer->next;
          try_consolidation = false;
        } else {
          prev_id = cur_id;
          cur_id = split_pointer->target_node_id;
          try_consolidation = true;
        }
        break;
      }
      case (MERGE): {
        MergeDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            merge_pointer =
                dynamic_cast<MergeDeltaNode<KeyType, ValueType, KeyComparator,
                                            KeyEqualityChecker>*>(node_pointer);
        if (merge_pointer->node_to_be_merged->type == INTERNAL_BW_NODE) {
          InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
              merged_node =
                  dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator,
                                              KeyEqualityChecker>*>(
                      merge_pointer->node_to_be_merged);
          KeyType cur_key = merge_pointer->merge_key;
          typename vector<pair<KeyType, KeyType>>::reverse_iterator
              updated_keys_iter = updated_keys.rbegin();
          for (; updated_keys_iter != updated_keys.rend();
               updated_keys_iter++) {
            if (this->equals(cur_key, updated_keys_iter->first)) {
              cur_key = updated_keys_iter->second;
            }
          }
          if (merged_node->left_sibling == node_pointer->id) {
            if (comparator(key, cur_key)) {
              node_pointer = merge_pointer->next;
            } else {
              node_pointer = merge_pointer->node_to_be_merged;
            }
          } else if (merged_node->right_sibling == node_pointer->id) {
            if (!comparator(cur_key, key))
              node_pointer = merge_pointer->node_to_be_merged;
            else
              node_pointer = merge_pointer->next;
          } else
            assert(false);
        }
        if (merge_pointer->node_to_be_merged->type == LEAF_BW_NODE) {
          LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
              merged_node =
                  dynamic_cast<LeafBWNode<KeyType, ValueType, KeyComparator,
                                          KeyEqualityChecker>*>(
                      merge_pointer->node_to_be_merged);
          if (merged_node->left_sibling == node_pointer->id) {
            if (comparator(key, merge_pointer->merge_key)) {
              node_pointer = merge_pointer->next;
            } else {
              node_pointer = merge_pointer->node_to_be_merged;
            }
          } else if (merged_node->right_sibling == node_pointer->id) {
            if (!comparator(merge_pointer->merge_key, key))
              node_pointer = merge_pointer->node_to_be_merged;
            else
              node_pointer = merge_pointer->next;
          } else
            assert(false);
        }

        try_consolidation = false;
        break;
      }
      case (REMOVE): {
        // TODO: This is incorrect. If we hit a remove delta node, we should not
        // proceed
        // The previous solution of going back to parent and waiting for the
        // merge thread seems reasonable to me.
        // The correct implementation of course is to complete the SMO, but this
        // is not doing that either
        // It just returns the id of the node which has a REMOVE in its delta
        // chain
        // When this will be used further ahead in operations, it would create a
        // problem
        RemoveDeltaNode<KeyType, ValueType, KeyComparator,
                        KeyEqualityChecker>* remove_pointer =
            dynamic_cast<RemoveDeltaNode<KeyType, ValueType, KeyComparator,
                                         KeyEqualityChecker>*>(node_pointer);
        merge_dir = remove_pointer->direction;
        if (merge_dir == UP) {
          // I will let you access it anyway
          node_pointer = remove_pointer->next;
          try_consolidation = false;
        } else {
          prev_id = cur_id;
          cur_id = remove_pointer->merged_to_id;
          need_redirection = true;
          try_consolidation = true;
        }
        break;
      }
      // The following has been resolved in another manner
      // Should try to complete the SMO instead of going to the parent and
      // waiting for the merge thread to complete it
      case (SPLIT_INDEX): {
        SplitIndexDeltaNode<KeyType, ValueType, KeyComparator,
                            KeyEqualityChecker>* split_index_pointer =
            dynamic_cast<SplitIndexDeltaNode<KeyType, ValueType, KeyComparator,
                                             KeyEqualityChecker>*>(
                node_pointer);
        KeyType cur_split_key = split_index_pointer->split_key;
        KeyType cur_boundary_key = split_index_pointer->boundary_key;
        typename vector<pair<KeyType, KeyType>>::reverse_iterator
            updated_keys_iter = updated_keys.rbegin();
        for (; updated_keys_iter != updated_keys.rend(); updated_keys_iter++) {
          if (this->equals(cur_split_key, updated_keys_iter->first)) {
            cur_split_key = updated_keys_iter->second;
          }
          if (this->equals(cur_boundary_key, updated_keys_iter->first)) {
            cur_boundary_key = updated_keys_iter->second;
          }
        }
        if (deleted_indexes.find(cur_split_key) == deleted_indexes.end()) {
          if (comparator(key, cur_split_key) ||
              (!comparator(key, cur_boundary_key) &&
               equals(key, cur_boundary_key))) {
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
        break;
      }
      case (REMOVE_INDEX): {
        RemoveIndexDeltaNode<KeyType, ValueType, KeyComparator,
                             KeyEqualityChecker>* delete_index_pointer =
            dynamic_cast<RemoveIndexDeltaNode<KeyType, ValueType, KeyComparator,
                                              KeyEqualityChecker>*>(
                node_pointer);
        deleted_indexes.insert(delete_index_pointer->deleted_key);
        node_pointer = delete_index_pointer->next;
        try_consolidation = false;
        node_pointer = delete_index_pointer->next;
        break;
      }
    }
  }
  assert(false);
  return 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
bool LeafBWNode<KeyType, ValueType, KeyComparator,
                KeyEqualityChecker>::LeafInsert(KeyType key, ValueType value) {
  DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* delta =
      new DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
          this->my_tree, this->id, INSERT);
  this->my_tree.memory_usage += sizeof(*delta);
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
                KeyEqualityChecker>::LeafDelete(KeyType key, ValueType value) {
  DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* delta =
      new DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
          this->my_tree, this->id, DELETE);
  this->my_tree.memory_usage += sizeof(*delta);
  delta->key = key;
  delta->value = value;
  delta->type = DELETE;
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_ =
      this->my_tree.table.Get(this->id);
  delta->next = node_;
  uint32_t chain_len = node_->chain_len;
  delta->chain_len = chain_len + 1;
  bool ret_val = this->my_tree.table.Install(this->id, delta);
  return ret_val;
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
bool LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
    LeafSplit(uint64_t* path, uint64_t index, KeyType key, ValueType value,
               ThreadWrapper<KeyType, ValueType, KeyComparator,
                             KeyEqualityChecker>* tw) {
  this->LeafInsert(key, value);
  bool result = this->my_tree.Consolidate(this->id, true, tw);
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* self_ =
      this->my_tree.table.Get(this->id);

  LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* self_node =
      dynamic_cast<
          LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
          self_);
  if (!result) {
    return result;
  }
  uint64_t new_node_id = this->my_tree.table.GetNextId();
  // This is Q
  LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
      new_leaf_node =
          new LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
              this->my_tree.metadata, this->my_tree, new_node_id);
  this->my_tree.memory_usage += sizeof(*new_leaf_node);
  new_leaf_node->left_sibling = this->id;
  new_leaf_node->right_sibling = this->right_sibling;
  LOG_DEBUG("Set the right sibling of %ld to %ld", new_node_id,
            new_leaf_node->right_sibling);
  if (self_node->right_sibling != 0) {
    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
        right_sibling_node = this->my_tree.table.Get(self_node->right_sibling);
    while (right_sibling_node->next)
      right_sibling_node = right_sibling_node->next;

    LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
        right_sibling_leaf_node = dynamic_cast<
            LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
            right_sibling_node);
    right_sibling_leaf_node->left_sibling = new_node_id;
    LOG_DEBUG("Set the left sibling of %ld to %ld", right_sibling_leaf_node->id,
              new_node_id);
  }
  self_node->right_sibling = new_node_id;
  LOG_DEBUG("Set the right sibling of %ld to %ld", self_node->id, new_node_id);

  uint64_t count = self_node->kv_list.size();
  typename multimap<KeyType, ValueType, KeyComparator>::iterator key_it =
      self_node->kv_list.begin();
  advance(key_it, count / 2);
  KeyType split_key = key_it->first;
  key_it = self_node->kv_list.end();
  KeyType boundary_key = key_it->first;

  typename multimap<KeyType, ValueType, KeyComparator>::iterator
      split_iterator = self_node->kv_list.begin();
  advance(split_iterator, count / 2);

  for (; split_iterator != self_node->kv_list.end(); split_iterator++) {
    pair<KeyType, ValueType> new_entry = *split_iterator;
    new_leaf_node->kv_list.insert(new_entry);
  }

  /*
  if (this->my_tree.comparator(key, split_key)) {
    self_node->LeafInsert(key, value);  // Prevent race condition
  } else {
    // This is ok since new_leaf_node is just created
    new_leaf_node->kv_list.insert(pair<KeyType, ValueType>(key, value));
  }
  */

  // Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_ =
  //     (this->my_tree).table.Get(this->id);
  SplitDeltaNode<KeyType, ValueType, KeyComparator,
                 KeyEqualityChecker>* split_node =
      new SplitDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
          this->my_tree, this->id);
  this->my_tree.memory_usage += sizeof(*split_node);
  split_node->next = self_node;
  split_node->target_node_id = new_node_id;
  split_node->split_key = split_key;

  uint32_t chain_len = self_node->chain_len;
  bool ret_val = true;
  ret_val = this->my_tree.table.Install(new_node_id, new_leaf_node);
  if (!ret_val) return false;
  split_node->chain_len = chain_len + 1;
  ret_val = this->my_tree.table.Install(this->id, split_node);
  if (!ret_val) return false;

  LOG_DEBUG("This split node now has size %ld and the new node has size %ld",
            this->my_tree.Get_size(this->id),
            this->my_tree.Get_size(new_node_id));

  if (index > 0) {
    uint64_t parent_id = path[index - 1];
    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* parent_node_ =
        this->my_tree.table.Get(parent_id);
    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* cur_pointer =
        parent_node_;
    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
        seen_remove_delta_node = nullptr;

    while (cur_pointer->next) {
      if (cur_pointer->type == REMOVE) {
        LOG_DEBUG("I've seen a remove delta in LeafSplit of my parent");
        seen_remove_delta_node = cur_pointer;
      }
      cur_pointer = cur_pointer->next;
    }
    InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
        internal_pointer =
            dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator,
                                        KeyEqualityChecker>*>(cur_pointer);

    uint64_t parent_size = this->my_tree.Get_size(parent_id);
    if (parent_size + 1 < this->my_tree.max_node_size) {
      LOG_DEBUG("Inserting in parent");
      if (seen_remove_delta_node != nullptr) {
        RemoveDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            seen_remove_delta =
                dynamic_cast<RemoveDeltaNode<KeyType, ValueType, KeyComparator,
                                             KeyEqualityChecker>*>(
                    seen_remove_delta_node);
        LOG_DEBUG("Redriect the cascading split to parent's neighbor");
        uint64_t neighbor_id = seen_remove_delta->merged_to_id;
        uint64_t neighbor_node_size = this->my_tree.Get_size(neighbor_id);
        if (neighbor_node_size == this->my_tree.max_node_size - 1) {
          // Tricky case, we consolidate because the node is about to underflow
          LOG_DEBUG("Finish the consolidation");
          this->my_tree.Consolidate(neighbor_id, true, tw);
        }
        // Since there could be consolidating happening, seen_remove_delta is no
        // longer accessible!!!
        Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* target =
            this->my_tree.table.Get(neighbor_id);
        Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* temptemp =
            target;
        while (temptemp->next != nullptr) {
          temptemp = temptemp->next;
        }
        InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            true_parent_pointer =
                dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator,
                                            KeyEqualityChecker>*>(temptemp);
        LOG_DEBUG("Internal insert split delta on my merged neighbor");
        free(path);
        auto retval = true_parent_pointer->InternalInsert(
            split_key, boundary_key, new_node_id, tw);
        auto root_node = this->my_tree.table.Get(this->my_tree.root);
        this->my_tree.Traverse(root_node);
        return retval;
      }

      ret_val = internal_pointer->InternalInsert(split_key, boundary_key,
                                                  new_node_id, tw);
    } else {
      LOG_DEBUG("Splitting parent");
      ret_val = internal_pointer->InternalSplit(path, index - 1, split_key,
                                                 boundary_key, new_node_id, tw);
    }
    return ret_val;
  } else {
    LOG_DEBUG("Calling split root");
    ret_val = this->my_tree.SplitRoot(split_key, this->id, new_node_id);
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
bool InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
    InternalMerge(uint64_t* path, uint64_t index, KeyType merge_key,
                   ThreadWrapper<KeyType, ValueType, KeyComparator,
                                 KeyEqualityChecker>* tw) {
  // typename multimap<KeyType, uint64_t, KeyComparator>::iterator iter =
  //     key_list.find(merge_key);
  // key_list.erase(iter);

  if (index == 0) {
    // I am the root
    uint64_t root_size = this->my_tree.Get_size(this->id);
    LOG_DEBUG("Internal merge called for node id %ld", this->id);
    if (root_size == 1) {
      // The root is about to be empty, hence need to find the new root
      this->my_tree.Consolidate(this->id, true, tw);
      Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* self_ =
          this->my_tree.table.Get(this->id);

      InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
          self_node =
              dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator,
                                          KeyEqualityChecker>*>(self_);

      uint64_t right_child_size =
          this->my_tree.Get_size(self_node->key_list.begin()->second);
      uint64_t left_child_size =
          this->my_tree.Get_size(self_node->leftmost_pointer);
      LOG_DEBUG("right id %ld size is %ld, left id %ld size is %ld",
                self_node->key_list.begin()->second, right_child_size,
                self_node->leftmost_pointer, left_child_size);
      uint64_t new_root_id;
      if (left_child_size == 0)
        new_root_id = self_node->key_list.begin()->second;
      else if (right_child_size == 0)
        new_root_id = self_node->leftmost_pointer;
      else
        return true;
      // assert(false);  // this should never happen
      // this->my_tree.freelist.insert(self_node);
      tw->e->to_be_cleaned.push_back(self_node);
      LOG_DEBUG("Setting the new root as %ld", new_root_id);
      return __sync_bool_compare_and_swap(&this->my_tree.root, self_node->id,
                                          new_root_id);

      // TODO free path
    }
  }
  // TODO function needs to free path at the end
  LOG_DEBUG("Earlier size is %ld", this->key_list.size());
  this->InternalDelete(merge_key);
  bool ret_val = this->my_tree.Consolidate(this->id, true, tw);
  if (!ret_val) {
    return ret_val;
  }
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* self_ =
      this->my_tree.table.Get(this->id);

  InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
      self_node = dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator,
                                              KeyEqualityChecker>*>(self_);
  LOG_DEBUG("Later size is %ld", self_node->key_list.size());
  // We are the root node
  if (index == 0) return true;
  uint64_t neighbour_node_id = self_node->left_sibling;
  int direction = LEFT;
  if (self_node->left_sibling != 0 &&
      this->my_tree.table.Get(self_node->left_sibling)->type != REMOVE) {
    neighbour_node_id = self_node->left_sibling;
  } else if (self_node->right_sibling != 0 &&
             this->my_tree.table.Get(self_node->right_sibling)->type !=
                 REMOVE) {
    neighbour_node_id = self_node->right_sibling;
    direction = RIGHT;
  } else
    direction = UP;

  uint64_t parent_id = path[index - 1];
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
      parent_node_pointer = this->my_tree.table.Get(parent_id);
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* cur_pointer =
      parent_node_pointer;
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
      seen_remove_delta_node = nullptr;

  while (cur_pointer->next) {
    if (cur_pointer->type == REMOVE) {
      LOG_DEBUG("I've seen a remove delta in internal_merge of my parent");
      seen_remove_delta_node = cur_pointer;
    }
    cur_pointer = cur_pointer->next;
  }

  InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
      parent_pointer =
          dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator,
                                      KeyEqualityChecker>*>(cur_pointer);
  if (direction == UP) {
    // TODO: how to merge up??
  }

  // (a) Posting remove node delta
  RemoveDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
      remove_node =
          new RemoveDeltaNode<KeyType, ValueType, KeyComparator,
                              KeyEqualityChecker>(this->my_tree, this->id);
  this->my_tree.memory_usage += sizeof(*remove_node);
  remove_node->next = self_node;
  remove_node->merged_to_id = neighbour_node_id;
  remove_node->direction = direction;
  uint32_t chain_len = self_node->chain_len;
  remove_node->chain_len = chain_len + 1;
  ret_val = this->my_tree.table.Install(this->id, remove_node);
  if (!ret_val) return false;

  this->my_tree.Consolidate(neighbour_node_id, true, tw);
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* n_node_ =
      this->my_tree.table.Get(neighbour_node_id);
  InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
      n_node_pointer =
          dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator,
                                      KeyEqualityChecker>*>(n_node_);
  LOG_DEBUG("neighbour node id is %ld and it has size %ld", neighbour_node_id,
            n_node_pointer->key_list.size());

  // (b) posting merge delta
  MergeDeltaNode<KeyType, ValueType, KeyComparator,
                 KeyEqualityChecker>* merge_node =
      new MergeDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
          this->my_tree, neighbour_node_id);
  this->my_tree.memory_usage += sizeof(*merge_node);
  merge_node->node_to_be_merged = this;
  merge_node->next = n_node_pointer;
  merge_node->chain_len = n_node_pointer->chain_len + 1;
  if (direction == LEFT)
    merge_node->merge_key = self_node->key_list.begin()->first;
  else  // assume either LEFT or RIGHT
    merge_node->merge_key = n_node_pointer->key_list.begin()->first;
  ret_val = this->my_tree.table.Install(neighbour_node_id, merge_node);
  if (!ret_val) return false;

  // (c) posting index term delete delta or key redistribution
  uint64_t total_count = this->my_tree.Get_size(this->id) +
                         this->my_tree.Get_size(neighbour_node_id);

  // Given we are now in (b) state, we want either complete the merge
  // or redistribute the keys. Make sure to consolidate before proceed
  if (total_count > this->my_tree.max_node_size) {
    // Redistribute keys
    uint64_t lid = 0, rid = 0, l_neighbour = 0, r_neighbour = 0;
    KeyType old_left_max, new_left_max;
    if (direction == LEFT) {
      lid = neighbour_node_id;
      rid = self_node->id;
      l_neighbour = n_node_pointer->left_sibling;
      r_neighbour = self_node->right_sibling;
      old_left_max = n_node_pointer->key_list.end()->first;
    } else if (direction == RIGHT) {
      lid = self_node->id;
      rid = neighbour_node_id;
      l_neighbour = self_node->right_sibling;
      r_neighbour = n_node_pointer->left_sibling;
      old_left_max = self_node->key_list.end()->first;
    } else if (direction == UP) {
      assert(false);
    }
    InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* L =
        new InternalBWNode<KeyType, ValueType, KeyComparator,
                           KeyEqualityChecker>(this->my_tree.metadata,
                                               this->my_tree, lid);
    this->my_tree.memory_usage += sizeof(*L);
    InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* R =
        new InternalBWNode<KeyType, ValueType, KeyComparator,
                           KeyEqualityChecker>(this->my_tree.metadata,
                                               this->my_tree, rid);
    this->my_tree.memory_usage += sizeof(*R);

    L->left_sibling = l_neighbour;
    L->right_sibling = R->id;
    R->left_sibling = L->id;
    R->right_sibling = r_neighbour;

    multimap<KeyType, uint64_t, KeyComparator> temp(
        KeyComparator(this->my_tree.metadata));
    uint64_t half_count = total_count / 2;
    // TODO: this if-else is not actually required, the multimap will anyway
    // sort the values
    if (direction == LEFT) {
      typename multimap<KeyType, uint64_t, KeyComparator>::iterator
          left_iterator = n_node_pointer->key_list.begin();
      typename multimap<KeyType, uint64_t, KeyComparator>::iterator
          right_iterator = self_node->key_list.begin();

      for (; left_iterator != n_node_pointer->key_list.end(); ++left_iterator) {
        KeyType key = left_iterator->first;
        uint64_t value = left_iterator->second;
        temp.insert(pair<KeyType, uint64_t>(key, value));
      }
      for (; right_iterator != self_node->key_list.end(); ++right_iterator) {
        KeyType key = right_iterator->first;
        uint64_t value = right_iterator->second;
        temp.insert(pair<KeyType, uint64_t>(key, value));
      }
    } else if (direction == RIGHT) {
      typename multimap<KeyType, uint64_t, KeyComparator>::iterator
          left_iterator = self_node->key_list.begin();
      typename multimap<KeyType, uint64_t, KeyComparator>::iterator
          right_iterator = n_node_pointer->key_list.begin();

      for (; left_iterator != self_node->key_list.end(); ++left_iterator) {
        KeyType key = left_iterator->first;
        uint64_t value = left_iterator->second;
        temp.insert(pair<KeyType, uint64_t>(key, value));
      }
      for (; right_iterator != n_node_pointer->key_list.end();
           ++right_iterator) {
        KeyType key = right_iterator->first;
        uint64_t value = right_iterator->second;
        temp.insert(pair<KeyType, uint64_t>(key, value));
      }
    }
    assert(temp.size() == total_count);
    typename multimap<KeyType, uint64_t, KeyComparator>::iterator
        temp_iterator = temp.begin();
    for (int i = 0; i < half_count; ++i) {
      KeyType key = temp_iterator->first;
      uint64_t value = temp_iterator->second;
      L->key_list.insert(pair<KeyType, uint64_t>(key, value));
      temp_iterator++;
    }
    for (int i = half_count; i < total_count; ++i) {
      KeyType key = temp_iterator->first;
      uint64_t value = temp_iterator->second;
      R->key_list.insert(pair<KeyType, uint64_t>(key, value));
      temp_iterator++;
    }
    new_left_max = L->key_list.end()->first;
    KeyType new_split_key = R->key_list.begin()->first;
    ret_val = self_node->my_tree.table.Install(L->id, L);
    if (!ret_val) return false;
    ret_val = self_node->my_tree.table.Install(R->id, R);
    if (!ret_val) return false;

    // TODO: Free two old leaf nodes

    ret_val =
        parent_pointer->InternalUpdate(merge_node->merge_key, new_split_key);

    // ret_val = parent_pointer->InternalDelete(merge_node->merge_key);
    if (!ret_val) return false;

    ret_val = parent_pointer->InternalUpdate(old_left_max, new_left_max);
    if (!ret_val) return false;
    // we treat it as a split; sp means special purpose
    // KeyType sp_boundary_key = R->key_list.end()->first;

    // ret_val =
    //    parent_pointer->InternalInsert(new_split_key, sp_boundary_key, R->id,
    //    tw);
    // if (!ret_val) return false;

    return ret_val;
  } else {  // Normal merge case
    // We don't need to actually move the key list items because we have already
    // put up the merge and remove delta
    // nodes which serve as the logical merge
    /*
    typename multimap<KeyType, uint64_t, KeyComparator>::iterator
        this_iterator = self_node->key_list.begin();
    if(this->my_tree.comparator(self_node->key_list.begin()->first,
    n_node_pointer->key_list.begin()->first))
    {
      //TODO should do a CAS here
      LOG_DEBUG("Updating the leftmost pointer of %ld to %ld",
    neighbour_node_id,
    self_node->leftmost_pointer);
      n_node_pointer->leftmost_pointer = self_node->leftmost_pointer;
    }
    for (; this_iterator != self_node->key_list.end(); this_iterator++) {
      KeyType key = this_iterator->first;
      uint64_t value = this_iterator->second;
      // This relies on the split_key <= search_key && search_key <=
      // boundary_key
      n_node_pointer->InternalInsert(key, key, value, tw);
    }
    */
    uint64_t parent_size = self_node->my_tree.Get_size(parent_id);
    if (index == 0)
      return true;  // I am the root, no need to do anything further
    else if (parent_size - 1 > self_node->my_tree.min_node_size)
      ret_val = parent_pointer->InternalDelete(merge_node->merge_key);
    else {
      LOG_DEBUG("Calling merge on the parent with id %ld", parent_id);
      if (seen_remove_delta_node != nullptr) {
        RemoveDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            seen_remove_delta =
                dynamic_cast<RemoveDeltaNode<KeyType, ValueType, KeyComparator,
                                             KeyEqualityChecker>*>(
                    seen_remove_delta_node);
        LOG_DEBUG("Redriect the cascading delete to parent's neighbor");
        uint64_t neighbor_id = seen_remove_delta->merged_to_id;
        uint64_t neighbor_node_size = this->my_tree.Get_size(neighbor_id);
        if (neighbor_node_size == this->my_tree.min_node_size + 1) {
          // Tricky case, we consolidate because the node is about to underflow
          LOG_DEBUG("Finish the consolidation");
          this->my_tree.Consolidate(neighbor_id, true, tw);
        }
        // Since there could be consolidating happening, seen_remove_delta is no
        // longer accessible!!!
        Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* target =
            this->my_tree.table.Get(neighbor_id);
        Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* temptemp =
            target;
        while (temptemp->next != nullptr) {
          temptemp = temptemp->next;
        }
        InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            true_parent_pointer =
                dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator,
                                            KeyEqualityChecker>*>(temptemp);
        LOG_DEBUG("Internal delete on my merged neighbor");
        free(path);
        auto retval =
            true_parent_pointer->InternalDelete(merge_node->merge_key);
        auto root_node = this->my_tree.table.Get(this->my_tree.root);
        this->my_tree.Traverse(root_node);
        return retval;
      }

      ret_val = parent_pointer->InternalMerge(path, index - 1,
                                               merge_node->merge_key, tw);
    }
    return ret_val;
  }

  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
bool LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
    LeafMerge(uint64_t* path, uint64_t index, KeyType key, ValueType value,
               ThreadWrapper<KeyType, ValueType, KeyComparator,
                             KeyEqualityChecker>* tw) {
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
  this->LeafDelete(key, value);
  // We are the root node
  if (index == 0) return true;
  bool ret_val = this->my_tree.Consolidate(this->id, true, tw);
  if (!ret_val) {
    return ret_val;
  }
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* self_ =
      this->my_tree.table.Get(this->id);

  LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* self_node =
      dynamic_cast<
          LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
          self_);
  uint64_t neighbour_node_id = self_node->left_sibling;
  int direction = LEFT;
  if (self_node->left_sibling != 0 &&
      this->my_tree.table.Get(self_node->left_sibling)->type != REMOVE) {
    neighbour_node_id = self_node->left_sibling;
  } else if (self_node->right_sibling != 0 &&
             this->my_tree.table.Get(self_node->right_sibling)->type !=
                 REMOVE) {
    neighbour_node_id = self_node->right_sibling;
    direction = RIGHT;
  } else {
    LOG_DEBUG("No neighbout node found");
    direction = UP;
  }

  uint64_t parent_id = path[index - 1];
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
      parent_node_pointer = this->my_tree.table.Get(parent_id);
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* cur_pointer =
      parent_node_pointer;
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
      seen_remove_delta_node = nullptr;

  while (cur_pointer->next) {
    if (cur_pointer->type == REMOVE) {
      LOG_DEBUG("I've seen a remove delta in LeafMerge of my parent");
      seen_remove_delta_node = cur_pointer;
    }
    cur_pointer = cur_pointer->next;
  }

  InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
      parent_pointer =
          dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator,
                                      KeyEqualityChecker>*>(cur_pointer);
  if (direction == UP) {
    // TODO: how to merge up??
  }

  LOG_DEBUG("neighbout node id  for leaf merge of %ld is %ld with direction %d",
            self_node->id, neighbour_node_id, direction);
  // (a) Posting remove node delta

  RemoveDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
      remove_node =
          new RemoveDeltaNode<KeyType, ValueType, KeyComparator,
                              KeyEqualityChecker>(this->my_tree, this->id);
  this->my_tree.memory_usage += sizeof(*remove_node);
  LOG_DEBUG("Remove node = %p | self_node->id = %lu | self_node = %p",
            remove_node, self_node->id, self_node);
  remove_node->next = self_node;
  remove_node->merged_to_id = neighbour_node_id;
  remove_node->direction = direction;
  uint32_t chain_len = self_node->chain_len;
  remove_node->chain_len = chain_len + 1;
  ret_val = this->my_tree.table.Install(this->id, remove_node);
  if (!ret_val) return false;

  // (b) posting merge delta
  this->my_tree.Consolidate(neighbour_node_id, true, tw);
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* n_node_ =
      this->my_tree.table.Get(neighbour_node_id);
  LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
      n_node_pointer = dynamic_cast<
          LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
          n_node_);
  MergeDeltaNode<KeyType, ValueType, KeyComparator,
                 KeyEqualityChecker>* merge_node =
      new MergeDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
          this->my_tree, neighbour_node_id);
  this->my_tree.memory_usage += sizeof(*merge_node);
  merge_node->node_to_be_merged = self_node;
  merge_node->next = n_node_pointer;
  merge_node->chain_len = n_node_pointer->chain_len + 1;
  if (direction == LEFT) {
    merge_node->merge_key = self_node->kv_list.begin()->first;
  } else  // assume direction is either left or right
  {
    merge_node->merge_key = n_node_pointer->kv_list.begin()->first;
  }
  ret_val = this->my_tree.table.Install(neighbour_node_id, merge_node);
  if (!ret_val) return false;

  // (c) posting index term delete delta or key redistribution
  uint64_t total_count = this->my_tree.Get_size(this->id) +
                         this->my_tree.Get_size(neighbour_node_id);

  // Given we are now in (b) state, we want either complete the merge
  // or redistribute the keys. Make sure to consolidate before proceed
  if (total_count > this->my_tree.max_node_size) {
    // Redistribute keys
    uint64_t lid, rid, l_neighbour, r_neighbour;
    KeyType old_left_max, new_left_max;
    if (direction == LEFT) {
      lid = neighbour_node_id;
      rid = self_node->id;
      l_neighbour = n_node_pointer->left_sibling;
      r_neighbour = self_node->right_sibling;
      old_left_max = n_node_pointer->kv_list.end()->first;
    } else if (direction == RIGHT) {
      lid = self_node->id;
      rid = neighbour_node_id;
      l_neighbour = self_node->right_sibling;
      r_neighbour = n_node_pointer->left_sibling;
      old_left_max = self_node->kv_list.end()->first;
    } else if (direction == UP) {
      assert(false);
    }
    LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* L =
        new LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
            this->my_tree.metadata, this->my_tree, lid);
    this->my_tree.memory_usage += sizeof(*L);
    LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* R =
        new LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
            this->my_tree.metadata, this->my_tree, rid);
    this->my_tree.memory_usage += sizeof(*R);
    L->left_sibling = l_neighbour;
    L->right_sibling = R->id;
    R->left_sibling = L->id;
    R->right_sibling = r_neighbour;

    multimap<KeyType, ValueType, KeyComparator> temp(
        KeyComparator(this->my_tree.metadata));
    uint64_t half_count = total_count / 2;
    // TODO: we don't need separate LEFT and RIGHT cases, the multimap would
    // sort according to key automatically
    if (direction == LEFT) {
      typename multimap<KeyType, ValueType, KeyComparator>::iterator
          left_iterator = n_node_pointer->kv_list.begin();
      typename multimap<KeyType, ValueType, KeyComparator>::iterator
          right_iterator = self_node->kv_list.begin();

      for (; left_iterator != n_node_pointer->kv_list.end(); ++left_iterator) {
        KeyType key = left_iterator->first;
        ValueType value = left_iterator->second;
        temp.insert(pair<KeyType, ValueType>(key, value));
      }
      for (; right_iterator != self_node->kv_list.end(); ++right_iterator) {
        KeyType key = right_iterator->first;
        ValueType value = right_iterator->second;
        temp.insert(pair<KeyType, ValueType>(key, value));
      }
    } else if (direction == RIGHT) {
      typename multimap<KeyType, ValueType, KeyComparator>::iterator
          left_iterator = self_node->kv_list.begin();
      typename multimap<KeyType, ValueType, KeyComparator>::iterator
          right_iterator = n_node_pointer->kv_list.begin();

      for (; left_iterator != self_node->kv_list.end(); ++left_iterator) {
        KeyType key = left_iterator->first;
        ValueType value = left_iterator->second;
        temp.insert(pair<KeyType, ValueType>(key, value));
      }
      for (; right_iterator != n_node_pointer->kv_list.end();
           ++right_iterator) {
        KeyType key = right_iterator->first;
        ValueType value = right_iterator->second;
        temp.insert(pair<KeyType, ValueType>(key, value));
      }
    }
    assert(temp.size() == total_count);
    typename multimap<KeyType, ValueType, KeyComparator>::iterator
        temp_iterator = temp.begin();
    for (int i = 0; i < half_count; ++i) {
      KeyType key = temp_iterator->first;
      ValueType value = temp_iterator->second;
      L->kv_list.insert(pair<KeyType, ValueType>(key, value));
      temp_iterator++;
    }
    for (int i = half_count; i < total_count; ++i) {
      KeyType key = temp_iterator->first;
      ValueType value = temp_iterator->second;
      R->kv_list.insert(pair<KeyType, ValueType>(key, value));
      temp_iterator++;
    }
    new_left_max = L->kv_list.end()->first;
    KeyType new_split_key = R->kv_list.begin()->first;
    ret_val = this->my_tree.table.Install(L->id, L);
    if (!ret_val) return false;
    ret_val = this->my_tree.table.Install(R->id, R);
    if (!ret_val) return false;

    // TODO: Free two old leaf nodes

    ret_val =
        parent_pointer->InternalUpdate(merge_node->merge_key, new_split_key);

    // ret_val = parent_pointer->InternalDelete(merge_node->merge_key);
    if (!ret_val) return false;

    ret_val = parent_pointer->InternalUpdate(old_left_max, new_left_max);
    if (!ret_val) return false;
    // we treat it as a split; sp means special purpose
    // KeyType sp_boundary_key = R->kv_list.end()->first;
    // ret_val =
    //    parent_pointer->InternalInsert(new_split_key, sp_boundary_key, R->id,
    //    tw);
    // if (!ret_val) return false;

    return ret_val;
  } else {  // Normal merge case
    // We don't need to actually move kv list items because we have put the
    // merge and delta nodes which are equivalent
    // to a logical merge
    /*
    typename multimap<KeyType, ValueType, KeyComparator>::iterator
        this_iterator = self_node->kv_list.begin();
    for (; this_iterator != self_node->kv_list.end(); this_iterator++) {
      KeyType key = this_iterator->first;auto root_node = table.Get(root);
    Traverse(root_node);
      ValueType value = this_iterator->second;
      n_node_pointer->LeafInsert(key, value);
    }
    */
    uint64_t parent_size = this->my_tree.Get_size(parent_id);
    if (index == 0)
      return true;  // I am the root, no need to do anything else
    else if (parent_size - 1 > this->my_tree.min_node_size) {
      LOG_DEBUG("calling internal delete");
      ret_val = parent_pointer->InternalDelete(merge_node->merge_key);
    } else {
      LOG_DEBUG("calling internal merge");
      if (seen_remove_delta_node != nullptr) {
        RemoveDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            seen_remove_delta =
                dynamic_cast<RemoveDeltaNode<KeyType, ValueType, KeyComparator,
                                             KeyEqualityChecker>*>(
                    seen_remove_delta_node);
        LOG_DEBUG("Redriect the cascading delete to parent's neighbor");
        uint64_t neighbor_id = seen_remove_delta->merged_to_id;
        uint64_t neighbor_node_size = this->my_tree.Get_size(neighbor_id);
        if (neighbor_node_size == this->my_tree.min_node_size + 1) {
          // Tricky case, we consolidate because the node is about to underflow
          LOG_DEBUG("Finish the consolidation");
          this->my_tree.Consolidate(neighbor_id, true, tw);
        }
        // Since there could be consolidating happening, seen_remove_delta is no
        // longer accessible!!!
        Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* target =
            this->my_tree.table.Get(neighbor_id);
        Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* temptemp =
            target;
        while (temptemp->next != nullptr) {
          temptemp = temptemp->next;
        }
        InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            true_parent_pointer =
                dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator,
                                            KeyEqualityChecker>*>(temptemp);
        LOG_DEBUG("Internal delete on my merged neighbor");
        free(path);
        auto retval =
            true_parent_pointer->InternalDelete(merge_node->merge_key);
        auto root_node = this->my_tree.table.Get(this->my_tree.root);
        this->my_tree.Traverse(root_node);
        return retval;
      }
      ret_val = parent_pointer->InternalMerge(path, index - 1,
                                               merge_node->merge_key, tw);
    }
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

  uint64_t new_id = this->my_tree.table.GetNextId();
  LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
      new_leaf_node =
          new LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
              this->my_tree.metadata, this->my_tree, new_id);
  this->my_tree.memory_usage += sizeof(*new_leaf_node);
  multimap<KeyType, ValueType, KeyComparator> insert_set(
      KeyComparator(this->my_tree.metadata));
  multimap<KeyType, ValueType, KeyComparator> delete_set(
      KeyComparator(this->my_tree.metadata));
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
  LOG_DEBUG("Get size called for node id %ld", node_id);

  // switch(node_pointer->type){
  //   case(LEAF_BW_NODE):
  //     return leaf_pointer->id;
  //     break;
  //   case(INTERNAL_BW_NODE):
  //     InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
  //     internal_pointer = dynamic_cast<InternalBWNode<KeyType, ValueType,
  //     KeyComparator, KeyEqualityChecker>*>(node_pointer);
  //     prev_id = cur_id;
  //     cur_id = internal_pointer->GetChildId(key);
  //     try_consolidation = true;
  //     break;
  KeyType split_key;
  bool split_delta_encountered = false;
  while (node_pointer != nullptr) {
    // Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
    // simple_pointer = nullptr;
    LOG_DEBUG("Type encountered is %d", node_pointer->type);
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
        typename multimap<KeyType, ValueType, KeyComparator>::iterator iter =
            leaf_pointer->kv_list.begin();
        LOG_DEBUG("Leaf node size = %lu", leaf_pointer->kv_list.size());
        for (; iter != leaf_pointer->kv_list.end(); iter++) {
          if (split_delta_encountered && !comparator(iter->first, split_key))
            continue;
          else {
            LOG_DEBUG("Adding one to count for the leaf");
            count++;
          }
        }
        end = true;
      } break;
      case (SPLIT): {
        split_delta_encountered = true;
        SplitDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            split_pointer =
                dynamic_cast<SplitDeltaNode<KeyType, ValueType, KeyComparator,
                                            KeyEqualityChecker>*>(node_pointer);
        split_key = split_pointer->split_key;
      } break;
      case (MERGE): {
        MergeDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            mdn =
                dynamic_cast<MergeDeltaNode<KeyType, ValueType, KeyComparator,
                                            KeyEqualityChecker>*>(node_pointer);

        Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            merged_node_pointer = mdn->node_to_be_merged;
        if (merged_node_pointer->type == LEAF_BW_NODE) {
          LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
              to_be_merged =
                  dynamic_cast<LeafBWNode<KeyType, ValueType, KeyComparator,
                                          KeyEqualityChecker>*>(
                      merged_node_pointer);
          LOG_DEBUG("Adding %ld from the merged node",
                    to_be_merged->kv_list.size());
          count += to_be_merged->kv_list.size();
        } else {
          InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
              to_be_merged =
                  dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator,
                                              KeyEqualityChecker>*>(
                      merged_node_pointer);
          LOG_DEBUG("Adding %ld from the merged node",
                    to_be_merged->key_list.size());
          count += to_be_merged->key_list.size();
        }
      } break;
      case (REMOVE):
        return 0;
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
        typename multimap<KeyType, uint64_t, KeyComparator>::iterator iter =
            internal_pointer->key_list.begin();
        for (; iter != internal_pointer->key_list.end(); iter++) {
          if (split_delta_encountered && !comparator(iter->first, split_key))
            continue;
          else {
            LOG_DEBUG("Adding one to self in internal case");
            count++;
          }
        }
        end = true;
      } break;
      default:
        LOG_DEBUG("here is where it is");
        break;
    }
    LOG_DEBUG("in the while loop");
    if (!end)
      node_pointer = node_pointer->next;
    else
      break;
  }
  LOG_DEBUG("returning count %lu...", count);
  return count;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
vector<ItemPointer>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanWrapper(
    const vector<Value>& values, const vector<oid_t>& key_column_ids,
    const vector<ExpressionType>& expr_types,
    const ScanDirectionType& scan_direction) {
  auto tw =
      new ThreadWrapper<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
          current_epoch);
  this->memory_usage += sizeof(*tw);
  // auto e = current_epoch;
  tw->e->join();
  auto result = Scan(values, key_column_ids, expr_types, scan_direction, tw);
  if (tw->e->leave()) {
    this->memory_usage -= sizeof(*(tw->e));
    delete tw->e;
  }
  this->memory_usage -= sizeof(*tw);
  delete tw;
  return result;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
vector<ItemPointer>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Scan(
    const vector<Value>& values, const vector<oid_t>& key_column_ids,
    const vector<ExpressionType>& expr_types,
    const ScanDirectionType& scan_direction,
    ThreadWrapper<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* tw) {
  vector<ItemPointer> result;
  KeyType index_key;
  std::unique_ptr<storage::Tuple> start_key;

  // If it is a special case, we can figure out the range to scan in the index
  // if (special_case == true) {
  auto schema = new storage::Tuple(metadata->GetKeySchema(), true);
  this->memory_usage += sizeof(*schema);
  start_key.reset(schema);
  index_key.SetFromKey(start_key.get());

  // Construct the lower bound key tuple

  // Set scan begin iterator
  // scan_begin_itr = container.equal_range(index_key).first;
  //}
  //

  uint64_t* path = (uint64_t*)malloc(tree_height * sizeof(uint64_t));
  uint64_t location;
  uint64_t leaf_id = Search(index_key, path, location, tw);

  bool reached_end = false;
  while (!reached_end) {
    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer =
        this->table.Get(leaf_id);
    while (node_pointer->next) node_pointer = node_pointer->next;
    LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
        leaf_pointer = nullptr;
    leaf_pointer = dynamic_cast<
        LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
        node_pointer);
    switch (scan_direction) {
      case SCAN_DIRECTION_TYPE_FORWARD:
        if (leaf_pointer->left_sibling != 0)
          leaf_id = leaf_pointer->left_sibling;
        else
          reached_end = true;
        break;
      case SCAN_DIRECTION_TYPE_BACKWARD:
        if (leaf_pointer->right_sibling != 0)
          leaf_id = leaf_pointer->right_sibling;
        else
          reached_end = true;
        break;
      case SCAN_DIRECTION_TYPE_INVALID:
      default:
        throw Exception("Invalid scan direction ");
        break;
    }
  }

  bool move_forward = true;

  switch (scan_direction) {
    case SCAN_DIRECTION_TYPE_FORWARD:
      move_forward = true;
      break;
    case SCAN_DIRECTION_TYPE_BACKWARD:
      move_forward = false;
      break;
    case SCAN_DIRECTION_TYPE_INVALID:
    default:
      throw Exception("Invalid scan direction ");
      break;
  }
  reached_end = false;
  while (!reached_end) {
    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer =
        this->table.Get(leaf_id);
    while (node_pointer->next) {
      switch (node_pointer->type) {
        case (INSERT): {
          DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
              simple_pointer =
                  dynamic_cast<DeltaNode<KeyType, ValueType, KeyComparator,
                                         KeyEqualityChecker>*>(node_pointer);
          auto inserted_key = simple_pointer->key;
          auto tuple =
              inserted_key.GetTupleForComparison(metadata->GetKeySchema());
          if (Index::Compare(tuple, key_column_ids, expr_types, values) ==
              true) {
            ItemPointer location = simple_pointer->value;
            result.push_back(location);
          }
          break;
        }
        case (DELETE):
          break;
        case (SPLIT):
          break;
        case (MERGE):
          break;
        case (REMOVE):
          break;
        case (LEAF_BW_NODE): {
          LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
              leaf_pointer = nullptr;
          leaf_pointer =
              dynamic_cast<LeafBWNode<KeyType, ValueType, KeyComparator,
                                      KeyEqualityChecker>*>(node_pointer);
          typename multimap<KeyType, ValueType>::iterator iter =
              leaf_pointer->kv_list.begin();
          for (; iter != leaf_pointer->kv_list.end(); iter++) {
            auto leaf_key = iter->first;
            auto tuple =
                leaf_key.GetTupleForComparison(metadata->GetKeySchema());
            if (Index::Compare(tuple, key_column_ids, expr_types, values) ==
                true) {
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
    typename multimap<KeyType, ValueType>::iterator iter =
        leaf_pointer->kv_list.begin();
    for (; iter != leaf_pointer->kv_list.end(); iter++) {
      auto leaf_key = iter->first;
      auto tuple = leaf_key.GetTupleForComparison(metadata->GetKeySchema());
      if (Index::Compare(tuple, key_column_ids, expr_types, values) == true) {
        ItemPointer location = iter->second;
        result.push_back(location);
      }
    }
    if (move_forward) {
      if (leaf_pointer->right_sibling)
        leaf_id = leaf_pointer->right_sibling;
      else
        reached_end = true;
    } else {
      if (leaf_pointer->left_sibling)
        leaf_id = leaf_pointer->left_sibling;
      else
        reached_end = true;
    }
  }

  return result;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
vector<ItemPointer> BWTree<KeyType, ValueType, KeyComparator,
                           KeyEqualityChecker>::ScanAllKeysWrapper() {
  auto tw =
      new ThreadWrapper<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
          current_epoch);
  this->memory_usage += sizeof(*tw);
  // auto e = current_epoch;
  tw->e->join();
  auto result = ScanAllKeys(tw);
  if (tw->e->leave()) {
    this->memory_usage -= sizeof(*(tw->e));
    delete tw->e;
  }
  this->memory_usage -= sizeof(*tw);
  delete tw;
  return result;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
vector<ItemPointer>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanAllKeys(
    ThreadWrapper<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* tw) {
  vector<ItemPointer> result;
  KeyType index_key;
  std::unique_ptr<storage::Tuple> start_key;
  auto schema = new storage::Tuple(metadata->GetKeySchema(), true);
  this->memory_usage += sizeof(*tw);
  start_key.reset(schema);
  index_key.SetFromKey(start_key.get());

  uint64_t* path = (uint64_t*)malloc(tree_height * sizeof(uint64_t));
  uint64_t location;
  uint64_t leaf_id = Search(index_key, path, location, tw);
  //uint64_t start_leaf_id = leaf_id;
  //uint64_t first_right_leaf_id = 0;
  bool reached_end = false;
  while (!reached_end) {
    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer =
        this->table.Get(leaf_id);
    while (node_pointer->next) node_pointer = node_pointer->next; // traversing down to leaf
    LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
        leaf_pointer = nullptr;
    leaf_pointer = dynamic_cast<
        LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
        node_pointer);
    //if((leaf_id == start_leaf_id) && (leaf_pointer->right_sibling)) {
      //first_right_leaf_id = leaf_pointer->right_sibling;
    //}
    if (leaf_pointer->left_sibling != 0)
      leaf_id = leaf_pointer->left_sibling;
    else
      reached_end = true;
  }

  reached_end = false;
  //leaf_id = first_right_leaf_id;
  while (!reached_end) {
    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer =
        this->table.Get(leaf_id);
    while (node_pointer->next) {
      switch (node_pointer->type) {
        case (INSERT): {
          /*DeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
              simple_pointer =
                  dynamic_cast<DeltaNode<KeyType, ValueType, KeyComparator,
                                         KeyEqualityChecker>*>(node_pointer);
          ItemPointer location = simple_pointer->value;
          result.push_back(location);*/
          break;
        }
        case (DELETE):
          break;
        case (SPLIT):
          break;
        case (MERGE):
          break;
        case (REMOVE):
          break;
        case (LEAF_BW_NODE):
          break;
          //{
          /*printf("LEAF ID = %lu\n", leaf_id);
          LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
              leaf_pointer = nullptr;
          leaf_pointer =
              dynamic_cast<LeafBWNode<KeyType, ValueType, KeyComparator,
                                      KeyEqualityChecker>*>(node_pointer);
          typename multimap<KeyType, ValueType>::iterator iter =
              leaf_pointer->kv_list.begin();
          for (; iter != leaf_pointer->kv_list.end(); iter++) {
            ItemPointer location = iter->second;
            result.push_back(location);
          }
          if (leaf_pointer->right_sibling)
            leaf_id = leaf_pointer->right_sibling;
          else
            reached_end = true;
        }*/
        //break;
      }
      node_pointer = node_pointer->next;
    }
    printf("LEAF ID = %lu | size of result = %lu\n", leaf_id, result.size());
    /*
     * Split leaf node is not truncated; so we call Get_size on the leaf node and only iterate
     * through those values starting from the left.
     */
    //FIXME: may need to change Get_size approach if we don't consolidate before ScanAllKeys.
    LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
        leaf_pointer = nullptr;
    leaf_pointer = dynamic_cast<
        LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*>(
        node_pointer);
    typename multimap<KeyType, ValueType>::iterator iter =
        leaf_pointer->kv_list.begin();
    for(uint64_t i=0; i<this->Get_size(leaf_id); i++) {
      ItemPointer location = iter->second;
      iter++;
      result.push_back(location);
    }
    printf("Siblings: left = %lu, right = %lu\n", leaf_pointer->left_sibling, leaf_pointer->right_sibling);
    if (leaf_pointer->right_sibling)
      leaf_id = leaf_pointer->right_sibling;
    else
      reached_end = true;
  }

  return result;
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
bool InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
    InternalInsert(
        KeyType split_key, KeyType boundary_key, uint64_t new_node_id,
        ThreadWrapper<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* tw
        __attribute__((unused))) {
  LOG_DEBUG("Internal insert called on node id %ld", this->id);
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer =
      this->my_tree.table.Get(this->id);
  SplitIndexDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
      split_index =
          new SplitIndexDeltaNode<KeyType, ValueType, KeyComparator,
                                  KeyEqualityChecker>(this->my_tree, this->id);
  this->my_tree.memory_usage += sizeof(*split_index);
  LOG_DEBUG("Here is how the new key compares to existing keys");
  typename multimap<KeyType, uint64_t, KeyComparator>::iterator iter =
      this->key_list.begin();
  for (; iter != this->key_list.end(); iter++) {
    LOG_DEBUG("LOOP %ld %d %d", distance(this->key_list.begin(), iter),
              this->my_tree.equals(iter->first, split_key),
              this->my_tree.comparator(iter->first, split_key));
    fflush(stdout);
  }
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
bool InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
    InternalSplit(uint64_t* path, uint64_t index, KeyType requested_key,
                   KeyType requested_boundary_key, uint64_t new_node_id,
                   ThreadWrapper<KeyType, ValueType, KeyComparator,
                                 KeyEqualityChecker>* tw) {
  bool ret_val = true;
  this->InternalInsert(requested_key, requested_boundary_key, new_node_id, tw);
  ret_val = this->my_tree.Consolidate(this->id, true, tw);
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* self_ =
      this->my_tree.table.Get(this->id);

  InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
      self_node = dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator,
                                              KeyEqualityChecker>*>(self_);
  if (!ret_val) return false;

  uint64_t new_internal_node_id = this->my_tree.table.GetNextId();
  InternalBWNode<KeyType, ValueType, KeyComparator,
                 KeyEqualityChecker>* new_internal_node =
      new InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
          this->my_tree.metadata, this->my_tree, new_internal_node_id);
  this->my_tree.memory_usage += sizeof(*new_internal_node);
  new_internal_node->left_sibling = this->id;
  new_internal_node->right_sibling = this->right_sibling;
  if (self_node->right_sibling != 0) {
    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
        right_sibling_node = this->my_tree.table.Get(self_node->right_sibling);
    while (right_sibling_node->next)
      right_sibling_node = right_sibling_node->next;

    InternalBWNode<KeyType, ValueType, KeyComparator,
                   KeyEqualityChecker>* right_sibling_internal_node =
        dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator,
                                    KeyEqualityChecker>*>(right_sibling_node);
    right_sibling_internal_node->left_sibling = new_node_id;
  }
  self_node->right_sibling = new_internal_node_id;
  uint64_t count = self_node->key_list.size();
  typename multimap<KeyType, uint64_t, KeyComparator>::iterator key_it =
      self_node->key_list.begin();
  advance(key_it, count / 2);
  KeyType split_key = key_it->first;
  key_it = self_node->key_list.end();
  KeyType boundary_key = key_it->first;

  typename multimap<KeyType, uint64_t, KeyComparator>::iterator split_iterator =
      self_node->key_list.begin();
  advance(split_iterator, count / 2);
  split_iterator--;
  new_internal_node->leftmost_pointer = split_iterator->second;
  LOG_DEBUG("Set the leftmost of the new internal node %ld to be %ld",
            new_internal_node->id, new_internal_node->leftmost_pointer);
  split_iterator++;

  LOG_DEBUG("Copying to internal new node");
  for (; split_iterator != self_node->key_list.end(); split_iterator++) {
    pair<KeyType, uint64_t> new_entry = *split_iterator;
    LOG_DEBUG("Aye ");
    new_internal_node->key_list.insert(new_entry);
  }

  /*
  // This is very tricky
  if (this->my_tree.comparator(requested_key, split_key)) {
    // If both req_split_key and req_boundary_key fall in my range, I add
    // split_index on myself
    if (this->my_tree.comparator(requested_boundary_key, split_key)) {
      self_node->InternalInsert(requested_key, requested_boundary_key,
  this->id, tw);
    } else {
      // Just set requested_boundary_key = 0 but only rely on the assuption that
      // no Search function will ever use this boundary key since it's on the
      // other side
      // UPDATE: it turns out that this could not happen since the
      // req_split/boundary key
      // won't be apart
      // this -> InternalInsert(requested_key, 0, this -> id);
      assert(false);
    }

  } else {
    LOG_DEBUG("Inserting into the new internal leaf node");
    new_internal_node->key_list.insert(pair<KeyType, uint64_t>(requested_key,
  new_node_id));
  }
  */
  // I just finished what the lower level tolds me to do, now it's for my
  // own request
  SplitDeltaNode<KeyType, ValueType, KeyComparator,
                 KeyEqualityChecker>* split_node =
      new SplitDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>(
          this->my_tree, this->id);
  this->my_tree.memory_usage += sizeof(*split_node);
  split_node->next = self_node;
  split_node->target_node_id = new_internal_node_id;
  split_node->split_key = split_key;

  uint32_t chain_len = self_node->chain_len;
  new_internal_node->chain_len = 0;
  ret_val =
      this->my_tree.table.Install(new_internal_node_id, new_internal_node);
  if (!ret_val) return false;
  split_node->chain_len = chain_len + 1;
  ret_val = this->my_tree.table.Install(this->id, split_node);
  if (!ret_val) return false;

  LOG_DEBUG("This split node now has size %ld and the new node has size %ld",
            this->my_tree.Get_size(this->id),
            this->my_tree.Get_size(new_node_id));
  if (index != 0) {
    uint64_t parent_id = path[index - 1];
    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
        parent_node_pointer = this->my_tree.table.Get(parent_id);

    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* cur_pointer =
        parent_node_pointer;
    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
        seen_remove_delta_node = nullptr;

    while (cur_pointer->next) {
      if (cur_pointer->type == REMOVE) {
        LOG_DEBUG("I've seen a remove delta in internal_split of my parent");
        seen_remove_delta_node = cur_pointer;
      }
      cur_pointer = cur_pointer->next;
    }

    InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
        internal_pointer =
            dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator,
                                        KeyEqualityChecker>*>(cur_pointer);

    uint64_t parent_size = this->my_tree.Get_size(parent_id);
    if (parent_size + 1 < this->my_tree.max_node_size) {
      if (seen_remove_delta_node != nullptr) {
        RemoveDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            seen_remove_delta =
                dynamic_cast<RemoveDeltaNode<KeyType, ValueType, KeyComparator,
                                             KeyEqualityChecker>*>(
                    seen_remove_delta_node);
        LOG_DEBUG("Redriect the cascading split to parent's neighbor");
        uint64_t neighbor_id = seen_remove_delta->merged_to_id;
        uint64_t neighbor_node_size = this->my_tree.Get_size(neighbor_id);
        if (neighbor_node_size == this->my_tree.max_node_size - 1) {
          // Tricky case, we consolidate because the node is about to underflow
          LOG_DEBUG("Finish the consolidation");
          this->my_tree.Consolidate(neighbor_id, true, tw);
        }
        // Since there could be consolidating happening, seen_remove_delta is no
        // longer accessible!!!
        Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* target =
            this->my_tree.table.Get(neighbor_id);
        Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* temptemp =
            target;
        while (temptemp->next != nullptr) {
          temptemp = temptemp->next;
        }
        InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
            true_parent_pointer =
                dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator,
                                            KeyEqualityChecker>*>(temptemp);
        LOG_DEBUG("Internal insert split delta on my merged neighbor");
        free(path);
        auto retval = true_parent_pointer->InternalInsert(
            split_key, boundary_key, new_node_id, tw);
        auto root_node = this->my_tree.table.Get(this->my_tree.root);
        this->my_tree.Traverse(root_node);
        return retval;
      }

      ret_val = internal_pointer->InternalInsert(split_key, boundary_key,
                                                  new_node_id, tw);
    } else
      ret_val = internal_pointer->InternalSplit(path, index - 1, split_key,
                                                 boundary_key, new_node_id, tw);
    return ret_val;
  } else {
    ret_val =
        this->my_tree.SplitRoot(split_key, this->id, new_internal_node_id);
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
uint64_t InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
    GetChildId(KeyType key, vector<pair<KeyType, KeyType>> updated_keys) {
  typename multimap<KeyType, uint64_t, KeyComparator>::reverse_iterator iter =
      key_list.rbegin();
  for (; iter != key_list.rend(); iter++) {
    KeyType cur_key = iter->first;
    typename vector<pair<KeyType, KeyType>>::reverse_iterator
        updated_keys_iter = updated_keys.rbegin();
    for (; updated_keys_iter != updated_keys.rend(); updated_keys_iter++) {
      if (this->my_tree.equals(cur_key, updated_keys_iter->first)) {
        cur_key = updated_keys_iter->second;
      }
    }
    if (!this->my_tree.comparator(key, cur_key)) {
      return iter->second;
    }
  }
  if (iter == key_list.rend()) {
    LOG_DEBUG("returning leftmost pointer because no key matched");
    return leftmost_pointer;
  }

  assert(false);
  return 0;
}

// template <typename KeyType, typename ValueType, typename KeyComparator,
//           typename KeyEqualityChecker>
// uint64_t InternalBWNode<KeyType, ValueType, KeyComparator,
//                         KeyEqualityChecker>::Get_parent_id(uint64_t id) {
//   typename multimap<KeyType, uint64_t, KeyComparator>::reverse_iterator iter
//   =
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
                    KeyEqualityChecker>::InternalDelete(KeyType merged_key) {
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer =
      this->my_tree.table.Get(this->id);
  RemoveIndexDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
      remove_index =
          new RemoveIndexDeltaNode<KeyType, ValueType, KeyComparator,
                                   KeyEqualityChecker>(this->my_tree, this->id);
  this->my_tree.memory_usage += sizeof(*remove_index);
  remove_index->deleted_key = merged_key;
  remove_index->next = node_pointer;
  uint32_t chain_len = node_pointer->chain_len;
  remove_index->chain_len = chain_len + 1;
  LOG_DEBUG(
      "merge key compares with the parent index as %d %d",
      this->my_tree.equals(merged_key, this->key_list.begin()->first),
      this->my_tree.comparator(merged_key, this->key_list.begin()->first));
  if (!this->my_tree.comparator(this->key_list.begin()->first, merged_key)) {
    // TODO should do CAS here
    this->leftmost_pointer = this->key_list.begin()->second;
  }
  return this->my_tree.table.Install(this->id, remove_index);
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
bool InternalBWNode<KeyType, ValueType, KeyComparator,
                    KeyEqualityChecker>::InternalUpdate(KeyType old_key,
                                                         KeyType new_key) {
  Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* node_pointer =
      this->my_tree.table.Get(this->id);
  UpdateDeltaNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
      update_node =
          new UpdateDeltaNode<KeyType, ValueType, KeyComparator,
                              KeyEqualityChecker>(this->my_tree, this->id);
  update_node->old_key = old_key;
  update_node->new_key = new_key;
  uint32_t chain_len = node_pointer->chain_len;
  update_node->chain_len = chain_len + 1;
  update_node->next = node_pointer;
  return this->my_tree.table.Install(this->id, update_node);
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Traverse() {
  auto root_node = table.Get(root);
  Traverse(root_node);
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Traverse(
    Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* n) {
  /*
   * Inorder traversal of the BW Tree for debugging purposes.
   */
  // TODO Add case for update node
  switch (n->type) {
    case INTERNAL_BW_NODE: {
      LOG_DEBUG("INTERNAL_BW_NODE id = %lu", n->id);
      InternalBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
          internal_pointer =
              dynamic_cast<InternalBWNode<KeyType, ValueType, KeyComparator,
                                          KeyEqualityChecker>*>(n);
      LOG_DEBUG("My id is %ld and my leftmost child is %ld",
                internal_pointer->id, internal_pointer->leftmost_pointer);
      Node<KeyType, ValueType, KeyComparator, KeyEqualityChecker>* leftmost =
          n->my_tree.table.Get(internal_pointer->leftmost_pointer);
      Traverse(leftmost);
      for (auto it = internal_pointer->key_list.begin();
           it != internal_pointer->key_list.end(); it++) {
        auto child = n->my_tree.table.Get(it->second);
        Traverse(child);
      }
    } break;
    case LEAF_BW_NODE: {
      LeafBWNode<KeyType, ValueType, KeyComparator, KeyEqualityChecker>*
          leaf_pointer =
              dynamic_cast<LeafBWNode<KeyType, ValueType, KeyComparator,
                                      KeyEqualityChecker>*>(n);
      LOG_DEBUG("LEAF_BW_NODE id = %lu | count = %lu", n->id,
                leaf_pointer->kv_list.size());
      for (auto it = leaf_pointer->kv_list.begin();
           it != leaf_pointer->kv_list.end(); it++) {
        // auto key = it->first;
        // auto value = it->second;
        // std::cout << "key = " << key << " | value = " << value << std::endl;
        // LOG_DEBUG("key = %s",
        // key.GetTupleForComparison(key.key_tuple_schema).GetData());
        LOG_DEBUG("key = bogus");
      }
    } break;
    case INSERT: {
      LOG_DEBUG("INSERT id = %lu", n->id);
      n = n->next;
    } break;
    case UPDATE: {
      LOG_DEBUG("UPDATE id = %lu", n->id);
      n = n->next;
    } break;
    case DELETE: {
      LOG_DEBUG("DELETE id = %lu", n->id);
      n = n->next;
    } break;
    case SPLIT: {
      LOG_DEBUG("SPLIT id = %lu", n->id);
      n = n->next;
    } break;
    case MERGE: {
      LOG_DEBUG("MERGE id = %lu", n->id);
      n = n->next;
    } break;
    case REMOVE: {
      LOG_DEBUG("REMOVE id = %lu", n->id);
    } break;
    case SPLIT_INDEX: {
      LOG_DEBUG("SPLIT_INDEX id = %lu", n->id);
      n = n->next;
    } break;
    case REMOVE_INDEX: {
      LOG_DEBUG("REMOVE_INDEX id = %lu", n->id);
      n = n->next;
    } break;
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
