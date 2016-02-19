/*-------------------------------------------------------------------------
 *
 * hash_join.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/executor/hash_join_executor.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <vector>

#include "backend/common/types.h"
#include "backend/common/logger.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/hash_join_executor.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/container_tuple.h"

using namespace std;

namespace peloton {
namespace executor {

/**
 * @brief Constructor for hash join executor.
 * @param node Hash join node corresponding to this executor.
 */
HashJoinExecutor::HashJoinExecutor(const planner::AbstractPlan *node,
                                   ExecutorContext *executor_context)
    : AbstractJoinExecutor(node, executor_context) {}

bool HashJoinExecutor::DInit() {
  assert(children_.size() == 2);

  auto status = AbstractJoinExecutor::DInit();
  if (status == false) return status;

  assert(children_[1]->GetRawNode()->GetPlanNodeType() == PLAN_NODE_TYPE_HASH);

  hash_executor_ = reinterpret_cast<HashExecutor *>(children_[1]);

  return true;
}

/**
 * @brief Creates logical tiles from the two input logical tiles after applying
 * join predicate.
 * @return true on success, false otherwise.
 */
bool HashJoinExecutor::DExecute() {
  LOG_DEBUG("********** Hash %s Join executor :: 2 children \n",
            GetJoinTypeString());

  // Loop until we have non-empty result join logical tile or exit
  for (;;) {
    // Build outer join output when done
    if (right_child_done_ && left_child_done_ &&
        buffered_output_tiles.size() == 0) {
      LOG_DEBUG("Left child and right child are already done");
      return BuildOuterJoinOutput();
    }
    //===--------------------------------------------------------------------===//
    // Pick right and left tiles
    //===--------------------------------------------------------------------===//
    // Get all the logical tiles from RIGHT child after constructing
    // the hash table
    while (!right_child_done_ && hash_executor_->Execute()) {
      LOG_DEBUG("hash_executor_ has not finished");
      BufferRightTile(hash_executor_->GetOutput());
    }
    right_child_done_ = true;
    if (right_result_tiles_.empty()) {
      LOG_DEBUG("Right child returned nothing. Exit.");
      return false;
    } else {
      LOG_DEBUG("Get all the logical tiles from RIGHT child done.");
    }

    // Get next logical tile from LEFT child
    while (!left_child_done_ && children_[0]->Execute()) {
      LOG_DEBUG("Executing left child is not finished.");
      BufferLeftTile(children_[0]->GetOutput());
    }
    if (left_result_tiles_.empty()) {
      LOG_DEBUG("Left child returned nothing. Exit.");
      return false;
    } else {
      LOG_DEBUG("Get all the logical tiles from LEFT child done.");
    }
    left_child_done_ = true;

    if (buffered_output_tiles.size() != 0) {
      LogicalTile *opt = buffered_output_tiles.front();
      buffered_output_tiles.pop_front();
      SetOutput(opt);
      return true;
    }
    //===--------------------------------------------------------------------===//
    // Build Join Tile
    //===--------------------------------------------------------------------===//
    // assert(left_tile != nullptr);
    // assert(right_tile != nullptr);
    // Get the hash table from the hash executor
    HashExecutor::HashMapType &hash_table = hash_executor_->GetHashTable();
    const vector<oid_t> &hash_key_ids = hash_executor_->GetHashKeyIds();
    size_t index = 0;
    for (index = 0; index < left_result_tiles_.size(); ++index) {
      LogicalTile *left_tile = left_result_tiles_[index].get();
      for (auto left_tile_row_itr : *left_tile) {
        // For each tuple, find matching tuples in the hash table built on top
        // of
        // the
        // right table
        expression::ContainerTuple<executor::LogicalTile> left_tuple(
            left_tile, left_tile_row_itr, &hash_key_ids);
        HashExecutor::HashMapType::const_iterator get =
            hash_table.find(left_tuple);
        if (get != hash_table.end()) {
          LOG_DEBUG("Get a set for this tuple");
          unordered_set<pair<size_t, oid_t>, boost::hash<pair<size_t, oid_t>>>
              tuples_set = get->second;
          unordered_set<pair<size_t, oid_t>,
                        boost::hash<pair<size_t, oid_t>>>::const_iterator it;
          // Go over the matching right tuples
          for (it = tuples_set.begin(); it != tuples_set.end(); ++it) {
            bool has_right_match = false;
            pair<size_t, oid_t> pair = *it;
            size_t right_result_itr_ = pair.first;
            oid_t right_tile_row_itr = pair.second;
            LogicalTile *right_tile =
                right_result_tiles_[right_result_itr_].get();
            // Build output join logical tile
            auto output_tile = BuildOutputLogicalTile(left_tile, right_tile);
            LogicalTile::PositionListsBuilder pos_lists_builder(left_tile,
                                                                right_tile);
            if (predicate_ != nullptr) {
              expression::ContainerTuple<executor::LogicalTile> right_tuple(
                  right_tile, right_tile_row_itr, &hash_key_ids);
              // Join predicate is false. Skip pair and continue.
              if (predicate_->Evaluate(&left_tuple, &right_tuple,
                                       executor_context_).IsFalse()) {
                continue;
              }
              LOG_DEBUG("Found a matched pair");
              RecordMatchedRightRow(right_result_itr_, right_tile_row_itr);

              // For Left and Full Outer Join
              has_right_match = true;
              pos_lists_builder.AddRow(left_tile_row_itr, right_tile_row_itr);

              // Check if we have any join tuples.
              if (pos_lists_builder.Size() > 0) {
                output_tile->SetPositionListsAndVisibility(
                    pos_lists_builder.Release());
                buffered_output_tiles.push_back(output_tile.release());
              }
            }
            // For Left and Full Outer Join
            if (has_right_match) {
              RecordMatchedLeftRow(index, left_tile_row_itr);
            }
          }
        }
      }
    }

    LOG_DEBUG("This pair produces empty join result. Continue the loop.");
  }
}

}  // namespace executor
}  // namespace peloton
