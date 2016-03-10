//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// index_test.cpp
//
// Identification: tests/index/index_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "gtest/gtest.h"
#include "harness.h"

#include "backend/common/logger.h"
#include "backend/index/index_factory.h"
#include "backend/storage/tuple.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Index Tests
//===--------------------------------------------------------------------===//

catalog::Schema *key_schema = nullptr;
catalog::Schema *tuple_schema = nullptr;

ItemPointer item0(120, 5);
ItemPointer item1(120, 7);
ItemPointer item2(123, 19);

index::Index *BuildIndex() {
  // Build tuple and key schema
  std::vector<std::vector<std::string>> column_names;
  std::vector<catalog::Column> columns;
  std::vector<catalog::Schema *> schemas;
  IndexType index_type = INDEX_TYPE_BTREE;
  // TODO: Uncomment the line below
  index_type = INDEX_TYPE_BWTREE;

  catalog::Column column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "A", true);
  catalog::Column column2(VALUE_TYPE_VARCHAR, 1024, "B", true);
  catalog::Column column3(VALUE_TYPE_DOUBLE, GetTypeSize(VALUE_TYPE_DOUBLE),
                          "C", true);
  catalog::Column column4(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "D", true);

  columns.push_back(column1);
  columns.push_back(column2);

  // INDEX KEY SCHEMA -- {column1, column2}
  key_schema = new catalog::Schema(columns);
  key_schema->SetIndexedColumns({0, 1});

  columns.push_back(column3);
  columns.push_back(column4);

  // TABLE SCHEMA -- {column1, column2, column3, column4}
  tuple_schema = new catalog::Schema(columns);

  // Build index metadata
  const bool unique_keys = false;

  index::IndexMetadata *index_metadata = new index::IndexMetadata(
      "test_index", 125, index_type, INDEX_CONSTRAINT_TYPE_DEFAULT,
      tuple_schema, key_schema, unique_keys);

  // Build index
  index::Index *index = index::IndexFactory::GetInstance(index_metadata);
  EXPECT_TRUE(index != NULL);

  return index;
}

// INSERT HELPER FUNCTION
void InsertTest(index::Index *index, VarlenPool *pool, size_t scale_factor) {
  // Loop based on scale factor
  for (size_t scale_itr = 1; scale_itr <= scale_factor; scale_itr++) {
    // Insert a bunch of keys based on scale itr
    std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
    std::unique_ptr<storage::Tuple> key1(new storage::Tuple(key_schema, true));
    std::unique_ptr<storage::Tuple> key2(new storage::Tuple(key_schema, true));
    std::unique_ptr<storage::Tuple> key3(new storage::Tuple(key_schema, true));
    std::unique_ptr<storage::Tuple> key4(new storage::Tuple(key_schema, true));
    std::unique_ptr<storage::Tuple> keynonce(
        new storage::Tuple(key_schema, true));

    key0->SetValue(0, ValueFactory::GetIntegerValue(100 * scale_itr), pool);
    key0->SetValue(1, ValueFactory::GetStringValue("a"), pool);
    key1->SetValue(0, ValueFactory::GetIntegerValue(100 * scale_itr), pool);
    key1->SetValue(1, ValueFactory::GetStringValue("b"), pool);
    key2->SetValue(0, ValueFactory::GetIntegerValue(100 * scale_itr), pool);
    key2->SetValue(1, ValueFactory::GetStringValue("c"), pool);
    key3->SetValue(0, ValueFactory::GetIntegerValue(400 * scale_itr), pool);
    key3->SetValue(1, ValueFactory::GetStringValue("d"), pool);
    key4->SetValue(0, ValueFactory::GetIntegerValue(500 * scale_itr), pool);
    key4->SetValue(1, ValueFactory::GetStringValue(
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
                   pool);
    keynonce->SetValue(0, ValueFactory::GetIntegerValue(1000 * scale_itr),
                       pool);
    keynonce->SetValue(1, ValueFactory::GetStringValue("f"), pool);

    // INSERT
    assert(index->InsertEntry(key0.get(), item0));
    assert(index->InsertEntry(key1.get(), item1));
    assert(index->InsertEntry(key1.get(), item2));
    assert(index->InsertEntry(key1.get(), item1));
    assert(index->InsertEntry(key1.get(), item1));
    assert(index->InsertEntry(key1.get(), item0));

    assert(index->InsertEntry(key2.get(), item1));
    assert(index->InsertEntry(key3.get(), item1));
    assert(index->InsertEntry(key4.get(), item1));
  }
}

// DELETE HELPER FUNCTION
void DeleteTest(index::Index *index, VarlenPool *pool, size_t scale_factor) {
  // Loop based on scale factor
  for (size_t scale_itr = 1; scale_itr <= scale_factor; scale_itr++) {
    // Delete a bunch of keys based on scale itr
    std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
    std::unique_ptr<storage::Tuple> key1(new storage::Tuple(key_schema, true));
    std::unique_ptr<storage::Tuple> key2(new storage::Tuple(key_schema, true));
    std::unique_ptr<storage::Tuple> key3(new storage::Tuple(key_schema, true));
    std::unique_ptr<storage::Tuple> key4(new storage::Tuple(key_schema, true));

    key0->SetValue(0, ValueFactory::GetIntegerValue(100 * scale_itr), pool);
    key0->SetValue(1, ValueFactory::GetStringValue("a"), pool);
    key1->SetValue(0, ValueFactory::GetIntegerValue(100 * scale_itr), pool);
    key1->SetValue(1, ValueFactory::GetStringValue("b"), pool);
    key2->SetValue(0, ValueFactory::GetIntegerValue(100 * scale_itr), pool);
    key2->SetValue(1, ValueFactory::GetStringValue("c"), pool);
    key3->SetValue(0, ValueFactory::GetIntegerValue(400 * scale_itr), pool);
    key3->SetValue(1, ValueFactory::GetStringValue("d"), pool);
    key4->SetValue(0, ValueFactory::GetIntegerValue(500 * scale_itr), pool);
    key4->SetValue(1, ValueFactory::GetStringValue(
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                          "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
                   pool);

    // DELETE
    index->DeleteEntry(key0.get(), item0);
    index->DeleteEntry(key1.get(), item1);
    index->DeleteEntry(key2.get(), item2);
    index->DeleteEntry(key3.get(), item1);
    index->DeleteEntry(key4.get(), item1);
  }
}

void SimpleInsert(index::Index *index, VarlenPool *pool,
                  size_t scale_factor __attribute__((unused))) {
  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key1(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key2(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key3(new storage::Tuple(key_schema, true));

  key0->SetValue(0, ValueFactory::GetIntegerValue(0), pool);
  key1->SetValue(0, ValueFactory::GetIntegerValue(1), pool);
  key2->SetValue(0, ValueFactory::GetIntegerValue(2), pool);
  key3->SetValue(0, ValueFactory::GetIntegerValue(3), pool);

  // INSERT
  assert(index->InsertEntry(key0.get(), item0) == true);
  assert(index->InsertEntry(key1.get(), item0) == true);
  assert(index->InsertEntry(key2.get(), item0) == true);
  assert(index->InsertEntry(key3.get(), item0) == true);
}

// already provided
TEST(IndexTests, BasicTest) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer> locations;

  // INDEX
  std::unique_ptr<index::Index> index(BuildIndex());

  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));

  key0->SetValue(0, ValueFactory::GetIntegerValue(100), pool);

  key0->SetValue(1, ValueFactory::GetStringValue("a"), pool);

  // INSERT
  index->InsertEntry(key0.get(), item0);

  locations = index->ScanKey(key0.get());
  EXPECT_EQ(locations.size(), 1);
  EXPECT_EQ(locations[0].block, item0.block);

  // DELETE
  index->DeleteEntry(key0.get(), item0);

  locations = index->ScanKey(key0.get());
  EXPECT_EQ(locations.size(), 0);

  delete tuple_schema;
}

// already provided
TEST(IndexTests, DeleteTest) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer> locations;

  // INDEX
  std::unique_ptr<index::Index> index(BuildIndex());

  // Single threaded test
  size_t scale_factor = 1;
  LaunchParallelTest(1, InsertTest, index.get(), pool, scale_factor);
  LaunchParallelTest(1, DeleteTest, index.get(), pool, scale_factor);

  // Checks
  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key1(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key2(new storage::Tuple(key_schema, true));

  key0->SetValue(0, ValueFactory::GetIntegerValue(100), pool);
  key0->SetValue(1, ValueFactory::GetStringValue("a"), pool);
  key1->SetValue(0, ValueFactory::GetIntegerValue(100), pool);
  key1->SetValue(1, ValueFactory::GetStringValue("b"), pool);
  key2->SetValue(0, ValueFactory::GetIntegerValue(100), pool);
  key2->SetValue(1, ValueFactory::GetStringValue("c"), pool);

  locations = index->ScanKey(key0.get());
  EXPECT_EQ(locations.size(), 0);

  locations = index->ScanKey(key1.get());
  EXPECT_EQ(locations.size(), 2);

  locations = index->ScanKey(key2.get());
  EXPECT_EQ(locations.size(), 1);
  EXPECT_EQ(locations[0].block, item1.block);

  delete tuple_schema;
}

// unit test for insertion
TEST(IndexTests, SimpleInsertSingleThreaded) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer> locations;

  // INDEX
  std::unique_ptr<index::Index> index(BuildIndex());

  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));

  key0->SetValue(0, ValueFactory::GetIntegerValue(0), pool);

  // INSERT
  assert(index->InsertEntry(key0.get(), item0) == true);

  // SEARCH
  locations = index->ScanKey(key0.get());
  EXPECT_EQ(locations.size(), 1);
  EXPECT_EQ(locations[0].block, item0.block);

  delete tuple_schema;
}

// unit test for deletion
TEST(IndexTests, SimpleDeleteSingleThreaded) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer> locations;

  // INDEX
  std::unique_ptr<index::Index> index(BuildIndex());

  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));

  key0->SetValue(0, ValueFactory::GetIntegerValue(0), pool);

  // INSERT
  assert(index->InsertEntry(key0.get(), item0) == true);

  // DELETE
  assert(index->DeleteEntry(key0.get(), item0) == true);

  // SEARCH
  locations = index->ScanKey(key0.get());
  EXPECT_EQ(locations.size(), 0);

  delete tuple_schema;
}

// unit test to check illegal insertion
TEST(IndexTests, InsertIllegalSingleThreaded) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer> locations;

  // INDEX
  std::unique_ptr<index::Index> index(BuildIndex());

  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));

  key0->SetValue(0, ValueFactory::GetIntegerValue(0), pool);
  key0->SetValue(1, ValueFactory::GetIntegerValue(10), pool);

  // INSERT
  assert(index->InsertEntry(key0.get(), item0) == true);
  assert(index->InsertEntry(key0.get(), item0) == true);
  assert(index->InsertEntry(key0.get(), item1) == true);

  // SEARCH
  locations = index->ScanKey(key0.get());
  EXPECT_EQ(locations.size(), 3);

  delete tuple_schema;
}

// unit test to check illegal deletion
TEST(IndexTests, DeleteIllegalSingleThreaded) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer> locations;

  // INDEX
  std::unique_ptr<index::Index> index(BuildIndex());

  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key1(new storage::Tuple(key_schema, true));

  key0->SetValue(0, ValueFactory::GetIntegerValue(0), pool);

  key0->SetValue(0, ValueFactory::GetIntegerValue(10), pool);
  // INSERT
  assert(index->InsertEntry(key0.get(), item0) == true);

  // DELETE
  assert(index->DeleteEntry(key1.get(), item0) == false);

  delete tuple_schema;
}

// unit test to search illegal entry
TEST(IndexTests, SearchIllegalSingleThreaded) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer> locations;

  // INDEX
  std::unique_ptr<index::Index> index(BuildIndex());

  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key1(new storage::Tuple(key_schema, true));

  key0->SetValue(0, ValueFactory::GetIntegerValue(0), pool);
  key0->SetValue(0, ValueFactory::GetIntegerValue(10), pool);

  // INSERT
  assert(index->InsertEntry(key0.get(), item0) == true);

  // SEARCH
  locations = index->ScanKey(key1.get());
  EXPECT_EQ(locations.size(), 0);

  delete tuple_schema;
}

// unit test for multiple values
TEST(IndexTests, MultipleValuesSingleThreaded) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer> locations;

  // INDEX
  std::unique_ptr<index::Index> index(BuildIndex());

  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));

  key0->SetValue(0, ValueFactory::GetIntegerValue(0), pool);

  // INSERT
  assert(index->InsertEntry(key0.get(), item0) == true);
  assert(index->InsertEntry(key0.get(), item1) == true);

  // SEARCH
  locations = index->ScanKey(key0.get());
  EXPECT_EQ(locations.size(), 2);
  EXPECT_EQ(locations[0].block, item0.block);
  EXPECT_EQ(locations[1].block, item1.block);
  delete tuple_schema;
}

// unit test to scan tree
TEST(IndexTests, ScanTreeSingleThreaded) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer> locations;

  // INDEX
  std::unique_ptr<index::Index> index(BuildIndex());

  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key1(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key2(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key3(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key4(new storage::Tuple(key_schema, true));

  key0->SetValue(0, ValueFactory::GetIntegerValue(0), pool);
  key1->SetValue(0, ValueFactory::GetIntegerValue(1), pool);
  key2->SetValue(0, ValueFactory::GetIntegerValue(2), pool);
  key3->SetValue(0, ValueFactory::GetIntegerValue(3), pool);
  key4->SetValue(0, ValueFactory::GetIntegerValue(4), pool);

  // INSERT
  assert(index->InsertEntry(key0.get(), item0) == true);
  assert(index->InsertEntry(key1.get(), item0) == true);
  assert(index->InsertEntry(key2.get(), item0) == true);
  assert(index->InsertEntry(key3.get(), item0) == true);
  assert(index->InsertEntry(key4.get(), item0) == true);

  locations = index->ScanAllKeys();
  EXPECT_EQ(locations.size(), 5);
  EXPECT_EQ(locations[0].block, item0.block);
  EXPECT_EQ(locations[1].block, item0.block);
  EXPECT_EQ(locations[2].block, item0.block);
  EXPECT_EQ(locations[3].block, item0.block);
  EXPECT_EQ(locations[4].block, item0.block);

  delete tuple_schema;
}

// delete whole tree in reverse
TEST(IndexTests, DeleteTreeSingleThreaded) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer> locations;

  // INDEX
  std::unique_ptr<index::Index> index(BuildIndex());
  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key1(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key2(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key3(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key4(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key5(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key6(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key7(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key8(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key9(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key10(new storage::Tuple(key_schema, true));

  key0->SetValue(0, ValueFactory::GetIntegerValue(0), pool);
  key1->SetValue(0, ValueFactory::GetIntegerValue(1), pool);
  key2->SetValue(0, ValueFactory::GetIntegerValue(2), pool);
  key3->SetValue(0, ValueFactory::GetIntegerValue(3), pool);
  key4->SetValue(0, ValueFactory::GetIntegerValue(4), pool);

  key5->SetValue(0, ValueFactory::GetIntegerValue(5), pool);
  key6->SetValue(0, ValueFactory::GetIntegerValue(6), pool);
  key7->SetValue(0, ValueFactory::GetIntegerValue(7), pool);
  key8->SetValue(0, ValueFactory::GetIntegerValue(8), pool);
  key9->SetValue(0, ValueFactory::GetIntegerValue(9), pool);
  key10->SetValue(0, ValueFactory::GetIntegerValue(10), pool);

  // INSERT
  assert(index->InsertEntry(key10.get(), item0) == true);
  assert(index->InsertEntry(key9.get(), item0) == true);
  assert(index->InsertEntry(key8.get(), item0) == true);
  assert(index->InsertEntry(key7.get(), item0) == true);
  assert(index->InsertEntry(key6.get(), item0) == true);
  assert(index->InsertEntry(key5.get(), item0) == true);
  assert(index->InsertEntry(key4.get(), item0) == true);
  assert(index->InsertEntry(key3.get(), item0) == true);
  assert(index->InsertEntry(key2.get(), item0) == true);
  assert(index->InsertEntry(key1.get(), item0) == true);
  assert(index->InsertEntry(key0.get(), item0) == true);
  assert(index->DeleteEntry(key1.get(), item0) == true);

  assert(index->DeleteEntry(key10.get(), item0) == true);
  assert(index->DeleteEntry(key9.get(), item0) == true);
  assert(index->DeleteEntry(key8.get(), item0) == true);
  assert(index->DeleteEntry(key7.get(), item0) == true);
  assert(index->DeleteEntry(key6.get(), item0) == true);
  assert(index->DeleteEntry(key5.get(), item0) == true);
  assert(index->DeleteEntry(key4.get(), item0) == true);
  assert(index->DeleteEntry(key3.get(), item0) == true);
  assert(index->DeleteEntry(key2.get(), item0) == true);
  assert(index->DeleteEntry(key0.get(), item0) == true);

  locations = index->ScanKey(key0.get());
  EXPECT_EQ(locations.size(), 0);
  delete tuple_schema;
}

// simple multi-threaded test
TEST(IndexTests, SimpleMultiThreadedTest) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer> locations;

  // INDEX
  std::unique_ptr<index::Index> index(BuildIndex());

  // Parallel Test
  size_t num_threads = 20;
  size_t scale_factor = 1;
  LaunchParallelTest(num_threads, SimpleInsert, index.get(), pool,
                     scale_factor);

  locations = index->ScanAllKeys();
  EXPECT_EQ(locations.size(), 4 * num_threads);

  delete tuple_schema;
}

// already provided
TEST(IndexTests, MultiThreadedInsertTest) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer> locations;

  // INDEX
  std::unique_ptr<index::Index> index(BuildIndex());

  // Parallel Test
  size_t num_threads = 3;
  size_t scale_factor = 1;
  LaunchParallelTest(num_threads, InsertTest, index.get(), pool, scale_factor);

  locations = index->ScanAllKeys();
  EXPECT_EQ(locations.size(), 9 * num_threads);

  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> keynonce(
      new storage::Tuple(key_schema, true));

  keynonce->SetValue(0, ValueFactory::GetIntegerValue(1000), pool);
  keynonce->SetValue(1, ValueFactory::GetStringValue("f"), pool);

  key0->SetValue(0, ValueFactory::GetIntegerValue(100), pool);
  key0->SetValue(1, ValueFactory::GetStringValue("a"), pool);

  locations = index->ScanKey(keynonce.get());
  EXPECT_EQ(locations.size(), 0);

  locations = index->ScanKey(key0.get());
  EXPECT_EQ(locations.size(), num_threads);
  EXPECT_EQ(locations[0].block, item0.block);

  delete tuple_schema;
}

}  // End test namespace
}  // End peloton namespace
