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
  printf("building index...\n");
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
  printf("built index...\n");
  EXPECT_TRUE(index != NULL);

  return index;
}

#if 0
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
#endif
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
    // key0->SetValue(0, ValueFactory::GetIntegerValue(100), pool);
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

//#if 0
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
//#if 0
TEST(IndexTests, DeleteTest) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer> locations;

  // INDEX
  std::unique_ptr<index::Index> index(BuildIndex());

  // Single threaded test
  size_t scale_factor = 1;
  LaunchParallelTest(1, InsertTest, index.get(), pool, scale_factor);
  printf("####### ABOUT TO DELETE #########\n");
  LaunchParallelTest(1, DeleteTest, index.get(), pool, scale_factor);
  printf("####### ABOUT TO CHECK #########\n");

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

  printf("####### ABOUT TO SEARCH 0 #########\n");
  locations = index->ScanKey(key0.get());
  EXPECT_EQ(locations.size(), 0);
  //EXPECT_EQ(locations.size(), 1);

  printf("####### ABOUT TO SEARCH 1 #########\n");
  locations = index->ScanKey(key1.get());
  EXPECT_EQ(locations.size(), 2);
  //EXPECT_EQ(locations.size(), 5);

  printf("####### ABOUT TO SEARCH 2 #########\n");
  locations = index->ScanKey(key2.get());
  EXPECT_EQ(locations.size(), 1);
  EXPECT_EQ(locations[0].block, item1.block);

  delete tuple_schema;
}
//#endif

/**
 * We need tests for:
 * 1. Insert single key and value - done (SimpleInsertSingleThreaded)
 * 2. Delete single key - done (SimpleDeleteSingleThreaded)
 * 3. Insert same key-value twice - done (InsertIllegalSingleThreaded)
 * 4. deleting unadded key - done (DeleteIllegalSingleThreaded)
 * 5. search unadded key - done (SearchIllegalSingleThreaded)
 * 6. adding multiple values for a single key - done
 * (MultipleValuesSingleThreaded)
 * 7. testing the consolidation limit (dependent on policy)
 * 8. scanning the whole tree - single threaded - done (ScanTreeSingleThreaded)
 * 9. deleting the root (do we need this? we delete in each test)
 * 10. delete the whole tree - single threaded - done (DeleteTreeSingleThreaded)
 * 11. merge nodes (dependent on max and min node sizes)
 * 12. test epoch based GC (dependent on epoch triggering GC)
 * 13. Multithreaded insert test - already given (MultiThreadedInsertTest)
 */

//#if 0
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
//#endif
//#if 0
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
  printf("####### Key 0 inserted once\n");
  assert(index->InsertEntry(key0.get(), item0) == true);
  printf("####### Key 0 inserted twice (duplicate)\n");
  assert(index->InsertEntry(key0.get(), item1) == true);
  printf("####### Key 0 inserted third time (unique)\n");
  //  assert(index->InsertEntry(key0.get(), item1) == false); // don't know if
  //  this is correct

  // SEARCH
  locations = index->ScanKey(key0.get());
  EXPECT_EQ(locations.size(), 3);

  delete tuple_schema;
}
//#endif
//#if 0
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

//#if 0
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

//#if 0
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
//#endif
//#if 0
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
//#endif
//#if 0
TEST(IndexTests, DeleteTreeSingleThreaded) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::vector<ItemPointer> locations;

  // INDEX
  std::unique_ptr<index::Index> index(BuildIndex());
  printf("finished building index...\n");
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
  //index->GetMemoryFootprint();
  assert(index->InsertEntry(key5.get(), item0) == true);
  //index->GetMemoryFootprint();
  assert(index->InsertEntry(key4.get(), item0) == true);
  assert(index->InsertEntry(key3.get(), item0) == true);
  assert(index->InsertEntry(key2.get(), item0) == true);
  assert(index->InsertEntry(key1.get(), item0) == true);
  assert(index->InsertEntry(key0.get(), item0) == true);
  printf("All the inserts were successful. Now deleting\n");
  //index->Traverse();
  // DELETE
  assert(index->DeleteEntry(key1.get(), item0) == true);
  printf("Deleted 1\n");
  fflush(stdout);
  assert(index->DeleteEntry(key10.get(), item0) == true);
  printf("\t\t####### Deleted 10\n");
  assert(index->DeleteEntry(key9.get(), item0) == true);
  printf("\t\t####### Deleted 9\n");
  assert(index->DeleteEntry(key8.get(), item0) == true);
  printf("\t\t####### Deleted 8\n");
  assert(index->DeleteEntry(key7.get(), item0) == true);
  printf("\t\t####### Deleted 7\n");
  assert(index->DeleteEntry(key6.get(), item0) == true);
  printf("\t\t####### Deleted 6\n");
  assert(index->DeleteEntry(key5.get(), item0) == true);
  printf("\t\t####### Deleted 5\n");
  assert(index->DeleteEntry(key4.get(), item0) == true);
  printf("\t\t####### Deleted 4\n");
  assert(index->DeleteEntry(key3.get(), item0) == true);
  printf("\t\t####### Deleted 3\n");
  assert(index->DeleteEntry(key2.get(), item0) == true);
  printf("\t\t####### Deleted 2\n");
  assert(index->DeleteEntry(key0.get(), item0) == true);
  printf("\t\t####### Deleted 0\n");
  locations = index->ScanKey(key0.get());
  EXPECT_EQ(locations.size(), 0);
  delete tuple_schema;
}
//#endif
//#if 0
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
  std::unique_ptr<storage::Tuple> keynonce(new storage::Tuple(key_schema,
  true));

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
//#endif
}  // End test namespace
}  // End peloton namespace
