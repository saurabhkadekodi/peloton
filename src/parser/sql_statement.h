/*
 * SQLStatement.h
 * Definition of the structure used to build the syntax tree.
 */

#pragma once

#include <vector>
#include <iostream>

#include "common/types.h"
#include "expr.h"

namespace nstore {
namespace parser {

// Base class for every SQLStatement
class SQLStatement {
 public:

  SQLStatement(StatementType type) :
    stmt_type(type) {};

  virtual ~SQLStatement() {}

  virtual StatementType GetType() {
    return stmt_type;
  }

  // Get a string representation of this statement
  friend std::ostream& operator<<(std::ostream& os, const SQLStatement& stmt);

 private:
  StatementType stmt_type;
};

// Represents the result of the SQLParser.
// If parsing was successful it is a list of SQLStatement.
class SQLStatementList {
 public:

  SQLStatementList() :
    is_valid(true),
    parser_msg(NULL),
    error_line(0),
    error_col(0){
  };

  SQLStatementList(SQLStatement* stmt) :
    is_valid(true),
    parser_msg(NULL) {
    addStatement(stmt);
  };

  virtual ~SQLStatementList() {

    // clean up statements
    for(auto stmt : statements)
      delete stmt;

    delete parser_msg;
  }

  void addStatement(SQLStatement* stmt) {
    statements.push_back(stmt);
  }

  SQLStatement* getStatement(int id) {
    return statements[id];
  }

  size_t numStatements() {
    return statements.size();
  }

  // Get a string representation of this statement list
  friend std::ostream& operator<<(std::ostream& os, const SQLStatementList& stmt_list);

  std::vector<SQLStatement*> statements;
  bool is_valid;
  const char* parser_msg;
  int error_line;
  int error_col;
};


} // End parser namespace
} // End nstore namespace

