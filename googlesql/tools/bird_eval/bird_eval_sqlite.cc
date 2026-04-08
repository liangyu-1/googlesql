//
// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "googlesql/tools/bird_eval/bird_eval_sqlite.h"

#include <sqlite3.h>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "googlesql/public/simple_catalog.h"
#include "googlesql/public/types/type.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/public/value.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/escaping.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "googlesql/base/status_macros.h"

namespace googlesql::bird_eval {
namespace {

class SqliteDb {
 public:
  static absl::StatusOr<SqliteDb> Open(const std::string& db_path) {
    sqlite3* db = nullptr;
    if (sqlite3_open(db_path.c_str(), &db) != SQLITE_OK) {
      std::string error = db == nullptr ? "unknown sqlite open error"
                                        : sqlite3_errmsg(db);
      if (db != nullptr) sqlite3_close(db);
      return absl::InvalidArgumentError(
          absl::StrCat("Failed to open SQLite DB ", db_path, ": ", error));
    }
    return SqliteDb(db);
  }

  SqliteDb(SqliteDb&& other) noexcept : db_(other.db_) { other.db_ = nullptr; }
  SqliteDb& operator=(SqliteDb&& other) noexcept {
    if (this != &other) {
      Close();
      db_ = other.db_;
      other.db_ = nullptr;
    }
    return *this;
  }

  ~SqliteDb() { Close(); }

  sqlite3* get() const { return db_; }

 private:
  explicit SqliteDb(sqlite3* db) : db_(db) {}

  void Close() {
    if (db_ != nullptr) {
      sqlite3_close(db_);
      db_ = nullptr;
    }
  }

  sqlite3* db_;
};

class Statement {
 public:
  static absl::StatusOr<Statement> Prepare(sqlite3* db, const std::string& sql) {
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
      return absl::InvalidArgumentError(
          absl::StrCat("SQLite prepare failed: ", sqlite3_errmsg(db)));
    }
    return Statement(stmt);
  }

  Statement(Statement&& other) noexcept : stmt_(other.stmt_) {
    other.stmt_ = nullptr;
  }
  Statement& operator=(Statement&& other) noexcept {
    if (this != &other) {
      Finalize();
      stmt_ = other.stmt_;
      other.stmt_ = nullptr;
    }
    return *this;
  }
  ~Statement() { Finalize(); }

  sqlite3_stmt* get() const { return stmt_; }

 private:
  explicit Statement(sqlite3_stmt* stmt) : stmt_(stmt) {}
  void Finalize() {
    if (stmt_ != nullptr) {
      sqlite3_finalize(stmt_);
      stmt_ = nullptr;
    }
  }

  sqlite3_stmt* stmt_;
};

std::string QuoteIdentifier(const std::string& identifier) {
  std::string quoted = "\"";
  for (char ch : identifier) {
    quoted.push_back(ch);
    if (ch == '"') quoted.push_back('"');
  }
  quoted.push_back('"');
  return quoted;
}

enum class ColumnMapping {
  kInt64,
  kDouble,
  kBool,
  kString,
  kBytes,
};

ColumnMapping MapDeclType(const std::string& decl_type) {
  const std::string normalized = absl::AsciiStrToUpper(decl_type);
  if (normalized.find("INT") != std::string::npos) {
    return ColumnMapping::kInt64;
  }
  if (normalized.find("DOUB") != std::string::npos ||
      normalized.find("FLOA") != std::string::npos ||
      normalized.find("REAL") != std::string::npos) {
    return ColumnMapping::kDouble;
  }
  if (normalized.find("BOOL") != std::string::npos) {
    return ColumnMapping::kBool;
  }
  if (normalized.find("BLOB") != std::string::npos) {
    return ColumnMapping::kBytes;
  }
  return ColumnMapping::kString;
}

const Type* GoogleSqlTypeForColumnMapping(ColumnMapping mapping,
                                          TypeFactory* type_factory) {
  switch (mapping) {
    case ColumnMapping::kInt64:
      return type_factory->get_int64();
    case ColumnMapping::kDouble:
      return type_factory->get_double();
    case ColumnMapping::kBool:
      return type_factory->get_bool();
    case ColumnMapping::kBytes:
      return type_factory->get_bytes();
    case ColumnMapping::kString:
      return type_factory->get_string();
  }
  return type_factory->get_string();
}

absl::StatusOr<Value> ReadTypedValue(sqlite3_stmt* stmt, int col_index,
                                     ColumnMapping mapping, const Type* type) {
  const int column_type = sqlite3_column_type(stmt, col_index);
  if (column_type == SQLITE_NULL) {
    return Value::Null(type);
  }
  switch (mapping) {
    case ColumnMapping::kInt64: {
      if (column_type == SQLITE_INTEGER) {
        return Value::Int64(sqlite3_column_int64(stmt, col_index));
      }
      if (column_type == SQLITE_FLOAT) {
        return Value::Int64(static_cast<int64_t>(
            sqlite3_column_double(stmt, col_index)));
      }
      int64_t value = 0;
      if (absl::SimpleAtoi(reinterpret_cast<const char*>(
                              sqlite3_column_text(stmt, col_index)),
                          &value)) {
        return Value::Int64(value);
      }
      return absl::InvalidArgumentError("Failed to coerce SQLite value to INT64");
    }
    case ColumnMapping::kDouble: {
      if (column_type == SQLITE_INTEGER || column_type == SQLITE_FLOAT) {
        return Value::Double(sqlite3_column_double(stmt, col_index));
      }
      double value = 0;
      if (absl::SimpleAtod(reinterpret_cast<const char*>(
                              sqlite3_column_text(stmt, col_index)),
                          &value)) {
        return Value::Double(value);
      }
      return absl::InvalidArgumentError(
          "Failed to coerce SQLite value to DOUBLE");
    }
    case ColumnMapping::kBool: {
      if (column_type == SQLITE_INTEGER || column_type == SQLITE_FLOAT) {
        return Value::Bool(sqlite3_column_int64(stmt, col_index) != 0);
      }
      const std::string text = reinterpret_cast<const char*>(
          sqlite3_column_text(stmt, col_index));
      if (absl::EqualsIgnoreCase(text, "true") || text == "1") {
        return Value::Bool(true);
      }
      if (absl::EqualsIgnoreCase(text, "false") || text == "0") {
        return Value::Bool(false);
      }
      return absl::InvalidArgumentError("Failed to coerce SQLite value to BOOL");
    }
    case ColumnMapping::kBytes: {
      const void* blob = sqlite3_column_blob(stmt, col_index);
      const int size = sqlite3_column_bytes(stmt, col_index);
      return Value::Bytes(absl::string_view(
          reinterpret_cast<const char*>(blob), size));
    }
    case ColumnMapping::kString: {
      const unsigned char* text = sqlite3_column_text(stmt, col_index);
      const int size = sqlite3_column_bytes(stmt, col_index);
      return Value::String(absl::string_view(
          reinterpret_cast<const char*>(text), size));
    }
  }
}

std::string NormalizeColumnValue(sqlite3_stmt* stmt, int col_index) {
  switch (sqlite3_column_type(stmt, col_index)) {
    case SQLITE_NULL:
      return "NULL";
    case SQLITE_INTEGER:
      return absl::StrCat("I:", sqlite3_column_int64(stmt, col_index));
    case SQLITE_FLOAT:
      return absl::StrFormat("F:%.17g", sqlite3_column_double(stmt, col_index));
    case SQLITE_TEXT: {
      const unsigned char* text = sqlite3_column_text(stmt, col_index);
      const int size = sqlite3_column_bytes(stmt, col_index);
      return absl::StrCat("S:", absl::CEscape(absl::string_view(
                                     reinterpret_cast<const char*>(text), size)));
    }
    case SQLITE_BLOB: {
      const void* blob = sqlite3_column_blob(stmt, col_index);
      const int size = sqlite3_column_bytes(stmt, col_index);
      return absl::StrCat("B:", absl::BytesToHexString(absl::string_view(
                                     reinterpret_cast<const char*>(blob), size)));
    }
  }
  return "UNKNOWN";
}

struct SqliteColumnSchema {
  std::string name;
  ColumnMapping mapping;
};

absl::StatusOr<std::vector<std::string>> ReadTableNames(sqlite3* db) {
  GOOGLESQL_ASSIGN_OR_RETURN(
      Statement stmt,
      Statement::Prepare(
          db,
          "SELECT name FROM sqlite_master WHERE type = 'table' "
          "AND name NOT LIKE 'sqlite_%' ORDER BY name"));
  std::vector<std::string> tables;
  while (sqlite3_step(stmt.get()) == SQLITE_ROW) {
    tables.emplace_back(reinterpret_cast<const char*>(
        sqlite3_column_text(stmt.get(), 0)));
  }
  return tables;
}

absl::StatusOr<std::vector<SqliteColumnSchema>> ReadTableSchema(sqlite3* db,
                                                                const std::string& table) {
  GOOGLESQL_ASSIGN_OR_RETURN(
      Statement stmt,
      Statement::Prepare(db, absl::StrCat("PRAGMA table_info(", QuoteIdentifier(table), ")")));
  std::vector<SqliteColumnSchema> columns;
  while (sqlite3_step(stmt.get()) == SQLITE_ROW) {
    const unsigned char* decl_type_text = sqlite3_column_text(stmt.get(), 2);
    columns.push_back(SqliteColumnSchema{
        .name = reinterpret_cast<const char*>(sqlite3_column_text(stmt.get(), 1)),
        .mapping = MapDeclType(decl_type_text == nullptr
                                   ? ""
                                   : reinterpret_cast<const char*>(decl_type_text)),
    });
  }
  if (columns.empty()) {
    return absl::InvalidArgumentError(
        absl::StrCat("No columns found for SQLite table ", table));
  }
  return columns;
}

}  // namespace

absl::StatusOr<SqliteQueryResult> ExecuteSqliteQuery(const std::string& db_path,
                                                     const std::string& sql) {
  GOOGLESQL_ASSIGN_OR_RETURN(SqliteDb db, SqliteDb::Open(db_path));
  GOOGLESQL_ASSIGN_OR_RETURN(Statement stmt, Statement::Prepare(db.get(), sql));

  SqliteQueryResult result;
  const int column_count = sqlite3_column_count(stmt.get());
  result.column_names.reserve(column_count);
  for (int i = 0; i < column_count; ++i) {
    const char* name = sqlite3_column_name(stmt.get(), i);
    result.column_names.push_back(name == nullptr ? "" : name);
  }

  while (true) {
    const int rc = sqlite3_step(stmt.get());
    if (rc == SQLITE_DONE) {
      break;
    }
    if (rc != SQLITE_ROW) {
      return absl::InvalidArgumentError(
          absl::StrCat("SQLite execution failed: ", sqlite3_errmsg(db.get())));
    }
    std::vector<std::string> row;
    row.reserve(column_count);
    for (int i = 0; i < column_count; ++i) {
      row.push_back(NormalizeColumnValue(stmt.get(), i));
    }
    result.normalized_rows.push_back(std::move(row));
  }
  return result;
}

absl::StatusOr<SqliteCatalogContents> BuildSimpleTablesFromSqlite(
    const std::string& db_path, TypeFactory* type_factory) {
  GOOGLESQL_ASSIGN_OR_RETURN(SqliteDb db, SqliteDb::Open(db_path));
  GOOGLESQL_ASSIGN_OR_RETURN(std::vector<std::string> tables, ReadTableNames(db.get()));

  SqliteCatalogContents contents;

  for (const std::string& table_name : tables) {
    GOOGLESQL_ASSIGN_OR_RETURN(std::vector<SqliteColumnSchema> schema,
                       ReadTableSchema(db.get(), table_name));
    std::vector<SimpleTable::NameAndType> fields;
    fields.reserve(schema.size());
    for (const SqliteColumnSchema& column : schema) {
      fields.push_back({column.name,
                        GoogleSqlTypeForColumnMapping(column.mapping, type_factory)});
    }

    auto table = std::make_unique<SimpleTable>(table_name, fields);

    GOOGLESQL_ASSIGN_OR_RETURN(
        Statement stmt,
        Statement::Prepare(
            db.get(), absl::StrCat("SELECT * FROM ", QuoteIdentifier(table_name))));

    std::vector<std::vector<Value>> rows;
    while (true) {
      const int rc = sqlite3_step(stmt.get());
      if (rc == SQLITE_DONE) {
        break;
      }
      if (rc != SQLITE_ROW) {
        return absl::InvalidArgumentError(
            absl::StrCat("SQLite table read failed: ", sqlite3_errmsg(db.get())));
      }

      std::vector<Value> cells;
      cells.reserve(schema.size());
      for (int i = 0; i < schema.size(); ++i) {
        GOOGLESQL_ASSIGN_OR_RETURN(
            Value value,
            ReadTypedValue(stmt.get(), i, schema[i].mapping, fields[i].second));
        cells.push_back(std::move(value));
      }
      rows.push_back(std::move(cells));
    }

    table->SetContents(rows);
    contents.tables.push_back(std::move(table));
  }

  return contents;
}

}  // namespace googlesql::bird_eval
