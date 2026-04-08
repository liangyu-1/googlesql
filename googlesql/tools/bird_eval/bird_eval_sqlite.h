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

#ifndef GOOGLESQL_TOOLS_BIRD_EVAL_BIRD_EVAL_SQLITE_H_
#define GOOGLESQL_TOOLS_BIRD_EVAL_BIRD_EVAL_SQLITE_H_

#include <memory>
#include <string>
#include <vector>

#include "googlesql/public/simple_catalog.h"
#include "googlesql/public/types/type_factory.h"
#include "absl/status/statusor.h"

namespace googlesql::bird_eval {

struct SqliteQueryResult {
  std::vector<std::string> column_names;
  std::vector<std::vector<std::string>> normalized_rows;
};

struct SqliteCatalogContents {
  std::vector<std::unique_ptr<SimpleTable>> tables;
};

absl::StatusOr<SqliteQueryResult> ExecuteSqliteQuery(const std::string& db_path,
                                                     const std::string& sql);
absl::StatusOr<SqliteCatalogContents> BuildSimpleTablesFromSqlite(
    const std::string& db_path, TypeFactory* type_factory);

}  // namespace googlesql::bird_eval

#endif  // GOOGLESQL_TOOLS_BIRD_EVAL_BIRD_EVAL_SQLITE_H_
