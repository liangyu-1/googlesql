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

#include "googlesql/tools/bird_eval/bird_eval_runner.h"

#include <sqlite3.h>

#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <string>

#include "googlesql/base/file_util.h"
#include "googlesql/base/path.h"
#include "googlesql/base/status_macros.h"
#include "googlesql/base/testing/status_matchers.h"
#include "absl/strings/str_cat.h"
#include "gtest/gtest.h"

namespace googlesql::bird_eval {
namespace {

void ExecOrDie(sqlite3* db, const std::string& sql) {
  char* errmsg = nullptr;
  ASSERT_EQ(sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &errmsg), SQLITE_OK)
      << (errmsg == nullptr ? "" : errmsg);
  if (errmsg != nullptr) sqlite3_free(errmsg);
}

TEST(BirdEvalRunnerTest, RunsEndToEndWithRewrite) {
  const char* tmpdir = getenv("TEST_TMPDIR");
  ASSERT_NE(tmpdir, nullptr);
  const std::string root = googlesql_base::JoinPath(tmpdir, "bird_eval_test");
  ASSERT_TRUE(mkdir(root.c_str(), 0755) == 0 || errno == EEXIST);

  const std::string db_dir = googlesql_base::JoinPath(root, "concert");
  ASSERT_TRUE(mkdir(db_dir.c_str(), 0755) == 0 || errno == EEXIST);
  const std::string db_path = googlesql_base::JoinPath(db_dir, "concert.sqlite");

  sqlite3* db = nullptr;
  ASSERT_EQ(sqlite3_open(db_path.c_str(), &db), SQLITE_OK);
  ExecOrDie(db,
            "CREATE TABLE singer (singer_id INTEGER PRIMARY KEY, name TEXT);");
  ExecOrDie(db,
            "INSERT INTO singer VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Cara');");
  ASSERT_EQ(sqlite3_close(db), SQLITE_OK);

  const std::string bird_json = absl::StrCat(
      "[{\"db_id\":\"concert\",\"question\":\"second singer?\","
      "\"SQL\":\"SELECT name FROM singer ORDER BY singer_id LIMIT 1, 1\"}]");
  const std::string bird_path = googlesql_base::JoinPath(root, "mini.json");
  ASSERT_TRUE(
      googlesql_base::SetContents(bird_path, bird_json, googlesql_base::Defaults())
          .ok());

  BirdEvalOptions options;
  options.bird_json_path = bird_path;
  options.sqlite_db_root = root;

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(BirdEvalSummary summary, RunBirdEvaluation(options));
  ASSERT_EQ(summary.total, 1);
  EXPECT_EQ(summary.parse_ok, 1);
  EXPECT_EQ(summary.analyze_ok, 1);
  EXPECT_EQ(summary.sqlite_execute_ok, 1);
  EXPECT_EQ(summary.googlesql_execute_ok, 1);
  EXPECT_EQ(summary.matched, 1);
  ASSERT_EQ(summary.sample_results.size(), 1);
  EXPECT_THAT(summary.sample_results[0].rewrite_rules,
              ::testing::ElementsAre("limit_offset_comma_syntax"));
  EXPECT_EQ(summary.sample_results[0].failure_stage, "ok");
}

}  // namespace
}  // namespace googlesql::bird_eval
