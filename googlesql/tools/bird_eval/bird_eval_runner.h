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

#ifndef GOOGLESQL_TOOLS_BIRD_EVAL_BIRD_EVAL_RUNNER_H_
#define GOOGLESQL_TOOLS_BIRD_EVAL_BIRD_EVAL_RUNNER_H_

#include <string>
#include <vector>

#include "absl/status/statusor.h"

namespace googlesql::bird_eval {

struct BirdEvalOptions {
  std::string bird_json_path;
  std::string sqlite_db_root;
  std::string db_filter;
  int sample_limit = -1;
};

struct BirdSampleResult {
  std::string sample_id;
  std::string db_id;
  std::string question;
  std::string original_sql;
  std::string normalized_sql;
  std::vector<std::string> rewrite_rules;
  bool parse_ok = false;
  bool analyze_ok = false;
  bool sqlite_execute_ok = false;
  bool googlesql_execute_ok = false;
  bool result_match = false;
  std::string failure_stage;
  std::string error_message;
};

struct BirdEvalSummary {
  int total = 0;
  int parse_ok = 0;
  int analyze_ok = 0;
  int sqlite_execute_ok = 0;
  int googlesql_execute_ok = 0;
  int matched = 0;
  std::vector<BirdSampleResult> sample_results;
};

absl::StatusOr<BirdEvalSummary> RunBirdEvaluation(const BirdEvalOptions& options);
std::string SummaryAsMarkdown(const BirdEvalSummary& summary);
std::string SampleResultAsJson(const BirdSampleResult& result);

}  // namespace googlesql::bird_eval

#endif  // GOOGLESQL_TOOLS_BIRD_EVAL_BIRD_EVAL_RUNNER_H_
