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

#include <iostream>
#include <string>

#include "googlesql/base/file_util.h"
#include "googlesql/tools/bird_eval/bird_eval_runner.h"
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/flags/usage.h"
#include "absl/log/initialize.h"
#include "absl/status/status.h"
#include "absl/strings/str_join.h"

ABSL_FLAG(std::string, bird_path, "", "Path to the BIRD benchmark JSON file.");
ABSL_FLAG(std::string, sqlite_db_root, "",
          "Root directory containing SQLite DB files for each BIRD db_id.");
ABSL_FLAG(std::string, db_filter, "",
          "Optional db_id filter to run a subset of BIRD.");
ABSL_FLAG(int, sample_limit, -1, "Optional max number of samples to run.");
ABSL_FLAG(std::string, details_out, "",
          "Optional path for JSONL per-sample results.");
ABSL_FLAG(std::string, report_out, "",
          "Optional path for Markdown summary report.");

int main(int argc, char* argv[]) {
  absl::SetProgramUsageMessage(
      "Run layered BIRD-Bench verification against SQLite and GoogleSQL.");
  absl::ParseCommandLine(argc, argv);
  absl::InitializeLog();

  googlesql::bird_eval::BirdEvalOptions options;
  options.bird_json_path = absl::GetFlag(FLAGS_bird_path);
  options.sqlite_db_root = absl::GetFlag(FLAGS_sqlite_db_root);
  options.db_filter = absl::GetFlag(FLAGS_db_filter);
  options.sample_limit = absl::GetFlag(FLAGS_sample_limit);

  absl::StatusOr<googlesql::bird_eval::BirdEvalSummary> summary =
      googlesql::bird_eval::RunBirdEvaluation(options);
  if (!summary.ok()) {
    std::cerr << summary.status() << std::endl;
    return 1;
  }

  const std::string markdown =
      googlesql::bird_eval::SummaryAsMarkdown(*summary);
  if (absl::GetFlag(FLAGS_report_out).empty()) {
    std::cout << markdown << std::endl;
  } else {
    const absl::Status write_status =
        googlesql::internal::SetContents(absl::GetFlag(FLAGS_report_out),
                                         markdown);
    if (!write_status.ok()) {
      std::cerr << write_status << std::endl;
      return 1;
    }
  }

  if (!absl::GetFlag(FLAGS_details_out).empty()) {
    std::string jsonl;
    for (const auto& result : summary->sample_results) {
      if (!jsonl.empty()) jsonl.append("\n");
      jsonl.append(googlesql::bird_eval::SampleResultAsJson(result));
    }
    const absl::Status write_status =
        googlesql::internal::SetContents(absl::GetFlag(FLAGS_details_out),
                                         jsonl);
    if (!write_status.ok()) {
      std::cerr << write_status << std::endl;
      return 1;
    }
  }

  return 0;
}
