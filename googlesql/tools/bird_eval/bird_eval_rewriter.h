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

#ifndef GOOGLESQL_TOOLS_BIRD_EVAL_BIRD_EVAL_REWRITER_H_
#define GOOGLESQL_TOOLS_BIRD_EVAL_BIRD_EVAL_REWRITER_H_

#include <string>
#include <vector>

#include "absl/strings/string_view.h"

namespace googlesql::bird_eval {

struct RewriteResult {
  std::string rewritten_sql;
  std::vector<std::string> applied_rules;
  bool changed = false;
};

RewriteResult RewriteSql(absl::string_view sql);

}  // namespace googlesql::bird_eval

#endif  // GOOGLESQL_TOOLS_BIRD_EVAL_BIRD_EVAL_REWRITER_H_
