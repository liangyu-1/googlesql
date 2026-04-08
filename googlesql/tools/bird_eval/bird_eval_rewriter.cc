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

#include "googlesql/tools/bird_eval/bird_eval_rewriter.h"

#include <regex>
#include <string>

#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/string_view.h"

namespace googlesql::bird_eval {
namespace {

bool ReplaceAllCaseInsensitive(std::string* sql, absl::string_view needle,
                               absl::string_view replacement) {
  std::string lowered = absl::AsciiStrToLower(*sql);
  const std::string lowered_needle = absl::AsciiStrToLower(needle);
  bool changed = false;
  size_t pos = 0;
  while ((pos = lowered.find(lowered_needle, pos)) != std::string::npos) {
    sql->replace(pos, needle.size(), replacement);
    lowered.replace(pos, needle.size(), replacement);
    pos += replacement.size();
    changed = true;
  }
  return changed;
}

bool RewriteLimitOffset(std::string* sql) {
  static const auto* const kLimitOffsetPattern = new std::regex(
      R"regex(\bLIMIT\s+([0-9]+)\s*,\s*([0-9]+))regex",
      std::regex_constants::icase);
  if (!std::regex_search(*sql, *kLimitOffsetPattern)) {
    return false;
  }
  *sql = std::regex_replace(*sql, *kLimitOffsetPattern, "LIMIT $2 OFFSET $1");
  return true;
}

bool RewriteRealTypeCast(std::string* sql) {
  static const auto* const kRealCastPattern = new std::regex(
      R"regex(\bAS\s+REAL\b)regex", std::regex_constants::icase);
  if (!std::regex_search(*sql, *kRealCastPattern)) {
    return false;
  }
  *sql = std::regex_replace(*sql, *kRealCastPattern, "AS DOUBLE");
  return true;
}

}  // namespace

RewriteResult RewriteSql(absl::string_view sql) {
  RewriteResult result;
  result.rewritten_sql = std::string(sql);

  if (ReplaceAllCaseInsensitive(&result.rewritten_sql, "ifnull(", "coalesce(")) {
    result.applied_rules.push_back("ifnull_to_coalesce");
  }
  if (ReplaceAllCaseInsensitive(&result.rewritten_sql, "length(", "char_length(")) {
    result.applied_rules.push_back("length_to_char_length");
  }
  if (RewriteLimitOffset(&result.rewritten_sql)) {
    result.applied_rules.push_back("limit_offset_comma_syntax");
  }
  if (RewriteRealTypeCast(&result.rewritten_sql)) {
    result.applied_rules.push_back("cast_real_to_double");
  }

  result.changed = !result.applied_rules.empty();
  return result;
}

}  // namespace googlesql::bird_eval
