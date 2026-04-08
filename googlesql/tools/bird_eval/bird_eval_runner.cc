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

#include <algorithm>
#include <fstream>
#include <iostream>
#include <memory>
#include <regex>
#include <string>
#include <utility>
#include <vector>

#include "googlesql/base/file_util.h"
#include "googlesql/base/path.h"
#include "googlesql/parser/parser.h"
#include "googlesql/public/analyzer.h"
#include "googlesql/public/analyzer_options.h"
#include "googlesql/public/analyzer_output.h"
#include "googlesql/public/builtin_function_options.h"
#include "googlesql/public/evaluator.h"
#include "googlesql/public/evaluator_table_iterator.h"
#include "googlesql/public/language_options.h"
#include "googlesql/public/simple_catalog.h"
#include "googlesql/public/type.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/public/value.h"
#include "googlesql/tools/bird_eval/bird_eval_rewriter.h"
#include "googlesql/tools/bird_eval/bird_eval_sqlite.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/escaping.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/struct.pb.h"
#include "google/protobuf/util/json_util.h"
#include "googlesql/base/status_macros.h"

namespace googlesql::bird_eval {
namespace {

struct BirdSample {
  std::string sample_id;
  std::string db_id;
  std::string question;
  std::string gold_sql;
  std::string db_path;
};

struct DatabaseContext {
  std::string db_path;
  TypeFactory type_factory;
  LanguageOptions language_options;
  SimpleCatalog catalog;
  std::vector<std::unique_ptr<SimpleTable>> owned_tables;

  DatabaseContext()
      : language_options(LanguageOptions()),
        catalog("bird_catalog", &type_factory) {}
};

absl::StatusOr<std::string> GetJsonStringField(const google::protobuf::Struct& object,
                                               absl::string_view name) {
  const auto it = object.fields().find(std::string(name));
  if (it == object.fields().end()) {
    return absl::NotFoundError(
        absl::StrCat("Missing JSON field: ", name));
  }
  if (it->second.kind_case() != google::protobuf::Value::kStringValue) {
    return absl::InvalidArgumentError(
        absl::StrCat("JSON field is not a string: ", name));
  }
  return it->second.string_value();
}

std::string GetOptionalJsonStringField(const google::protobuf::Struct& object,
                                       absl::string_view name) {
  const auto it = object.fields().find(std::string(name));
  if (it == object.fields().end()) {
    return "";
  }
  if (it->second.kind_case() != google::protobuf::Value::kStringValue) {
    return "";
  }
  return it->second.string_value();
}

absl::StatusOr<std::string> ResolveSqlField(const google::protobuf::Struct& object) {
  for (absl::string_view field_name : {"SQL", "sql", "query"}) {
    const std::string value = GetOptionalJsonStringField(object, field_name);
    if (!value.empty()) return value;
  }
  return absl::NotFoundError("Missing SQL field");
}

absl::StatusOr<std::string> ResolveQuestionField(
    const google::protobuf::Struct& object) {
  for (absl::string_view field_name : {"question", "Question"}) {
    const std::string value = GetOptionalJsonStringField(object, field_name);
    if (!value.empty()) return value;
  }
  return absl::NotFoundError("Missing question field");
}

absl::StatusOr<std::string> ResolveDbPath(absl::string_view db_root,
                                          absl::string_view db_id) {
  const std::vector<std::string> candidates = {
      googlesql_base::JoinPath(db_root, db_id, absl::StrCat(db_id, ".sqlite")),
      googlesql_base::JoinPath(db_root, absl::StrCat(db_id, ".sqlite")),
      googlesql_base::JoinPath(db_root, db_id, "database.sqlite"),
  };
  for (const std::string& candidate : candidates) {
    std::ifstream stream(candidate);
    if (stream.good()) {
      return candidate;
    }
  }
  return absl::NotFoundError(
      absl::StrCat("Unable to locate SQLite DB for db_id=", db_id));
}

absl::StatusOr<std::vector<BirdSample>> LoadBirdSamples(
    const BirdEvalOptions& options) {
  std::string json;
  GOOGLESQL_RETURN_IF_ERROR(
      googlesql::internal::GetContents(options.bird_json_path, &json));

  google::protobuf::Value root;
  GOOGLESQL_RETURN_IF_ERROR(google::protobuf::util::JsonStringToMessage(json, &root));
  if (root.kind_case() != google::protobuf::Value::kListValue) {
    return absl::InvalidArgumentError("BIRD JSON root must be an array");
  }

  std::vector<BirdSample> samples;
  for (int i = 0; i < root.list_value().values_size(); ++i) {
    const google::protobuf::Value& sample_value = root.list_value().values(i);
    if (sample_value.kind_case() != google::protobuf::Value::kStructValue) {
      return absl::InvalidArgumentError("BIRD sample must be a JSON object");
    }
    const google::protobuf::Struct& object = sample_value.struct_value();
    GOOGLESQL_ASSIGN_OR_RETURN(std::string db_id, GetJsonStringField(object, "db_id"));
    if (!options.db_filter.empty() && db_id != options.db_filter) {
      continue;
    }
    GOOGLESQL_ASSIGN_OR_RETURN(std::string gold_sql, ResolveSqlField(object));
    GOOGLESQL_ASSIGN_OR_RETURN(std::string question, ResolveQuestionField(object));
    GOOGLESQL_ASSIGN_OR_RETURN(std::string db_path,
                       ResolveDbPath(options.sqlite_db_root, db_id));

    const std::string explicit_id =
        GetOptionalJsonStringField(object, "question_id");
    samples.push_back(BirdSample{
        .sample_id = explicit_id.empty() ? absl::StrCat(db_id, ":", i) : explicit_id,
        .db_id = std::move(db_id),
        .question = std::move(question),
        .gold_sql = std::move(gold_sql),
        .db_path = std::move(db_path),
    });
    if (options.sample_limit >= 0 &&
        samples.size() >= static_cast<size_t>(options.sample_limit)) {
      break;
    }
  }
  return samples;
}

absl::StatusOr<DatabaseContext*> GetOrCreateContext(
    const BirdSample& sample,
    absl::flat_hash_map<std::string, std::unique_ptr<DatabaseContext>>* cache) {
  auto it = cache->find(sample.db_id);
  if (it != cache->end()) {
    return it->second.get();
  }

  auto context = std::make_unique<DatabaseContext>();
  context->db_path = sample.db_path;
  context->language_options.EnableMaximumLanguageFeatures();
  GOOGLESQL_RETURN_IF_ERROR(
      context->language_options.EnableReservableKeyword("GRAPH_TABLE"));
  GOOGLESQL_RETURN_IF_ERROR(
      context->language_options.EnableReservableKeyword("MATCH_RECOGNIZE"));
  context->catalog.AddBuiltinFunctions(
      BuiltinFunctionOptions(context->language_options));
  GOOGLESQL_ASSIGN_OR_RETURN(
      SqliteCatalogContents contents,
      BuildSimpleTablesFromSqlite(sample.db_path, &context->type_factory));
  context->owned_tables = std::move(contents.tables);
  for (const auto& table : context->owned_tables) {
    context->catalog.AddTable(table.get());
  }

  DatabaseContext* raw = context.get();
  cache->emplace(sample.db_id, std::move(context));
  return raw;
}

absl::Status ParseSql(const std::string& sql, const LanguageOptions& options) {
  ParserOptions parser_options(options);
  std::unique_ptr<ParserOutput> parser_output;
  return ParseStatement(sql, parser_options, &parser_output);
}

std::string NormalizeValue(const Value& value);

std::string NormalizeArrayOrScalar(const Value& value) {
  if (value.type_kind() != TYPE_ARRAY) {
    return NormalizeValue(value);
  }
  std::vector<std::string> rows;
  rows.reserve(value.num_elements());
  for (const Value& element : value.elements()) {
    rows.push_back(NormalizeValue(element));
  }
  std::sort(rows.begin(), rows.end());
  return absl::StrCat("[", absl::StrJoin(rows, ","), "]");
}

std::string NormalizeValue(const Value& value) {
  if (value.is_null()) {
    return "NULL";
  }
  switch (value.type_kind()) {
    case TYPE_INT32:
    case TYPE_INT64:
    case TYPE_UINT32:
    case TYPE_UINT64:
    case TYPE_DATE:
    case TYPE_ENUM:
      return absl::StrCat("I:", value.ToInt64());
    case TYPE_BOOL:
      return absl::StrCat("B:", value.bool_value() ? "true" : "false");
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
      return absl::StrFormat("F:%.17g", value.ToDouble());
    case TYPE_STRING:
      return absl::StrCat("S:", absl::CEscape(value.string_value()));
    case TYPE_BYTES:
      return absl::StrCat("Y:", absl::BytesToHexString(value.bytes_value()));
    case TYPE_STRUCT: {
      std::vector<std::string> fields;
      fields.reserve(value.num_fields());
      for (const Value& field : value.fields()) {
        fields.push_back(NormalizeValue(field));
      }
      return absl::StrCat("{", absl::StrJoin(fields, ","), "}");
    }
    case TYPE_ARRAY:
      return NormalizeArrayOrScalar(value);
    default:
      return value.DebugString();
  }
}

std::string NormalizeSqliteRows(const SqliteQueryResult& result) {
  std::vector<std::string> rows;
  rows.reserve(result.normalized_rows.size());
  for (const auto& row : result.normalized_rows) {
    rows.push_back(absl::StrCat("{", absl::StrJoin(row, ","), "}"));
  }
  std::sort(rows.begin(), rows.end());
  return absl::StrCat("[", absl::StrJoin(rows, ","), "]");
}

absl::StatusOr<std::string> NormalizeIteratorRows(EvaluatorTableIterator* iter) {
  constexpr int64_t kMaxRowsToNormalize = 100000;
  std::vector<std::string> rows;
  int64_t row_count = 0;
  while (true) {
    if (!iter->NextRow()) {
      GOOGLESQL_RETURN_IF_ERROR(iter->Status());
      break;
    }
    ++row_count;
    if (row_count > kMaxRowsToNormalize) {
      return absl::ResourceExhaustedError(
          absl::StrCat("Iterator row count exceeded limit: ",
                       kMaxRowsToNormalize));
    }
    std::vector<std::string> row;
    row.reserve(iter->NumColumns());
    for (int i = 0; i < iter->NumColumns(); ++i) {
      row.push_back(NormalizeValue(iter->GetValue(i)));
    }
    rows.push_back(absl::StrCat("{", absl::StrJoin(row, ","), "}"));
  }
  std::sort(rows.begin(), rows.end());
  return absl::StrCat("[", absl::StrJoin(rows, ","), "]");
}

std::string JsonEscape(absl::string_view value) {
  std::string escaped;
  escaped.reserve(value.size() + 4);
  for (char ch : value) {
    switch (ch) {
      case '\\':
        escaped.append("\\\\");
        break;
      case '"':
        escaped.append("\\\"");
        break;
      case '\n':
        escaped.append("\\n");
        break;
      case '\r':
        escaped.append("\\r");
        break;
      case '\t':
        escaped.append("\\t");
        break;
      default:
        escaped.push_back(ch);
    }
  }
  return escaped;
}

bool IsKnownBlockingExecutePattern(absl::string_view sql) {
  static const auto* const kScalarSubqueryOrderByLimitPattern = new std::regex(
      R"regex(=\s*\(\s*SELECT[\s\S]*ORDER\s+BY[\s\S]*LIMIT\s+[0-9]+\s*\))regex",
      std::regex_constants::icase);
  return std::regex_search(std::string(sql),
                           *kScalarSubqueryOrderByLimitPattern);
}

BirdSampleResult EvaluateSample(const BirdSample& sample, DatabaseContext* context) {
  std::cerr << "[bird_eval] start sample " << sample.sample_id << std::endl;
  BirdSampleResult result;
  result.sample_id = sample.sample_id;
  result.db_id = sample.db_id;
  result.question = sample.question;
  result.original_sql = sample.gold_sql;
  result.normalized_sql = sample.gold_sql;

  std::cerr << "[bird_eval] parse sample " << sample.sample_id << std::endl;
  const absl::Status parse_status = ParseSql(sample.gold_sql, context->language_options);
  if (!parse_status.ok()) {
    RewriteResult rewrite = RewriteSql(sample.gold_sql);
    result.rewrite_rules = rewrite.applied_rules;
    result.normalized_sql = rewrite.rewritten_sql;
    if (rewrite.changed) {
      const absl::Status rewritten_parse_status =
          ParseSql(rewrite.rewritten_sql, context->language_options);
      if (rewritten_parse_status.ok()) {
        result.parse_ok = true;
      } else {
        result.failure_stage = "parse";
        result.error_message = rewritten_parse_status.ToString();
        return result;
      }
    } else {
      result.failure_stage = "parse";
      result.error_message = parse_status.ToString();
      return result;
    }
  } else {
    result.parse_ok = true;
  }

  std::cerr << "[bird_eval] analyze sample " << sample.sample_id << std::endl;
  AnalyzerOptions analyzer_options;
  analyzer_options.set_language(context->language_options);
  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  absl::Status analyze_status =
      AnalyzeStatement(result.normalized_sql, analyzer_options,
                       &context->catalog, &context->type_factory,
                       &analyzer_output);
  if (!analyze_status.ok() && result.rewrite_rules.empty()) {
    RewriteResult rewrite = RewriteSql(sample.gold_sql);
    if (rewrite.changed) {
      result.rewrite_rules = rewrite.applied_rules;
      result.normalized_sql = rewrite.rewritten_sql;
      analyze_status = AnalyzeStatement(result.normalized_sql, analyzer_options,
                                        &context->catalog,
                                        &context->type_factory,
                                        &analyzer_output);
    }
  }
  if (!analyze_status.ok()) {
    result.failure_stage = "analyze";
    result.error_message = analyze_status.ToString();
    return result;
  }
  result.analyze_ok = true;

  std::cerr << "[bird_eval] sqlite sample " << sample.sample_id << std::endl;
  absl::StatusOr<SqliteQueryResult> sqlite_result =
      ExecuteSqliteQuery(sample.db_path, sample.gold_sql);
  if (!sqlite_result.ok()) {
    result.failure_stage = "sqlite_execute";
    result.error_message = sqlite_result.status().ToString();
    return result;
  }
  result.sqlite_execute_ok = true;

  std::cerr << "[bird_eval] googlesql_prepare sample " << sample.sample_id << std::endl;
  PreparedQuery prepared_query(result.normalized_sql, EvaluatorOptions());
  const absl::Status prepare_status =
      prepared_query.Prepare(analyzer_options, &context->catalog);
  if (!prepare_status.ok()) {
    result.failure_stage = "googlesql_execute";
    result.error_message = prepare_status.ToString();
    return result;
  }
  if (IsKnownBlockingExecutePattern(result.normalized_sql)) {
    result.failure_stage = "googlesql_execute";
    result.error_message =
        "Known blocking execute pattern: scalar subquery with ORDER BY/LIMIT";
    return result;
  }
  std::cerr << "[bird_eval] googlesql_execute sample " << sample.sample_id << std::endl;
  absl::StatusOr<std::unique_ptr<EvaluatorTableIterator>> iter =
      prepared_query.Execute();
  if (!iter.ok()) {
    result.failure_stage = "googlesql_execute";
    result.error_message = iter.status().ToString();
    return result;
  }
  std::cerr << "[bird_eval] normalize sample " << sample.sample_id << std::endl;
  absl::StatusOr<std::string> googlesql_rows =
      NormalizeIteratorRows(iter->get());
  if (!googlesql_rows.ok()) {
    result.failure_stage = "googlesql_execute";
    result.error_message = googlesql_rows.status().ToString();
    return result;
  }
  result.googlesql_execute_ok = true;

  std::cerr << "[bird_eval] done sample " << sample.sample_id << std::endl;
  result.result_match =
      *googlesql_rows == NormalizeSqliteRows(*sqlite_result);
  if (!result.result_match) {
    result.failure_stage = "result_mismatch";
    result.error_message = "SQLite and GoogleSQL results differ";
    return result;
  }

  result.failure_stage = "ok";
  return result;
}

}  // namespace

absl::StatusOr<BirdEvalSummary> RunBirdEvaluation(const BirdEvalOptions& options) {
  GOOGLESQL_ASSIGN_OR_RETURN(std::vector<BirdSample> samples, LoadBirdSamples(options));

  BirdEvalSummary summary;
  absl::flat_hash_map<std::string, std::unique_ptr<DatabaseContext>> db_cache;
  for (const BirdSample& sample : samples) {
    GOOGLESQL_ASSIGN_OR_RETURN(DatabaseContext * context,
                       GetOrCreateContext(sample, &db_cache));
    BirdSampleResult result = EvaluateSample(sample, context);
    ++summary.total;
    if (result.parse_ok) ++summary.parse_ok;
    if (result.analyze_ok) ++summary.analyze_ok;
    if (result.sqlite_execute_ok) ++summary.sqlite_execute_ok;
    if (result.googlesql_execute_ok) ++summary.googlesql_execute_ok;
    if (result.result_match) ++summary.matched;
    summary.sample_results.push_back(std::move(result));
  }
  return summary;
}

std::string SummaryAsMarkdown(const BirdEvalSummary& summary) {
  return absl::StrFormat(
      "# BIRD Eval Summary\n\n"
      "- Samples: %d\n"
      "- Parse OK: %d\n"
      "- Analyze OK: %d\n"
      "- SQLite Execute OK: %d\n"
      "- GoogleSQL Execute OK: %d\n"
      "- Result Match: %d\n",
      summary.total, summary.parse_ok, summary.analyze_ok,
      summary.sqlite_execute_ok, summary.googlesql_execute_ok, summary.matched);
}

std::string SampleResultAsJson(const BirdSampleResult& result) {
  std::vector<std::string> quoted_rules;
  quoted_rules.reserve(result.rewrite_rules.size());
  for (const std::string& rule : result.rewrite_rules) {
    quoted_rules.push_back(absl::StrCat("\"", JsonEscape(rule), "\""));
  }
  return absl::StrCat(
      "{",
      "\"sample_id\":\"", JsonEscape(result.sample_id), "\",",
      "\"db_id\":\"", JsonEscape(result.db_id), "\",",
      "\"question\":\"", JsonEscape(result.question), "\",",
      "\"original_sql\":\"", JsonEscape(result.original_sql), "\",",
      "\"normalized_sql\":\"", JsonEscape(result.normalized_sql), "\",",
      "\"rewrite_rules\":[", absl::StrJoin(quoted_rules, ","), "],",
      "\"parse_ok\":", result.parse_ok ? "true" : "false", ",",
      "\"analyze_ok\":", result.analyze_ok ? "true" : "false", ",",
      "\"sqlite_execute_ok\":", result.sqlite_execute_ok ? "true" : "false", ",",
      "\"googlesql_execute_ok\":", result.googlesql_execute_ok ? "true" : "false", ",",
      "\"result_match\":", result.result_match ? "true" : "false", ",",
      "\"failure_stage\":\"", JsonEscape(result.failure_stage), "\",",
      "\"error_message\":\"", JsonEscape(result.error_message), "\"",
      "}");
}

}  // namespace googlesql::bird_eval
