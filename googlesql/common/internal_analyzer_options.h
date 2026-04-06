//
// Copyright 2019 Google LLC
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

#ifndef GOOGLESQL_COMMON_INTERNAL_ANALYZER_OPTIONS_H_
#define GOOGLESQL_COMMON_INTERNAL_ANALYZER_OPTIONS_H_

#include <utility>

#include "googlesql/common/errors.h"
#include "googlesql/public/analyzer_options.h"
#include "googlesql/public/property_graph.h"
#include "googlesql/public/types/type.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/string_view.h"
#include "googlesql/base/map_util.h"

namespace googlesql {

// InternalAnalyzerOptions only contains static methods to access private
// fields of AnalyzerOptions that are not part of the public APIs.
// Internal components (e.g. AnalyzeSubstitute) also use public APIs
// (e.g. AnalyzerOptions) but could be using private fields for settings that
// aren't safe for users to use directly.
// It is more preferred to use friend class than maintain redundant versions
// of the above mentioned public APIs.
class InternalAnalyzerOptions {
 public:
  InternalAnalyzerOptions() = delete;
  InternalAnalyzerOptions(const InternalAnalyzerOptions&) = delete;
  InternalAnalyzerOptions& operator=(const InternalAnalyzerOptions&) = delete;

  static void SetLookupExpressionCallback(
      AnalyzerOptions& options,
      AnalyzerOptions::LookupExpressionCallback lookup_expression_callback) {
    options.data_->lookup_expression_callback =
        std::move(lookup_expression_callback);
  }

  static const AnalyzerOptions::LookupExpressionCallback&
  GetLookupExpressionCallback(const AnalyzerOptions& options) {
    return options.data_->lookup_expression_callback;
  }

  static void ClearExpressionColumns(AnalyzerOptions& options) {
    options.data_->expression_columns.clear();
  }

  // AnalyzerOptions::validate_resolved_ast_ is used by internal components
  // calling public API to be distinguished. Internal calls might not have a
  // complete tree and thus could lookup AnalyzerOptions instead of the global
  // flag value to decide whether or not validator is triggered.
  static void SetValidateResolvedAST(AnalyzerOptions& options, bool validate) {
    options.data_->validate_resolved_ast = validate;
  }

  static bool GetValidateResolvedAST(const AnalyzerOptions& options) {
    return options.data_->validate_resolved_ast;
  }

  static absl::Status AddGraphProperty(const AnalyzerOptions& options,
                                       absl::string_view name,
                                       const Type* type,
                                       GraphPropertyDeclaration::Kind kind =
                                           GraphPropertyDeclaration::Kind::
                                               kScalar) {
    GOOGLESQL_RETURN_IF_ERROR(AddGraphPropertyMetadata(options, name, type, kind));
    const std::string lower_name = absl::AsciiStrToLower(name);
    if (!googlesql_base::InsertIfNotPresent(
            &(options.data_->graph_properties), std::make_pair(lower_name, type))) {
      return MakeSqlError()
             << "Duplicate graph property name " << lower_name;
    }

    return absl::OkStatus();
  }

  static absl::Status AddGraphPropertyMetadata(
      const AnalyzerOptions& options, absl::string_view name, const Type* type,
      GraphPropertyDeclaration::Kind kind) {
    if (type == nullptr) {
      return MakeSqlError()
             << "Type associated with graph property cannot be NULL";
    }
    if (name.empty()) {
      return MakeSqlError() << "Graph property cannot have empty name";
    }

    const std::string lower_name = absl::AsciiStrToLower(name);
    const auto* existing = googlesql_base::FindOrNull(
        options.data_->graph_property_metadata, lower_name);
    if (existing != nullptr) {
      if (existing->type == type && existing->kind == kind &&
          absl::AsciiStrToLower(existing->semantic_name) == lower_name) {
        return absl::OkStatus();
      }
      return MakeSqlError() << "Duplicate graph property name " << lower_name;
    }
    googlesql_base::InsertOrDie(
        &(options.data_->graph_property_metadata), lower_name,
        AnalyzerOptions::Data::GraphPropertyMetadata{
            .type = type, .kind = kind, .semantic_name = std::string(name)});

    return absl::OkStatus();
  }

  static void ClearGraphProperty(const AnalyzerOptions& options) {
    options.data_->graph_properties.clear();
    options.data_->graph_property_metadata.clear();
  }

  static const Type* FindGraphPropertyType(const AnalyzerOptions& options,
                                           absl::string_view name) {
    const auto* metadata = googlesql_base::FindOrNull(
        options.data_->graph_property_metadata, absl::AsciiStrToLower(name));
    return metadata == nullptr ? nullptr : metadata->type;
  }

  static GraphPropertyDeclaration::Kind FindGraphPropertyKind(
      const AnalyzerOptions& options, absl::string_view name) {
    const auto* metadata = googlesql_base::FindOrNull(
        options.data_->graph_property_metadata, absl::AsciiStrToLower(name));
    return metadata == nullptr ? GraphPropertyDeclaration::Kind::kInvalid
                               : metadata->kind;
  }

  static absl::string_view FindGraphPropertySemanticName(
      const AnalyzerOptions& options, absl::string_view name) {
    const auto* metadata = googlesql_base::FindOrNull(
        options.data_->graph_property_metadata, absl::AsciiStrToLower(name));
    return metadata == nullptr ? absl::string_view() : metadata->semantic_name;
  }
};
}  // namespace googlesql

#endif  // GOOGLESQL_COMMON_INTERNAL_ANALYZER_OPTIONS_H_
