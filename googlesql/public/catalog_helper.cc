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

#include "googlesql/public/catalog_helper.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "googlesql/base/logging.h"
#include "googlesql/public/strings.h"
#include "googlesql/public/property_graph.h"
#include "googlesql/base/case.h"
#include "absl/flags/flag.h"
#include "absl/functional/function_ref.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "googlesql/base/edit_distance.h"

ABSL_FLAG(int64_t, googlesql_min_length_required_for_edit_distance, 3,
          "Minimum length required of the input string to find its closest "
          "match based on edit distance.");

namespace googlesql {

template <typename NameListT>
typename NameListT::value_type ClosestNameImpl(
    absl::string_view mistyped_name, const NameListT& possible_names,
    absl::FunctionRef<bool(char, char)> char_equal_fn,
    absl::FunctionRef<int(absl::string_view, absl::string_view)>
        string_compare_fn) {
  if (mistyped_name.size() <
      absl::GetFlag(FLAGS_googlesql_min_length_required_for_edit_distance)) {
    return {};
  }

  // Allow ~20% edit distance for suggestions.
  const int distance_threshold = (mistyped_name.size() < 5)
                                 ? mistyped_name.size() / 2
                                 : mistyped_name.size() / 5 + 2;
  int min_edit_distance = distance_threshold + 1;

  int closest_name_index = -1;
  for (int i = 0; i < possible_names.size(); ++i) {
    // Exclude internal names (with '$' prefix).
    if (IsInternalAlias(possible_names[i])) continue;

    const int edit_distance = googlesql_base::CappedLevenshteinDistance(
        mistyped_name.begin(), mistyped_name.end(), possible_names[i].begin(),
        possible_names[i].end(), char_equal_fn, distance_threshold + 1);
    if (edit_distance < min_edit_distance) {
      min_edit_distance = edit_distance;
      closest_name_index = i;
    } else if (edit_distance == min_edit_distance && closest_name_index != -1 &&
               string_compare_fn(possible_names[i],
                                 possible_names[closest_name_index]) < 0) {
      // As a tie-breaker we chose the string which occurs lexicographically
      // first.
      closest_name_index = i;
    }
  }

  if (closest_name_index == -1) {
    // No match found within the allowed ~20% edit distance.
    return {};
  }

  ABSL_DCHECK_GE(closest_name_index, 0);
  ABSL_DCHECK_LT(closest_name_index, possible_names.size());
  return possible_names[closest_name_index];
}

std::string ClosestName(absl::string_view mistyped_name,
                        const std::vector<std::string>& possible_names) {
  // TODO: Should this be case insensitive like SuggestEnumValue?
  return ClosestNameImpl(
      mistyped_name, possible_names, std::equal_to<char>(),
      [](absl::string_view a, absl::string_view b) { return a.compare(b); });
}

std::string SuggestEnumValue(const EnumType* type,
                             absl::string_view mistyped_value) {
  if (type == nullptr || mistyped_value.empty()) {
    // Nothing to suggest here.
    return {};
  }
  std::vector<absl::string_view> suggest_strings;
  const google::protobuf::EnumDescriptor* type_descriptor = type->enum_descriptor();
  for (int i = 0; i < type_descriptor->value_count(); ++i) {
    const google::protobuf::EnumValueDescriptor* value_descriptor =
        type_descriptor->value(i);
    if (type->IsValidEnumValue(value_descriptor)) {
      suggest_strings.emplace_back(value_descriptor->name());
    }
  }

  // We implement a case-insensitive matching for purposes of suggesting enums.
  return std::string(ClosestNameImpl(
      mistyped_value, suggest_strings,
      // Case insensitive single character equals lambda.
      [](char a, char b) {
        return absl::ascii_toupper(a) == absl::ascii_toupper(b);
      },
      // Case insensitive string comparison.
      [](absl::string_view a, absl::string_view b) {
        return googlesql_base::CaseCompare(a, b);
      }));
}

std::string GetPropertyGraphSqlColumnName(
    absl::string_view graph_name, PropertyGraphSqlColumnKind kind,
    absl::string_view semantic_name) {
  const std::string prefix = absl::AsciiStrToLower(graph_name);
  switch (kind) {
    case PropertyGraphSqlColumnKind::kProperty:
      return absl::StrCat(prefix, "_property_", semantic_name);
    case PropertyGraphSqlColumnKind::kDynamicLabel:
      return absl::StrCat(prefix, "_dynamic_label");
    case PropertyGraphSqlColumnKind::kDynamicProperties:
      return absl::StrCat(prefix, "_dynamic_properties");
    case PropertyGraphSqlColumnKind::kSource:
      return absl::StrCat(prefix, "_source_", semantic_name);
    case PropertyGraphSqlColumnKind::kDestination:
      return absl::StrCat(prefix, "_destination_", semantic_name);
    case PropertyGraphSqlColumnKind::kOutgoing:
      return absl::StrCat(prefix, "_outgoing_", semantic_name);
    case PropertyGraphSqlColumnKind::kIncoming:
      return absl::StrCat(prefix, "_incoming_", semantic_name);
  }
  ABSL_LOG(FATAL) << "Unknown property graph SQL column kind";  // Crash OK
}

absl::Status FindPropertyGraphSqlColumn(
    const Table& table, absl::string_view graph_name,
    PropertyGraphSqlColumnKind kind, const Column** column,
    absl::string_view semantic_name) {
  *column =
      table.FindColumnByName(GetPropertyGraphSqlColumnName(
          graph_name, kind, semantic_name));
  if (*column == nullptr) {
    return absl::NotFoundError(
        absl::StrCat("Property graph SQL column not found on table ",
                     table.FullName(), ": ",
                     GetPropertyGraphSqlColumnName(graph_name, kind,
                                                   semantic_name)));
  }
  return absl::OkStatus();
}

absl::Status FindPropertyGraphSqlColumn(
    const PropertyGraph& graph, const GraphElementTable& element_table,
    PropertyGraphSqlColumnKind kind, const Column** column,
    absl::string_view semantic_name) {
  return FindPropertyGraphSqlColumn(*element_table.GetTable(), graph.Name(),
                                    kind, column, semantic_name);
}

absl::Status FindPropertyGraphPropertyColumn(
    const PropertyGraph& graph, const GraphElementTable& element_table,
    const GraphPropertyDeclaration& property_declaration, const Column** column) {
  return FindPropertyGraphSqlColumn(
      graph, element_table, PropertyGraphSqlColumnKind::kProperty, column,
      property_declaration.Name());
}

absl::Status FindPropertyGraphDynamicLabelColumn(
    const PropertyGraph& graph, const GraphElementTable& element_table,
    const Column** column) {
  return FindPropertyGraphSqlColumn(graph, element_table,
                                    PropertyGraphSqlColumnKind::kDynamicLabel,
                                    column);
}

absl::Status FindPropertyGraphDynamicPropertiesColumn(
    const PropertyGraph& graph, const GraphElementTable& element_table,
    const Column** column) {
  return FindPropertyGraphSqlColumn(
      graph, element_table, PropertyGraphSqlColumnKind::kDynamicProperties,
      column);
}

absl::Status FindPropertyGraphSourceColumn(const PropertyGraph& graph,
                                           const GraphEdgeTable& edge_table,
                                           const Column** column) {
  const GraphNodeTableReference& source_ref =
      GetSourceNodeTableReference(edge_table);
  return FindPropertyGraphSqlColumn(
      *edge_table.GetTable(), graph.Name(), PropertyGraphSqlColumnKind::kSource,
      column, source_ref.GetReferencedNodeTable()->Name());
}

absl::Status FindPropertyGraphDestinationColumn(const PropertyGraph& graph,
                                                const GraphEdgeTable& edge_table,
                                                const Column** column) {
  const GraphNodeTableReference& destination_ref =
      GetDestinationNodeTableReference(edge_table);
  return FindPropertyGraphSqlColumn(
      *edge_table.GetTable(), graph.Name(),
      PropertyGraphSqlColumnKind::kDestination, column,
      destination_ref.GetReferencedNodeTable()->Name());
}

absl::Status FindPropertyGraphOutgoingColumn(const PropertyGraph& graph,
                                             const GraphEdgeTable& edge_table,
                                             const Column** column) {
  const GraphNodeTableReference& source_ref =
      GetSourceNodeTableReference(edge_table);
  return FindPropertyGraphSqlColumn(
      *source_ref.GetReferencedNodeTable()->GetTable(), graph.Name(),
      PropertyGraphSqlColumnKind::kOutgoing, column, edge_table.Name());
}

absl::Status FindPropertyGraphIncomingColumn(const PropertyGraph& graph,
                                             const GraphEdgeTable& edge_table,
                                             const Column** column) {
  const GraphNodeTableReference& destination_ref =
      GetDestinationNodeTableReference(edge_table);
  return FindPropertyGraphSqlColumn(
      *destination_ref.GetReferencedNodeTable()->GetTable(), graph.Name(),
      PropertyGraphSqlColumnKind::kIncoming, column, edge_table.Name());
}

absl::Status FindPropertyGraphRelationSqlColumns(
    const PropertyGraph& graph, const GraphEdgeTable& edge_table,
    PropertyGraphRelationSqlColumns& columns) {
  GOOGLESQL_RETURN_IF_ERROR(
      FindPropertyGraphSourceColumn(graph, edge_table, &columns.source));
  GOOGLESQL_RETURN_IF_ERROR(FindPropertyGraphDestinationColumn(
      graph, edge_table, &columns.destination));
  GOOGLESQL_RETURN_IF_ERROR(
      FindPropertyGraphOutgoingColumn(graph, edge_table, &columns.outgoing));
  GOOGLESQL_RETURN_IF_ERROR(
      FindPropertyGraphIncomingColumn(graph, edge_table, &columns.incoming));
  return absl::OkStatus();
}

PropertyGraphRelationMetadata GetPropertyGraphRelationMetadata(
    const GraphEdgeTable& edge_table) {
  if (const PropertyGraphRelationMetadata* relation_metadata =
          edge_table.GetRelationMetadata();
      relation_metadata != nullptr) {
    PropertyGraphRelationMetadata metadata = *relation_metadata;
    metadata.edge_table = &edge_table;
    if (metadata.source_node_table == nullptr) {
      metadata.source_node_table =
          GetSourceNodeTableReference(edge_table).GetReferencedNodeTable();
    }
    if (metadata.destination_node_table == nullptr) {
      metadata.destination_node_table =
          GetDestinationNodeTableReference(edge_table).GetReferencedNodeTable();
    }
    return metadata;
  }
  const GraphNodeTableReference& source_ref =
      GetSourceNodeTableReference(edge_table);
  const GraphNodeTableReference& destination_ref =
      GetDestinationNodeTableReference(edge_table);
  return PropertyGraphRelationMetadata{
      .edge_table = &edge_table,
      .source_node_table = source_ref.GetReferencedNodeTable(),
      .destination_node_table = destination_ref.GetReferencedNodeTable(),
      .source_exposure_name = source_ref.GetReferencedNodeTable()->Name(),
      .destination_exposure_name = destination_ref.GetReferencedNodeTable()->Name(),
      .outgoing_exposure_name = edge_table.Name(),
      .incoming_exposure_name = edge_table.Name(),
      .source_is_multi = false,
      .destination_is_multi = false,
      .outgoing_is_multi = true,
      .incoming_is_multi = true,
  };
}

PropertyGraphElementMetadata GetPropertyGraphElementMetadata(
    const GraphElementTable& element_table) {
  PropertyGraphElementMetadata metadata;
  metadata.element_table = &element_table;
  metadata.kind = element_table.kind();
  metadata.labels = GetLabelsInDeclarationOrder(element_table);
  metadata.property_declarations =
      GetPropertyDeclarationsInDeclarationOrder(element_table);
  metadata.has_dynamic_label = element_table.HasDynamicLabel();
  metadata.has_dynamic_properties = element_table.HasDynamicProperties();
  if (const GraphEdgeTable* edge_table = element_table.AsEdgeTable();
      edge_table != nullptr) {
    metadata.relation = GetPropertyGraphRelationMetadata(*edge_table);
  }
  return metadata;
}

absl::Status FindPropertyGraphElementSqlColumns(
    const PropertyGraph& graph, const GraphElementTable& element_table,
    PropertyGraphElementSqlColumns& columns) {
  columns = PropertyGraphElementSqlColumns{};
  const std::vector<const GraphPropertyDeclaration*> property_declarations =
      GetPropertyDeclarationsInDeclarationOrder(element_table);
  columns.property_columns.reserve(property_declarations.size());
  for (const GraphPropertyDeclaration* property_declaration :
       property_declarations) {
    const Column* column = nullptr;
    GOOGLESQL_RETURN_IF_ERROR(FindPropertyGraphPropertyColumn(
        graph, element_table, *property_declaration, &column));
    columns.property_columns.emplace_back(property_declaration, column);
  }
  if (element_table.HasDynamicLabel()) {
    GOOGLESQL_RETURN_IF_ERROR(
        FindPropertyGraphDynamicLabelColumn(graph, element_table,
                                            &columns.dynamic_label));
  }
  if (element_table.HasDynamicProperties()) {
    GOOGLESQL_RETURN_IF_ERROR(FindPropertyGraphDynamicPropertiesColumn(
        graph, element_table, &columns.dynamic_properties));
  }
  return absl::OkStatus();
}

}  // namespace googlesql
