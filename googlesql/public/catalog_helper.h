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

#ifndef GOOGLESQL_PUBLIC_CATALOG_HELPER_H_
#define GOOGLESQL_PUBLIC_CATALOG_HELPER_H_

#include "googlesql/public/catalog.h"
#include "googlesql/public/property_graph.h"
#include "absl/status/status.h"
#include <optional>
#include <string>
#include <vector>

#include "googlesql/public/types/enum_type.h"
#include "absl/strings/string_view.h"

namespace googlesql {

enum class PropertyGraphSqlColumnKind {
  kProperty,
  kDynamicLabel,
  kDynamicProperties,
  kSource,
  kDestination,
  kOutgoing,
  kIncoming,
};

struct PropertyGraphRelationSqlColumns {
  const Column* source = nullptr;
  const Column* destination = nullptr;
  const Column* outgoing = nullptr;
  const Column* incoming = nullptr;
};

struct PropertyGraphRelationMetadata {
  const GraphEdgeTable* edge_table = nullptr;
  const GraphNodeTable* source_node_table = nullptr;
  const GraphNodeTable* destination_node_table = nullptr;
  std::string source_exposure_name;
  std::string destination_exposure_name;
  std::string outgoing_exposure_name;
  std::string incoming_exposure_name;
  bool source_is_multi = false;
  bool destination_is_multi = false;
  bool outgoing_is_multi = true;
  bool incoming_is_multi = true;
};

struct PropertyGraphElementSqlColumns {
  std::vector<std::pair<const GraphPropertyDeclaration*, const Column*>>
      property_columns;
  const Column* dynamic_label = nullptr;
  const Column* dynamic_properties = nullptr;
};

struct PropertyGraphElementMetadata {
  const GraphElementTable* element_table = nullptr;
  GraphElementTable::Kind kind = GraphElementTable::Kind::kNode;
  std::vector<const GraphElementLabel*> labels;
  std::vector<const GraphPropertyDeclaration*> property_declarations;
  bool has_dynamic_label = false;
  bool has_dynamic_properties = false;
  std::optional<PropertyGraphRelationMetadata> relation = std::nullopt;
};

// Returns an entry from <possible_names> with the least edit-distance from
// <mistyped_name> (we allow a maximum edit-distance of ~20%), if one exists.
// Edit distance is computed case-sensitively.
// Internal names (with prefix '$') are excluded as possible suggestions.
//
// Returns an empty string if none of the entries in <possible_names> are
// within the allowed edit-distance.
std::string ClosestName(absl::string_view mistyped_name,
                        const std::vector<std::string>& possible_names);

// Returns a string that matches a value in `type` the is a near match
// to `mistyped_value`. If no close match is found, returns empty string.
//
// Uses case insensitive comparison, although enums are usually all
// upper-case.
//
// Note: this is a reasonable implementation of Catalog::SuggestEnumValue.
std::string SuggestEnumValue(const EnumType* type,
                             absl::string_view mistyped_value);

std::string GetPropertyGraphSqlColumnName(
    absl::string_view graph_name, PropertyGraphSqlColumnKind kind,
    absl::string_view semantic_name = "");

absl::Status FindPropertyGraphSqlColumn(
    const Table& table, absl::string_view graph_name,
    PropertyGraphSqlColumnKind kind, const Column** column,
    absl::string_view semantic_name = "");

absl::Status FindPropertyGraphSqlColumn(
    const PropertyGraph& graph, const GraphElementTable& element_table,
    PropertyGraphSqlColumnKind kind, const Column** column,
    absl::string_view semantic_name = "");

absl::Status FindPropertyGraphPropertyColumn(
    const PropertyGraph& graph, const GraphElementTable& element_table,
    const GraphPropertyDeclaration& property_declaration, const Column** column);

absl::Status FindPropertyGraphDynamicLabelColumn(
    const PropertyGraph& graph, const GraphElementTable& element_table,
    const Column** column);

absl::Status FindPropertyGraphDynamicPropertiesColumn(
    const PropertyGraph& graph, const GraphElementTable& element_table,
    const Column** column);

absl::Status FindPropertyGraphSourceColumn(const PropertyGraph& graph,
                                           const GraphEdgeTable& edge_table,
                                           const Column** column);

absl::Status FindPropertyGraphDestinationColumn(const PropertyGraph& graph,
                                                const GraphEdgeTable& edge_table,
                                                const Column** column);

absl::Status FindPropertyGraphOutgoingColumn(const PropertyGraph& graph,
                                             const GraphEdgeTable& edge_table,
                                             const Column** column);

absl::Status FindPropertyGraphIncomingColumn(const PropertyGraph& graph,
                                             const GraphEdgeTable& edge_table,
                                             const Column** column);

absl::Status FindPropertyGraphRelationSqlColumns(
    const PropertyGraph& graph, const GraphEdgeTable& edge_table,
    PropertyGraphRelationSqlColumns& columns);

PropertyGraphRelationMetadata GetPropertyGraphRelationMetadata(
    const GraphEdgeTable& edge_table);

PropertyGraphElementMetadata GetPropertyGraphElementMetadata(
    const GraphElementTable& element_table);

absl::Status FindPropertyGraphElementSqlColumns(
    const PropertyGraph& graph, const GraphElementTable& element_table,
    PropertyGraphElementSqlColumns& columns);

}  // namespace googlesql

#endif  // GOOGLESQL_PUBLIC_CATALOG_HELPER_H_
