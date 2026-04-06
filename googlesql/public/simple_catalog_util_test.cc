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

#include "googlesql/public/simple_catalog_util.h"

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "googlesql/base/testing/status_matchers.h"
#include "googlesql/public/analyzer.h"
#include "googlesql/public/analyzer_options.h"
#include "googlesql/analyzer/graph_query_resolver.h"
#include "googlesql/public/catalog.h"
#include "googlesql/public/catalog_helper.h"
#include "googlesql/public/function.h"
#include "googlesql/public/function_signature.h"
#include "googlesql/public/options.pb.h"
#include "googlesql/public/simple_catalog.h"
#include "googlesql/proto/simple_catalog.pb.h"
#include "googlesql/public/table_valued_function.h"
#include "googlesql/public/types/type.h"
#include "googlesql/public/value.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "googlesql/resolved_ast/resolved_node_kind.pb.h"
#include "googlesql/resolved_ast/sql_builder.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "googlesql/base/status_macros.h"

using ::testing::HasSubstr;
using ::testing::MatchesRegex;
using ::testing::Not;
using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;

namespace googlesql {

TEST(SimpleCatalogUtilTest, PropertyGraphSqlColumnNameHelpers) {
  EXPECT_EQ(GetPropertyGraphSqlColumnName("fin_graph",
                                          PropertyGraphSqlColumnKind::kProperty,
                                          "total_id"),
            "fin_graph_property_total_id");
  EXPECT_EQ(SimpleCatalog::GetPropertyGraphSqlColumnName(
                "fin_graph", SimpleCatalog::PropertyGraphSqlColumnKind::kProperty,
                "total_id"),
            "fin_graph_property_total_id");
  EXPECT_EQ(SimpleCatalog::GetPropertyGraphSqlColumnName(
                "fin_graph",
                SimpleCatalog::PropertyGraphSqlColumnKind::kDynamicLabel),
            "fin_graph_dynamic_label");
  EXPECT_EQ(SimpleCatalog::GetPropertyGraphSqlColumnName(
                "fin_graph", SimpleCatalog::PropertyGraphSqlColumnKind::kOutgoing,
                "owns"),
            "fin_graph_outgoing_owns");
}

TEST(SimpleCatalogUtilTest, AddFunctionFromCreateFunctionTest) {
  SimpleCatalog simple("simple");
  AnalyzerOptions analyzer_options;
  std::unique_ptr<const AnalyzerOutput> analyzer_output;

  // Invalid analyzer options
  EXPECT_THAT(
      AddFunctionFromCreateFunction(
          "CREATE TEMP FUNCTION Basic() AS (1)", analyzer_options,
          /*allow_persistent_function=*/false,
          /*function_options=*/nullptr, analyzer_output, simple, simple),
      Not(IsOk()));

  analyzer_options.mutable_language()->AddSupportedStatementKind(
      RESOLVED_CREATE_FUNCTION_STMT);
  GOOGLESQL_EXPECT_OK(AddFunctionFromCreateFunction(
      "CREATE TEMP FUNCTION Basic() AS (1)", analyzer_options,
      /*allow_persistent_function=*/false, /*function_options=*/nullptr,
      analyzer_output, simple, simple));
  const Function* function;
  GOOGLESQL_EXPECT_OK(simple.FindFunction({"Basic"}, &function));
  EXPECT_EQ(function->FullName(), "Lazy_resolution_function:Basic");

  // Duplicate
  EXPECT_THAT(
      AddFunctionFromCreateFunction(
          "CREATE TEMP FUNCTION Basic() AS (1)", analyzer_options,
          /*allow_persistent_function=*/false,
          /*function_options=*/nullptr, analyzer_output, simple, simple),
      Not(IsOk()));

  // Invalid persistent function.
  EXPECT_THAT(
      AddFunctionFromCreateFunction(
          "CREATE FUNCTION Persistent() AS (1)", analyzer_options,
          /*allow_persistent_function=*/false,
          /*function_options=*/nullptr, analyzer_output, simple, simple),
      Not(IsOk()));
  GOOGLESQL_EXPECT_OK(AddFunctionFromCreateFunction(
      "CREATE FUNCTION Persistent() AS (1)", analyzer_options,
      /*allow_persistent_function=*/true, /*function_options=*/nullptr,
      analyzer_output, simple, simple));

  // Analysis failure
  EXPECT_THAT(
      AddFunctionFromCreateFunction(
          "CREATE TEMP FUNCTION Template(arg ANY TYPE) AS (arg)",
          analyzer_options, /*allow_persistent_function=*/false,
          /*function_options=*/nullptr, analyzer_output, simple, simple),
      Not(IsOk()));
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_TEMPLATE_FUNCTIONS);
  GOOGLESQL_EXPECT_OK(AddFunctionFromCreateFunction(
      "CREATE TEMP FUNCTION Template(arg ANY TYPE) AS (arg)", analyzer_options,
      /*allow_persistent_function=*/false, /*function_options=*/nullptr,
      analyzer_output, simple, simple));
  GOOGLESQL_EXPECT_OK(simple.FindFunction({"Template"}, &function));
  EXPECT_EQ(function->FullName(), "Templated_SQL_Function:Template");

  // Different resolving catalog.
  std::unique_ptr<googlesql::SimpleConstant> constant;
  GOOGLESQL_ASSERT_OK(
      SimpleConstant::Create({"TestConstant"}, Value::Int32(42), &constant));
  SimpleCatalog resolving_catalog("resolving");
  resolving_catalog.AddOwnedConstant("TestConstant", std::move(constant));
  GOOGLESQL_EXPECT_OK(AddFunctionFromCreateFunction(
      "CREATE TEMP FUNCTION MyFunc() RETURNS INT32 AS (TestConstant)",
      analyzer_options,
      /*allow_persistent_function=*/false, /*function_options=*/nullptr,
      analyzer_output, resolving_catalog, simple));
  GOOGLESQL_EXPECT_OK(simple.FindFunction({"MyFunc"}, &function));
  EXPECT_EQ(function->FullName(), "Lazy_resolution_function:MyFunc");
  EXPECT_THAT(resolving_catalog.FindFunction({"MyFunc"}, &function),
              Not(IsOk()));

  GOOGLESQL_EXPECT_OK(AddFunctionFromCreateFunction(
      "CREATE TEMP FUNCTION NonSQL(x INT64) RETURNS DOUBLE LANGUAGE C",
      analyzer_options,
      /*allow_persistent_function=*/false, /*function_options=*/nullptr,
      analyzer_output, simple, simple));
  GOOGLESQL_EXPECT_OK(simple.FindFunction({"NonSQL"}, &function));
  EXPECT_EQ(function->FullName(), "External_function:NonSQL");
}

TEST(SimpleCatalogUtilTest, MakeFunctionFromCreateFunctionBasic) {
  SimpleCatalog catalog("simple");
  AnalyzerOptions analyzer_options;
  analyzer_options.mutable_language()->AddSupportedStatementKind(
      RESOLVED_CREATE_FUNCTION_STMT);

  TypeFactory type_factory;
  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  GOOGLESQL_EXPECT_OK(AnalyzeStatement("CREATE TEMP FUNCTION Basic() AS (1)",
                             analyzer_options, &catalog, &type_factory,
                             &analyzer_output));

  ASSERT_TRUE(
      analyzer_output->resolved_statement()->Is<ResolvedCreateFunctionStmt>());

  const ResolvedCreateFunctionStmt* create_function_stmt =
      analyzer_output->resolved_statement()
          ->GetAs<ResolvedCreateFunctionStmt>();

  FunctionOptions function_options;
  function_options.set_is_deprecated(true);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Function> function,
      MakeFunctionFromCreateFunction(*create_function_stmt, &function_options));
  EXPECT_EQ(function->Name(), "Basic");
  EXPECT_TRUE(function->IsDeprecated());
  EXPECT_EQ(function->mode(), Function::SCALAR);
  EXPECT_EQ(function->NumSignatures(), 1);

  // Use string comparison as a proxy for signature equality.
  EXPECT_EQ(FunctionSignature::SignaturesToString(function->signatures()),
            FunctionSignature::SignaturesToString(
                {create_function_stmt->signature()}));
}

TEST(SimpleCatalogUtilTest, MakeFunctionFromCreateFunctionAgg) {
  SimpleCatalog catalog("simple");
  AnalyzerOptions analyzer_options;
  analyzer_options.mutable_language()->AddSupportedStatementKind(
      RESOLVED_CREATE_FUNCTION_STMT);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_CREATE_AGGREGATE_FUNCTION);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_TEMPLATE_FUNCTIONS);

  TypeFactory type_factory;
  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  GOOGLESQL_EXPECT_OK(AnalyzeStatement(
      "CREATE AGGREGATE FUNCTION Path.to.F(x any type) AS (sum(x))",
      analyzer_options, &catalog, &type_factory, &analyzer_output));

  ASSERT_TRUE(
      analyzer_output->resolved_statement()->Is<ResolvedCreateFunctionStmt>());

  const ResolvedCreateFunctionStmt* create_function_stmt =
      analyzer_output->resolved_statement()
          ->GetAs<ResolvedCreateFunctionStmt>();

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Function> function,
      MakeFunctionFromCreateFunction(*create_function_stmt,
                                     /*function_options=*/nullptr));
  EXPECT_EQ(function->Name(), "F");
  EXPECT_THAT(function->FunctionNamePath(),
              testing::ElementsAre("Path", "to", "F"));
  EXPECT_EQ(function->mode(), Function::AGGREGATE);
  EXPECT_EQ(function->NumSignatures(), 1);

  // Use string comparison as a proxy for signature equality.
  EXPECT_EQ(FunctionSignature::SignaturesToString(function->signatures()),
            FunctionSignature::SignaturesToString(
                {create_function_stmt->signature()}));
}

TEST(SimpleCatalogUtilTest, AddTableFromCreateTable) {
  SimpleCatalog catalog("simple");
  SimpleTable* table;
  AnalyzerOptions analyzer_options;
  std::unique_ptr<const AnalyzerOutput> analyzer_output;

  const char* create_t1 = "CREATE TEMP TABLE t1 (x INT64)";

  // Invalid analyzer options
  EXPECT_THAT(AddTableFromCreateTable(create_t1, analyzer_options,
                                      /*allow_non_temp=*/false, analyzer_output,
                                      table, catalog),
              Not(IsOk()));

  analyzer_options.mutable_language()->AddSupportedStatementKind(
      RESOLVED_CREATE_TABLE_STMT);
  GOOGLESQL_EXPECT_OK(AddTableFromCreateTable(create_t1, analyzer_options,
                                    /*allow_non_temp=*/false, analyzer_output,
                                    table, catalog));

  // Duplicate table.
  EXPECT_THAT(AddTableFromCreateTable(create_t1, analyzer_options,
                                      /*allow_non_temp=*/false, analyzer_output,
                                      table, catalog),
              Not(IsOk()));

  const char* create_t2 = "CREATE TABLE t2 (x INT64)";

  // Invalid persistent table.
  EXPECT_THAT(AddTableFromCreateTable(create_t2, analyzer_options,
                                      /*allow_non_temp=*/false, analyzer_output,
                                      table, catalog),
              Not(IsOk()));

  GOOGLESQL_EXPECT_OK(AddTableFromCreateTable(create_t2, analyzer_options,
                                    /*allow_non_temp=*/true, analyzer_output,
                                    table, catalog));

  // Check the table got created correctly.
  const Table* found_table;
  GOOGLESQL_EXPECT_OK(catalog.FindTable({"t2"}, &found_table));
  EXPECT_EQ(table, found_table);
  EXPECT_EQ(found_table->Name(), "t2");
  EXPECT_EQ(found_table->FullName(), "t2");
  EXPECT_EQ(found_table->NumColumns(), 1);
  const Column* found_column = found_table->GetColumn(0);
  EXPECT_EQ(found_column->Name(), "x");
  EXPECT_EQ(found_column->GetType()->DebugString(), "INT64");

  // Check we get an error if the CREATE has any modifiers that aren't
  // handled by AddTableFromCreateTable.
  const char* create_t3 = "CREATE TEMP TABLE t3 (x INT64) OPTIONS(opt=2)";

  EXPECT_THAT(AddTableFromCreateTable(create_t3, analyzer_options,
                                      /*allow_non_temp=*/false, analyzer_output,
                                      table, catalog),
              StatusIs(absl::StatusCode::kUnimplemented));
}

TEST(SimpleCatalogUtilTest, AddTVFFromCreateTableFunction) {
  SimpleCatalog catalog("simple");
  AnalyzerOptions analyzer_options;
  std::unique_ptr<const AnalyzerOutput> analyzer_output;

  // Non-templated SQL TVF, with TEMP.
  const char* create_tvf1 =
      "CREATE TEMP TABLE FUNCTION tvf1(x INT64) AS (SELECT x)";

  // Invalid analyzer options
  EXPECT_THAT(AddTVFFromCreateTableFunction(create_tvf1, analyzer_options,
                                            /*allow_persistent=*/false,
                                            analyzer_output, catalog),
              Not(IsOk()));

  analyzer_options.mutable_language()->AddSupportedStatementKind(
      RESOLVED_CREATE_TABLE_FUNCTION_STMT);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_CREATE_TABLE_FUNCTION);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_TEMPLATE_FUNCTIONS);
  GOOGLESQL_EXPECT_OK(AddTVFFromCreateTableFunction(create_tvf1, analyzer_options,
                                          /*allow_persistent=*/false,
                                          analyzer_output, catalog));

  // Check the TVF got created correctly.
  const TableValuedFunction* found_tvf;
  GOOGLESQL_EXPECT_OK(catalog.FindTableValuedFunction({"tvf1"}, &found_tvf));
  EXPECT_EQ(found_tvf->Name(), "tvf1");
  EXPECT_EQ(found_tvf->FullName(), "tvf1");
  EXPECT_EQ(found_tvf->NumSignatures(), 1);
  EXPECT_EQ(found_tvf->GetSignature(0)->DebugString(),
            "(INT64 x) -> TABLE<x INT64>");

  // Duplicate TVF.
  EXPECT_THAT(AddTVFFromCreateTableFunction(create_tvf1, analyzer_options,
                                            /*allow_persistent=*/false,
                                            analyzer_output, catalog),
              Not(IsOk()));

  // Templated SQL TVF, without TEMP.
  const char* create_tvf2 =
      "CREATE TABLE FUNCTION tvf2(t ANY TABLE) AS (SELECT * FROM t)";

  // Invalid non-TEMP table.
  EXPECT_THAT(AddTVFFromCreateTableFunction(create_tvf2, analyzer_options,
                                            /*allow_persistent=*/false,
                                            analyzer_output, catalog),
              Not(IsOk()));

  // Allowed if allow_persistent is true.
  GOOGLESQL_EXPECT_OK(AddTVFFromCreateTableFunction(create_tvf2, analyzer_options,
                                          /*allow_persistent=*/true,
                                          analyzer_output, catalog));

  // Non-SQL TVF with a fixed output schema.
  const char* create_tvf3 =
      "CREATE TABLE FUNCTION tvf3(t ANY TABLE) RETURNS TABLE<x INT64>";

  GOOGLESQL_EXPECT_OK(AddTVFFromCreateTableFunction(create_tvf3, analyzer_options,
                                          /*allow_persistent=*/true,
                                          analyzer_output, catalog));

  // Non-SQL TVF without a fixed output schema.
  const char* create_tvf4 = "CREATE TABLE FUNCTION tvf4(t ANY TABLE)";

  EXPECT_THAT(
      AddTVFFromCreateTableFunction(create_tvf4, analyzer_options,
                                    /*allow_persistent=*/true, analyzer_output,
                                    catalog),
      StatusIs(
          absl::StatusCode::kInternal,
          MatchesRegex(
              ".*Only TVFs with fixed output table schemas are supported.*")));

  // Check we get an error if the CREATE has any modifiers that aren't
  // handled by AddTVFFromCreateTableFunction.
  const char* create_tvf5 =
      "CREATE TABLE FUNCTION tvf2(t ANY TABLE) "
      "OPTIONS (opt=5) "
      "AS (SELECT * FROM t)";

  EXPECT_THAT(AddTVFFromCreateTableFunction(create_tvf5, analyzer_options,
                                            /*allow_persistent=*/true,
                                            analyzer_output, catalog),
              StatusIs(absl::StatusCode::kUnimplemented));
}

TEST(SimpleCatalogUtilTest, AddPropertyGraphAddsSqlNavigationColumns) {
  SimpleCatalog catalog("simple");
  catalog.AddOwnedTable(std::make_unique<SimpleTable>(
      "person", std::vector<SimpleTable::NameAndType>{{"id", types::Int64Type()},
                                                      {"name", types::StringType()}}));
  catalog.AddOwnedTable(std::make_unique<SimpleTable>(
      "account",
      std::vector<SimpleTable::NameAndType>{{"id", types::Int64Type()}}));
  catalog.AddOwnedTable(std::make_unique<SimpleTable>(
      "owns", std::vector<SimpleTable::NameAndType>{{"person_id", types::Int64Type()},
                                                    {"account_id", types::Int64Type()}}));

  AnalyzerOptions analyzer_options;
  analyzer_options.mutable_language()->EnableLanguageFeature(FEATURE_SQL_GRAPH);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_ENABLE_MEASURES);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_SQL_GRAPH_DEFAULT_LABEL_AND_PROPERTY_DEFINITION_OPTIONS);
  analyzer_options.mutable_language()->AddSupportedStatementKind(
      RESOLVED_CREATE_PROPERTY_GRAPH_STMT);
  std::vector<std::unique_ptr<const AnalyzerOutput>> artifacts;

  GOOGLESQL_ASSERT_OK(AddPropertyGraphFromCreatePropertyGraphStmt(R"sql(
      CREATE PROPERTY GRAPH fin_graph
      NODE TABLES (
        person KEY(id)
          LABEL person PROPERTIES (
            UPPER(name) AS upper_name OPTIONS(
              description = "Upper-cased name",
              display_name = "Upper Name",
              semantic_role = "dimension",
              semantic_aliases = ["upper", "display_name"]
            ),
            MEASURE(SUM(id)) AS total_id OPTIONS(
              description = "Total id measure",
              semantic_role = "metric",
              hidden = true
            )
          ),
        account KEY(id)
      )
      EDGE TABLES (
        owns KEY(person_id, account_id)
          OPTIONS(
            source_name = "owner",
            destination_name = "account_owner_target",
            outgoing_name = "ownerships",
            incoming_name = "owned_by",
            outgoing_is_multi = false,
            incoming_is_multi = true
          )
          SOURCE KEY(person_id) REFERENCES person(id)
          DESTINATION KEY(account_id) REFERENCES account(id)
      )
      )sql",
      analyzer_options, artifacts, catalog));

  const Table* person;
  const Table* account;
  const Table* owns;
  GOOGLESQL_ASSERT_OK(catalog.FindTable({"person"}, &person));
  GOOGLESQL_ASSERT_OK(catalog.FindTable({"account"}, &account));
  GOOGLESQL_ASSERT_OK(catalog.FindTable({"owns"}, &owns));
  const PropertyGraph* fin_graph;
  GOOGLESQL_ASSERT_OK(catalog.FindPropertyGraph({"fin_graph"}, &fin_graph));
  const GraphElementTable* person_element_table;
  const GraphElementTable* owns_element_table;
  GOOGLESQL_ASSERT_OK(
      fin_graph->FindElementTableByName("person", person_element_table));
  GOOGLESQL_ASSERT_OK(fin_graph->FindElementTableByName("owns", owns_element_table));

  const Column* outgoing = nullptr;
  GOOGLESQL_ASSERT_OK(catalog.FindPropertyGraphSqlColumn(
      *person, "fin_graph",
      SimpleCatalog::PropertyGraphSqlColumnKind::kOutgoing, &outgoing,
      "owns"));
  ASSERT_NE(outgoing, nullptr);
  ASSERT_TRUE(outgoing->GetJoinColumn().has_value());
  EXPECT_TRUE(outgoing->GetJoinColumn()->is_multi());
  EXPECT_EQ(outgoing->GetJoinColumn()->target_table(), owns);

  const Column* incoming = nullptr;
  GOOGLESQL_ASSERT_OK(catalog.FindPropertyGraphSqlColumn(
      *account, "fin_graph",
      SimpleCatalog::PropertyGraphSqlColumnKind::kIncoming, &incoming,
      "owns"));
  ASSERT_NE(incoming, nullptr);
  ASSERT_TRUE(incoming->GetJoinColumn().has_value());
  EXPECT_TRUE(incoming->GetJoinColumn()->is_multi());
  EXPECT_EQ(incoming->GetJoinColumn()->target_table(), owns);

  const Column* source = nullptr;
  GOOGLESQL_ASSERT_OK(catalog.FindPropertyGraphSqlColumn(
      *owns, "fin_graph", SimpleCatalog::PropertyGraphSqlColumnKind::kSource,
      &source, "person"));
  ASSERT_NE(source, nullptr);
  ASSERT_TRUE(source->GetJoinColumn().has_value());
  EXPECT_FALSE(source->GetJoinColumn()->is_multi());
  EXPECT_EQ(source->GetJoinColumn()->target_table(), person);

  const Column* destination = nullptr;
  GOOGLESQL_ASSERT_OK(catalog.FindPropertyGraphSqlColumn(
      *owns, "fin_graph",
      SimpleCatalog::PropertyGraphSqlColumnKind::kDestination, &destination,
      "account"));
  ASSERT_NE(destination, nullptr);
  ASSERT_TRUE(destination->GetJoinColumn().has_value());
  EXPECT_FALSE(destination->GetJoinColumn()->is_multi());
  EXPECT_EQ(destination->GetJoinColumn()->target_table(), account);

  const Column* upper_name = nullptr;
  GOOGLESQL_ASSERT_OK(catalog.FindPropertyGraphSqlColumn(
      *person, "fin_graph",
      SimpleCatalog::PropertyGraphSqlColumnKind::kProperty, &upper_name,
      "upper_name"));
  ASSERT_NE(upper_name, nullptr);
  ASSERT_TRUE(upper_name->HasGeneratedExpression());
  EXPECT_EQ(upper_name->GetExpression()->GetExpressionString(), "UPPER(name)");
  const GraphPropertyDefinition* upper_name_definition = nullptr;
  GOOGLESQL_ASSERT_OK(person_element_table->FindPropertyDefinitionByName(
      "upper_name", upper_name_definition));
  ASSERT_NE(upper_name_definition, nullptr);
  EXPECT_EQ(upper_name_definition->SemanticMetadata().description,
            "Upper-cased name");
  EXPECT_EQ(upper_name_definition->SemanticMetadata().display_name,
            "Upper Name");
  EXPECT_EQ(upper_name_definition->SemanticMetadata().semantic_role,
            "dimension");
  EXPECT_THAT(upper_name_definition->SemanticMetadata().semantic_aliases,
              testing::ElementsAre("upper", "display_name"));
  EXPECT_FALSE(upper_name_definition->SemanticMetadata().hidden);

  const Column* total_id = nullptr;
  GOOGLESQL_ASSERT_OK(catalog.FindPropertyGraphSqlColumn(
      *person, "fin_graph",
      SimpleCatalog::PropertyGraphSqlColumnKind::kProperty, &total_id,
      "total_id"));
  ASSERT_NE(total_id, nullptr);
  ASSERT_TRUE(total_id->HasMeasureExpression());
  EXPECT_EQ(total_id->GetExpression()->GetExpressionString(), "SUM(id)");
  EXPECT_THAT(total_id->GetExpression()->RowIdentityColumns(),
              testing::Optional(std::vector<int>{0}));
  const GraphPropertyDefinition* total_id_definition = nullptr;
  GOOGLESQL_ASSERT_OK(person_element_table->FindPropertyDefinitionByName(
      "total_id", total_id_definition));
  ASSERT_NE(total_id_definition, nullptr);
  EXPECT_EQ(total_id_definition->SemanticMetadata().description,
            "Total id measure");
  EXPECT_EQ(total_id_definition->SemanticMetadata().semantic_role, "metric");
  EXPECT_TRUE(total_id_definition->SemanticMetadata().hidden);

  const Column* upper_name_via_graph = nullptr;
  GOOGLESQL_ASSERT_OK(catalog.FindPropertyGraphSqlColumn(
      *fin_graph, *person_element_table,
      SimpleCatalog::PropertyGraphSqlColumnKind::kProperty,
      &upper_name_via_graph, "upper_name"));
  EXPECT_EQ(upper_name_via_graph, upper_name);

  const Column* upper_name_via_helper = nullptr;
  GOOGLESQL_ASSERT_OK(googlesql::FindPropertyGraphSqlColumn(
      *fin_graph, *person_element_table, PropertyGraphSqlColumnKind::kProperty,
      &upper_name_via_helper, "upper_name"));
  EXPECT_EQ(upper_name_via_helper, upper_name);
  const GraphPropertyDeclaration* upper_name_declaration = nullptr;
  GOOGLESQL_ASSERT_OK(
      fin_graph->FindPropertyDeclarationByName("upper_name",
                                               upper_name_declaration));
  ASSERT_NE(upper_name_declaration, nullptr);
  const PropertyGraphElementMetadata person_metadata =
      googlesql::GetPropertyGraphElementMetadata(*person_element_table);
  EXPECT_EQ(person_metadata.element_table, person_element_table);
  EXPECT_EQ(person_metadata.kind, GraphElementTable::Kind::kNode);
  ASSERT_EQ(person_metadata.labels.size(), 1);
  EXPECT_EQ(person_metadata.labels[0]->Name(), "person");
  ASSERT_EQ(person_metadata.property_declarations.size(), 2);
  EXPECT_EQ(person_metadata.property_declarations[0]->Name(), "upper_name");
  EXPECT_EQ(person_metadata.property_declarations[1]->Name(), "total_id");
  EXPECT_FALSE(person_metadata.has_dynamic_label);
  EXPECT_FALSE(person_metadata.has_dynamic_properties);
  EXPECT_FALSE(person_metadata.relation.has_value());
  const Column* upper_name_via_declaration = nullptr;
  GOOGLESQL_ASSERT_OK(googlesql::FindPropertyGraphPropertyColumn(
      *fin_graph, *person_element_table, *upper_name_declaration,
      &upper_name_via_declaration));
  EXPECT_EQ(upper_name_via_declaration, upper_name);
  const Column* upper_name_via_catalog_declaration = nullptr;
  GOOGLESQL_ASSERT_OK(catalog.FindPropertyGraphPropertyColumn(
      *fin_graph, *person_element_table, *upper_name_declaration,
      &upper_name_via_catalog_declaration));
  EXPECT_EQ(upper_name_via_catalog_declaration, upper_name);
  PropertyGraphElementSqlColumns person_sql_columns;
  GOOGLESQL_ASSERT_OK(googlesql::FindPropertyGraphElementSqlColumns(
      *fin_graph, *person_element_table, person_sql_columns));
  ASSERT_EQ(person_sql_columns.property_columns.size(), 2);
  EXPECT_EQ(person_sql_columns.property_columns[0].first->Name(), "upper_name");
  EXPECT_EQ(person_sql_columns.property_columns[0].second, upper_name);
  EXPECT_EQ(person_sql_columns.property_columns[1].first->Name(), "total_id");
  EXPECT_EQ(person_sql_columns.property_columns[1].second, total_id);
  EXPECT_EQ(person_sql_columns.dynamic_label, nullptr);
  EXPECT_EQ(person_sql_columns.dynamic_properties, nullptr);
  PropertyGraphElementSqlColumns person_sql_columns_via_catalog;
  GOOGLESQL_ASSERT_OK(catalog.FindPropertyGraphElementSqlColumns(
      *fin_graph, *person_element_table, person_sql_columns_via_catalog));
  ASSERT_EQ(person_sql_columns_via_catalog.property_columns.size(), 2);
  EXPECT_EQ(person_sql_columns_via_catalog.property_columns[0].second,
            upper_name);
  EXPECT_EQ(person_sql_columns_via_catalog.property_columns[1].second,
            total_id);

  const Column* destination_via_graph = nullptr;
  GOOGLESQL_ASSERT_OK(catalog.FindPropertyGraphSqlColumn(
      *fin_graph, *owns_element_table,
      SimpleCatalog::PropertyGraphSqlColumnKind::kDestination,
      &destination_via_graph, "account"));
  EXPECT_EQ(destination_via_graph, destination);

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      PropertyGraphNavigationBinding destination_binding,
      googlesql::FindPropertyGraphNavigationBinding(*fin_graph,
                                                    *owns_element_table,
                                                    "account"));
  EXPECT_EQ(destination_binding.navigation_kind,
            PropertyGraphNavigationKind::kDestination);
  EXPECT_EQ(destination_binding.edge_table, owns_element_table->AsEdgeTable());
  ASSERT_NE(destination_binding.target_element_table, nullptr);
  EXPECT_EQ(destination_binding.target_element_table->Name(), "account");
  EXPECT_FALSE(destination_binding.is_multi);

  const std::vector<PropertyGraphNavigationBinding> person_navigations =
      googlesql::GetPropertyGraphNavigationBindings(*fin_graph,
                                                    *person_element_table);
  ASSERT_EQ(person_navigations.size(), 1);
  EXPECT_EQ(person_navigations[0].navigation_kind,
            PropertyGraphNavigationKind::kOutgoing);
  EXPECT_EQ(person_navigations[0].navigation_name, "owns");
  EXPECT_TRUE(person_navigations[0].is_multi);
  std::vector<const GraphElementTable*> person_only = {person_element_table};
  const std::vector<PropertyGraphNavigationBinding> person_navigations_via_set =
      googlesql::GetPropertyGraphNavigationBindingsForElementTables(
          *fin_graph, person_only);
  ASSERT_EQ(person_navigations_via_set.size(), 1);
  EXPECT_EQ(person_navigations_via_set[0].navigation_name, "owns");
  ElementTableSet person_element_set;
  person_element_set.insert(person_element_table);
  NavigationBindingSet analyzer_person_bindings;
  GOOGLESQL_ASSERT_OK(googlesql::GetNavigationBindingSet(
      *fin_graph, person_element_set, analyzer_person_bindings));
  ASSERT_EQ(analyzer_person_bindings.size(), 1);
  EXPECT_EQ(analyzer_person_bindings[0].navigation_kind,
            PropertyGraphNavigationKind::kOutgoing);
  EXPECT_EQ(analyzer_person_bindings[0].navigation_name, "owns");
  ElementTableSet owns_element_set;
  owns_element_set.insert(owns_element_table);
  NavigationBindingSet analyzer_owns_bindings;
  GOOGLESQL_ASSERT_OK(googlesql::GetNavigationBindingSet(
      *fin_graph, owns_element_set, analyzer_owns_bindings));
  ASSERT_EQ(analyzer_owns_bindings.size(), 2);
  EXPECT_EQ(analyzer_owns_bindings[0].navigation_kind,
            PropertyGraphNavigationKind::kSource);
  EXPECT_EQ(analyzer_owns_bindings[1].navigation_kind,
            PropertyGraphNavigationKind::kDestination);

  const PropertyGraphPlannerHooks planner_hooks =
      googlesql::GetPropertyGraphPlannerHooks(*fin_graph);
  EXPECT_EQ(planner_hooks.graph, fin_graph);
  ASSERT_EQ(planner_hooks.materialization_candidates.size(), 3);
  EXPECT_EQ(planner_hooks.materialization_candidates[0].element_table->Name(),
            "person");
  EXPECT_THAT(planner_hooks.materialization_candidates[0].key_columns,
              testing::ElementsAre(0));
  ASSERT_EQ(planner_hooks.materialization_candidates[0].measure_properties.size(),
            1);
  EXPECT_EQ(planner_hooks.materialization_candidates[0]
                .measure_properties[0]
                ->Name(),
            "total_id");
  EXPECT_FALSE(
      planner_hooks.materialization_candidates[0].has_dynamic_properties);
  ASSERT_EQ(planner_hooks.navigation_bindings.size(), 4);

  const WritablePropertyGraphViewDefinition writable_view =
      googlesql::GetWritablePropertyGraphViewDefinition(*fin_graph);
  EXPECT_EQ(writable_view.graph, fin_graph);
  ASSERT_EQ(writable_view.node_tables.size(), 2);
  EXPECT_EQ(writable_view.node_tables[0]->Name(), "person");
  EXPECT_EQ(writable_view.node_tables[1]->Name(), "account");
  ASSERT_EQ(writable_view.edge_tables.size(), 1);
  EXPECT_EQ(writable_view.edge_tables[0]->Name(), "owns");
  EXPECT_TRUE(writable_view.supports_node_updates);
  EXPECT_TRUE(writable_view.supports_edge_updates);
  EXPECT_TRUE(writable_view.supports_property_updates);
  EXPECT_TRUE(writable_view.supports_relation_updates);

  const PropertyGraphRoutineBindings routine_bindings =
      googlesql::GetPropertyGraphRoutineBindings(*fin_graph);
  EXPECT_EQ(routine_bindings.graph, fin_graph);
  ASSERT_EQ(routine_bindings.udf_binding_points.size(), 2);
  EXPECT_EQ(routine_bindings.udf_binding_points[0].node_table->Name(),
            "person");
  ASSERT_EQ(routine_bindings.udf_binding_points[0].property_declarations.size(),
            2);
  EXPECT_EQ(routine_bindings.udf_binding_points[0]
                .property_declarations[0]
                ->Name(),
            "upper_name");
  ASSERT_EQ(routine_bindings.procedure_binding_points.size(), 2);

  const PropertyGraphMatchSemanticModel match_model =
      googlesql::GetPropertyGraphMatchSemanticModel(*fin_graph);
  EXPECT_EQ(match_model.graph, fin_graph);
  ASSERT_EQ(match_model.node_tables.size(), 2);
  ASSERT_EQ(match_model.edge_tables.size(), 1);
  ASSERT_EQ(match_model.labels.size(), 1);
  EXPECT_EQ(match_model.labels[0]->Name(), "person");
  ASSERT_EQ(match_model.property_declarations.size(), 2);
  EXPECT_EQ(match_model.property_declarations[0]->Name(), "upper_name");
  EXPECT_EQ(match_model.property_declarations[1]->Name(), "total_id");
  ASSERT_EQ(match_model.navigation_bindings.size(), 4);

  const PropertyGraphRelationMetadata relation_metadata =
      googlesql::GetPropertyGraphRelationMetadata(
          *owns_element_table->AsEdgeTable());
  EXPECT_EQ(relation_metadata.source_exposure_name, "owner");
  EXPECT_EQ(relation_metadata.destination_exposure_name,
            "account_owner_target");
  EXPECT_EQ(relation_metadata.outgoing_exposure_name, "ownerships");
  EXPECT_EQ(relation_metadata.incoming_exposure_name, "owned_by");
  EXPECT_FALSE(relation_metadata.outgoing_is_multi);
  EXPECT_TRUE(relation_metadata.incoming_is_multi);

  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  GOOGLESQL_EXPECT_OK(AnalyzeStatement(
      "SELECT o.fin_graph_destination_account.id FROM owns AS o",
      analyzer_options, &catalog, catalog.type_factory(), &analyzer_output));
  GOOGLESQL_EXPECT_OK(AnalyzeStatement(
      "SELECT p.fin_graph_property_upper_name FROM person AS p",
      analyzer_options, &catalog, catalog.type_factory(), &analyzer_output));
  GOOGLESQL_EXPECT_OK(AnalyzeStatement(
      "SELECT AGGREGATE(p.fin_graph_property_total_id) FROM person AS p",
      analyzer_options, &catalog, catalog.type_factory(), &analyzer_output));

  ASSERT_FALSE(artifacts.empty());
  SQLBuilder sql_builder;
  GOOGLESQL_ASSERT_OK(
      sql_builder.Process(*artifacts.back()->resolved_statement()));
  EXPECT_THAT(sql_builder.sql(), HasSubstr("display_name = \"Upper Name\""));
  EXPECT_THAT(sql_builder.sql(), HasSubstr("semantic_role = \"dimension\""));
  EXPECT_THAT(sql_builder.sql(), HasSubstr("hidden = TRUE"));
  EXPECT_THAT(sql_builder.sql(), HasSubstr("source_name = \"owner\""));
  EXPECT_THAT(sql_builder.sql(), HasSubstr("destination_name = \"account_owner_target\""));
  EXPECT_THAT(sql_builder.sql(), HasSubstr("outgoing_name = \"ownerships\""));
  EXPECT_THAT(sql_builder.sql(), HasSubstr("incoming_name = \"owned_by\""));
  std::unique_ptr<const AnalyzerOutput> rebuilt_graph_output;
  GOOGLESQL_EXPECT_OK(AnalyzeStatement(sql_builder.sql(), analyzer_options,
                                       &catalog, catalog.type_factory(),
                                       &rebuilt_graph_output));

  FileDescriptorSetMap file_descriptor_set_map;
  SimpleCatalogProto proto;
  GOOGLESQL_ASSERT_OK(catalog.Serialize(&file_descriptor_set_map, &proto));

  std::vector<const google::protobuf::DescriptorPool*> pools(
      file_descriptor_set_map.size());
  for (const auto& [pool, descriptor_set] : file_descriptor_set_map) {
    pools[descriptor_set->descriptor_set_index] = pool;
  }

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SimpleCatalog> restored_catalog,
      SimpleCatalog::Deserialize(proto, pools));
  const Table* restored_person;
  GOOGLESQL_ASSERT_OK(restored_catalog->FindTable({"person"}, &restored_person));

  const Column* restored_upper_name = nullptr;
  GOOGLESQL_ASSERT_OK(restored_catalog->FindPropertyGraphSqlColumn(
      *restored_person, "fin_graph",
      SimpleCatalog::PropertyGraphSqlColumnKind::kProperty,
      &restored_upper_name, "upper_name"));
  ASSERT_NE(restored_upper_name, nullptr);
  ASSERT_TRUE(restored_upper_name->HasGeneratedExpression());
  EXPECT_TRUE(restored_upper_name->GetExpression()->HasResolvedExpression());
  const PropertyGraph* restored_fin_graph = nullptr;
  GOOGLESQL_ASSERT_OK(
      restored_catalog->FindPropertyGraph({"fin_graph"}, &restored_fin_graph));
  const GraphElementTable* restored_person_element_table = nullptr;
  GOOGLESQL_ASSERT_OK(restored_fin_graph->FindElementTableByName(
      "person", restored_person_element_table));
  const GraphPropertyDefinition* restored_upper_name_definition = nullptr;
  GOOGLESQL_ASSERT_OK(
      restored_person_element_table->FindPropertyDefinitionByName(
          "upper_name", restored_upper_name_definition));
  ASSERT_NE(restored_upper_name_definition, nullptr);
  EXPECT_EQ(restored_upper_name_definition->SemanticMetadata().display_name,
            "Upper Name");
  EXPECT_THAT(
      restored_upper_name_definition->SemanticMetadata().semantic_aliases,
      testing::ElementsAre("upper", "display_name"));

  const Column* restored_total_id = nullptr;
  GOOGLESQL_ASSERT_OK(restored_catalog->FindPropertyGraphSqlColumn(
      *restored_person, "fin_graph",
      SimpleCatalog::PropertyGraphSqlColumnKind::kProperty,
      &restored_total_id, "total_id"));
  ASSERT_NE(restored_total_id, nullptr);
  ASSERT_TRUE(restored_total_id->HasMeasureExpression());
  EXPECT_TRUE(restored_total_id->GetExpression()->HasResolvedExpression());
  const GraphPropertyDefinition* restored_total_id_definition = nullptr;
  GOOGLESQL_ASSERT_OK(
      restored_person_element_table->FindPropertyDefinitionByName(
          "total_id", restored_total_id_definition));
  ASSERT_NE(restored_total_id_definition, nullptr);
  EXPECT_TRUE(restored_total_id_definition->SemanticMetadata().hidden);
  const GraphElementTable* restored_owns_element_table = nullptr;
  GOOGLESQL_ASSERT_OK(
      restored_fin_graph->FindElementTableByName("owns",
                                                 restored_owns_element_table));
  const PropertyGraphRelationMetadata restored_relation_metadata =
      googlesql::GetPropertyGraphRelationMetadata(
          *restored_owns_element_table->AsEdgeTable());
  EXPECT_EQ(restored_relation_metadata.outgoing_exposure_name, "ownerships");
  EXPECT_EQ(restored_relation_metadata.incoming_exposure_name, "owned_by");
  EXPECT_FALSE(restored_relation_metadata.outgoing_is_multi);
  EXPECT_TRUE(restored_relation_metadata.incoming_is_multi);

  GOOGLESQL_EXPECT_OK(AnalyzeStatement(
      "SELECT AGGREGATE(p.fin_graph_property_total_id) FROM person AS p",
      analyzer_options, restored_catalog.get(), restored_catalog->type_factory(),
      &analyzer_output));
}

TEST(SimpleCatalogUtilTest, AddPropertyGraphAllowsDerivedPropertyChaining) {
  SimpleCatalog catalog("simple");
  catalog.AddOwnedTable(std::make_unique<SimpleTable>(
      "person", std::vector<SimpleTable::NameAndType>{{"id", types::Int64Type()},
                                                      {"name", types::StringType()}}));

  AnalyzerOptions analyzer_options;
  analyzer_options.mutable_language()->EnableLanguageFeature(FEATURE_SQL_GRAPH);
  analyzer_options.mutable_language()->AddSupportedStatementKind(
      RESOLVED_CREATE_PROPERTY_GRAPH_STMT);
  std::vector<std::unique_ptr<const AnalyzerOutput>> artifacts;

  GOOGLESQL_ASSERT_OK(AddPropertyGraphFromCreatePropertyGraphStmt(R"sql(
      CREATE PROPERTY GRAPH chain_graph
      NODE TABLES (
        person KEY(id)
          LABEL person PROPERTIES (
            UPPER(name) AS upper_name,
            CONCAT(upper_name, "!") AS excited_name
          )
      )
      )sql",
      analyzer_options, artifacts, catalog));

  const Table* person;
  GOOGLESQL_ASSERT_OK(catalog.FindTable({"person"}, &person));

  const Column* excited_name = nullptr;
  GOOGLESQL_ASSERT_OK(catalog.FindPropertyGraphSqlColumn(
      *person, "chain_graph",
      SimpleCatalog::PropertyGraphSqlColumnKind::kProperty, &excited_name,
      "excited_name"));
  ASSERT_NE(excited_name, nullptr);
  ASSERT_TRUE(excited_name->HasGeneratedExpression());
  EXPECT_EQ(excited_name->GetExpression()->GetExpressionString(),
            "CONCAT(upper_name, \"!\")");
  ASSERT_TRUE(excited_name->GetExpression()->HasResolvedExpression());
  const ResolvedFunctionCallBase* excited_name_expr =
      excited_name->GetExpression()
          ->GetResolvedExpression()
          ->GetAs<ResolvedFunctionCallBase>();
  ASSERT_NE(excited_name_expr, nullptr);
  ASSERT_EQ(excited_name_expr->argument_list_size(), 2);
  const ResolvedCatalogColumnRef* upper_name_ref =
      excited_name_expr->argument_list(0)->GetAs<ResolvedCatalogColumnRef>();
  ASSERT_NE(upper_name_ref, nullptr);
  EXPECT_EQ(upper_name_ref->name(), "upper_name");
  ASSERT_EQ(person->NumColumns(), 4);
  EXPECT_EQ(person->GetColumn(2)->Name(), "chain_graph_property_upper_name");
  EXPECT_EQ(person->GetColumn(3)->Name(), "chain_graph_property_excited_name");

  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  GOOGLESQL_EXPECT_OK(AnalyzeStatement(
      "SELECT p.chain_graph_property_excited_name FROM person AS p",
      analyzer_options, &catalog, catalog.type_factory(), &analyzer_output));

  const PropertyGraph* chain_graph = nullptr;
  GOOGLESQL_ASSERT_OK(catalog.FindPropertyGraph({"chain_graph"}, &chain_graph));
  ASSERT_NE(chain_graph, nullptr);
  auto ordered_property_declarations =
      googlesql::GetPropertyDeclarationsInDeclarationOrder(*chain_graph);
  ASSERT_EQ(ordered_property_declarations.size(), 2);
  EXPECT_EQ(ordered_property_declarations[0]->Name(), "upper_name");
  EXPECT_EQ(ordered_property_declarations[1]->Name(), "excited_name");
  const GraphElementTable* person_element_table = nullptr;
  GOOGLESQL_ASSERT_OK(chain_graph->FindElementTableByName("person",
                                                          person_element_table));
  ASSERT_NE(person_element_table, nullptr);
  auto ordered_property_definitions =
      googlesql::GetPropertyDefinitionsInDeclarationOrder(*person_element_table);
  ASSERT_EQ(ordered_property_definitions.size(), 2);
  EXPECT_EQ(ordered_property_definitions[0]->GetDeclaration().Name(),
            "upper_name");
  EXPECT_EQ(ordered_property_definitions[1]->GetDeclaration().Name(),
            "excited_name");
  auto ordered_property_declarations_for_element =
      googlesql::GetPropertyDeclarationsInDeclarationOrder(*person_element_table);
  ASSERT_EQ(ordered_property_declarations_for_element.size(), 2);
  EXPECT_EQ(ordered_property_declarations_for_element[0]->Name(), "upper_name");
  EXPECT_EQ(ordered_property_declarations_for_element[1]->Name(),
            "excited_name");
}

TEST(SimpleCatalogUtilTest, AddPropertyGraphAllowsMeasureThroughWithExpr) {
  SimpleCatalog catalog("simple");
  catalog.AddOwnedTable(std::make_unique<SimpleTable>(
      "person", std::vector<SimpleTable::NameAndType>{{"id", types::Int64Type()},
                                                      {"name", types::StringType()}}));

  AnalyzerOptions analyzer_options;
  analyzer_options.mutable_language()->EnableLanguageFeature(FEATURE_SQL_GRAPH);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_ENABLE_MEASURES);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_WITH_EXPRESSION);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_MULTILEVEL_AGGREGATION);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_GROUP_BY_STRUCT);
  analyzer_options.mutable_language()->AddSupportedStatementKind(
      RESOLVED_CREATE_PROPERTY_GRAPH_STMT);
  std::vector<std::unique_ptr<const AnalyzerOutput>> artifacts;

  GOOGLESQL_ASSERT_OK(AddPropertyGraphFromCreatePropertyGraphStmt(R"sql(
      CREATE PROPERTY GRAPH measure_graph
      NODE TABLES (
        person KEY(id)
          LABEL person PROPERTIES (
            MEASURE(SUM(id)) AS total_id
          )
      )
      )sql",
      analyzer_options, artifacts, catalog));

  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  GOOGLESQL_EXPECT_OK(AnalyzeStatement(R"sql(
      SELECT AGGREGATE(renamed_measure)
      FROM (
        SELECT WITH(tmp AS 1 + 2, p.measure_graph_property_total_id)
          AS renamed_measure
        FROM person AS p
      )
      )sql",
      analyzer_options, &catalog, catalog.type_factory(), &analyzer_output));
}

TEST(SimpleCatalogUtilTest, AddPropertyGraphAllowsMeasureThroughScalarSubquery) {
  SimpleCatalog catalog("simple");
  catalog.AddOwnedTable(std::make_unique<SimpleTable>(
      "person", std::vector<SimpleTable::NameAndType>{{"id", types::Int64Type()},
                                                      {"name", types::StringType()}}));

  AnalyzerOptions analyzer_options;
  analyzer_options.mutable_language()->EnableLanguageFeature(FEATURE_SQL_GRAPH);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_ENABLE_MEASURES);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_WITH_EXPRESSION);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_MULTILEVEL_AGGREGATION);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_GROUP_BY_STRUCT);
  analyzer_options.mutable_language()->AddSupportedStatementKind(
      RESOLVED_CREATE_PROPERTY_GRAPH_STMT);
  analyzer_options.enable_rewrite(REWRITE_MEASURE_TYPE);
  std::vector<std::unique_ptr<const AnalyzerOutput>> artifacts;

  GOOGLESQL_ASSERT_OK(AddPropertyGraphFromCreatePropertyGraphStmt(R"sql(
      CREATE PROPERTY GRAPH measure_graph
      NODE TABLES (
        person KEY(id)
          LABEL person PROPERTIES (
            MEASURE(SUM(id)) AS total_id
          )
      )
      )sql",
      analyzer_options, artifacts, catalog));

  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  GOOGLESQL_EXPECT_OK(AnalyzeStatement(R"sql(
      SELECT AGGREGATE(measure_from_subquery)
      FROM (
        SELECT (
          SELECT inner_p.measure_graph_property_total_id
          FROM person AS inner_p
          WHERE inner_p.id = outer_p.id
        ) AS measure_from_subquery
        FROM person AS outer_p
      )
      )sql",
      analyzer_options, &catalog, catalog.type_factory(), &analyzer_output));
}

TEST(SimpleCatalogUtilTest,
     AddPropertyGraphAllowsMeasureThroughWithScanDuplicateColumns) {
  SimpleCatalog catalog("simple");
  catalog.AddOwnedTable(std::make_unique<SimpleTable>(
      "person", std::vector<SimpleTable::NameAndType>{{"id", types::Int64Type()},
                                                      {"name", types::StringType()}}));

  AnalyzerOptions analyzer_options;
  analyzer_options.mutable_language()->EnableLanguageFeature(FEATURE_SQL_GRAPH);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_ENABLE_MEASURES);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_MULTILEVEL_AGGREGATION);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_GROUP_BY_STRUCT);
  analyzer_options.mutable_language()->AddSupportedStatementKind(
      RESOLVED_CREATE_PROPERTY_GRAPH_STMT);
  analyzer_options.enable_rewrite(REWRITE_MEASURE_TYPE);
  std::vector<std::unique_ptr<const AnalyzerOutput>> artifacts;

  GOOGLESQL_ASSERT_OK(AddPropertyGraphFromCreatePropertyGraphStmt(R"sql(
      CREATE PROPERTY GRAPH measure_graph
      NODE TABLES (
        person KEY(id)
          LABEL person PROPERTIES (
            MEASURE(SUM(id)) AS total_id
          )
      )
      )sql",
      analyzer_options, artifacts, catalog));

  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  GOOGLESQL_EXPECT_OK(AnalyzeStatement(R"sql(
      WITH wq0 AS (
        SELECT
          id,
          measure_graph_property_total_id,
          measure_graph_property_total_id AS repeated_measure
        FROM person
      )
      SELECT
        id,
        AGGREGATE(measure_graph_property_total_id)
      FROM wq0
      GROUP BY 1
      )sql",
      analyzer_options, &catalog, catalog.type_factory(), &analyzer_output));
}

TEST(SimpleCatalogUtilTest, AddPropertyGraphAllowsMeasureThroughLateralJoin) {
  SimpleCatalog catalog("simple");
  catalog.AddOwnedTable(std::make_unique<SimpleTable>(
      "person", std::vector<SimpleTable::NameAndType>{{"id", types::Int64Type()},
                                                      {"name", types::StringType()}}));

  AnalyzerOptions analyzer_options;
  analyzer_options.mutable_language()->EnableLanguageFeature(FEATURE_SQL_GRAPH);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_ENABLE_MEASURES);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_LATERAL_JOIN);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_MULTILEVEL_AGGREGATION);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_GROUP_BY_STRUCT);
  analyzer_options.mutable_language()->AddSupportedStatementKind(
      RESOLVED_CREATE_PROPERTY_GRAPH_STMT);
  analyzer_options.enable_rewrite(REWRITE_MEASURE_TYPE);
  std::vector<std::unique_ptr<const AnalyzerOutput>> artifacts;

  GOOGLESQL_ASSERT_OK(AddPropertyGraphFromCreatePropertyGraphStmt(R"sql(
      CREATE PROPERTY GRAPH measure_graph
      NODE TABLES (
        person KEY(id)
          LABEL person PROPERTIES (
            MEASURE(SUM(id)) AS total_id
          )
      )
      )sql",
      analyzer_options, artifacts, catalog));

  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  GOOGLESQL_EXPECT_OK(AnalyzeStatement(R"sql(
      SELECT AGGREGATE(t1.measure_graph_property_total_id)
      FROM
        (SELECT id, measure_graph_property_total_id FROM person) AS t1
        INNER JOIN LATERAL (
          SELECT id, measure_graph_property_total_id FROM person
        ) AS t2 USING (id)
      )sql",
      analyzer_options, &catalog, catalog.type_factory(), &analyzer_output));
}

TEST(SimpleCatalogUtilTest, AddPropertyGraphAllowsMeasureInMatchRecognize) {
  SimpleCatalog catalog("simple");
  catalog.AddOwnedTable(std::make_unique<SimpleTable>(
      "person", std::vector<SimpleTable::NameAndType>{{"id", types::Int64Type()},
                                                      {"name", types::StringType()}}));

  AnalyzerOptions analyzer_options;
  analyzer_options.mutable_language()->EnableLanguageFeature(FEATURE_SQL_GRAPH);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_ENABLE_MEASURES);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_MATCH_RECOGNIZE);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_MULTILEVEL_AGGREGATION);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_GROUP_BY_STRUCT);
  analyzer_options.mutable_language()->AddSupportedStatementKind(
      RESOLVED_CREATE_PROPERTY_GRAPH_STMT);
  analyzer_options.enable_rewrite(REWRITE_MEASURE_TYPE);
  std::vector<std::unique_ptr<const AnalyzerOutput>> artifacts;

  GOOGLESQL_ASSERT_OK(AddPropertyGraphFromCreatePropertyGraphStmt(R"sql(
      CREATE PROPERTY GRAPH measure_graph
      NODE TABLES (
        person KEY(id)
          LABEL person PROPERTIES (
            MEASURE(SUM(id)) AS total_id
          )
      )
      )sql",
      analyzer_options, artifacts, catalog));

  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  GOOGLESQL_EXPECT_OK(AnalyzeStatement(R"sql(
      SELECT mr
      FROM person MATCH_RECOGNIZE(
        ORDER BY id
        MEASURES
          AGGREGATE(measure_graph_property_total_id) AS aggregated
        PATTERN (is_true)
        DEFINE
          is_true AS TRUE
      ) AS mr
      )sql",
      analyzer_options, &catalog, catalog.type_factory(), &analyzer_output));
}

TEST(SimpleCatalogUtilTest,
     AddPropertyGraphPreservesSemanticPropertyNameInChainedResolution) {
  SimpleCatalog catalog("simple");
  catalog.AddOwnedTable(std::make_unique<SimpleTable>(
      "person", std::vector<SimpleTable::NameAndType>{{"id", types::Int64Type()},
                                                      {"name", types::StringType()}}));

  AnalyzerOptions analyzer_options;
  analyzer_options.mutable_language()->EnableLanguageFeature(FEATURE_SQL_GRAPH);
  analyzer_options.mutable_language()->AddSupportedStatementKind(
      RESOLVED_CREATE_PROPERTY_GRAPH_STMT);
  std::vector<std::unique_ptr<const AnalyzerOutput>> artifacts;

  GOOGLESQL_ASSERT_OK(AddPropertyGraphFromCreatePropertyGraphStmt(R"sql(
      CREATE PROPERTY GRAPH case_graph
      NODE TABLES (
        person KEY(id)
          LABEL person PROPERTIES (
            UPPER(name) AS UpperName,
            CONCAT(UpperName, "!") AS excited_name
          )
      )
      )sql",
      analyzer_options, artifacts, catalog));

  const Table* person;
  GOOGLESQL_ASSERT_OK(catalog.FindTable({"person"}, &person));

  const Column* excited_name = nullptr;
  GOOGLESQL_ASSERT_OK(catalog.FindPropertyGraphSqlColumn(
      *person, "case_graph",
      SimpleCatalog::PropertyGraphSqlColumnKind::kProperty, &excited_name,
      "excited_name"));
  ASSERT_NE(excited_name, nullptr);
  ASSERT_TRUE(excited_name->HasGeneratedExpression());
  ASSERT_TRUE(excited_name->GetExpression()->HasResolvedExpression());
  const ResolvedFunctionCallBase* excited_name_expr =
      excited_name->GetExpression()
          ->GetResolvedExpression()
          ->GetAs<ResolvedFunctionCallBase>();
  ASSERT_NE(excited_name_expr, nullptr);
  ASSERT_EQ(excited_name_expr->argument_list_size(), 2);
  const ResolvedCatalogColumnRef* upper_name_ref =
      excited_name_expr->argument_list(0)->GetAs<ResolvedCatalogColumnRef>();
  ASSERT_NE(upper_name_ref, nullptr);
  EXPECT_EQ(upper_name_ref->name(), "UpperName");
}

TEST(SimpleCatalogUtilTest,
     AddPropertyGraphRejectsMeasurePropertyReferenceInMeasureDefinition) {
  SimpleCatalog catalog("simple");
  catalog.AddOwnedTable(std::make_unique<SimpleTable>(
      "person", std::vector<SimpleTable::NameAndType>{{"id", types::Int64Type()}}));

  AnalyzerOptions analyzer_options;
  analyzer_options.mutable_language()->EnableLanguageFeature(FEATURE_SQL_GRAPH);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_SQL_GRAPH_MEASURE_DDL);
  analyzer_options.mutable_language()->AddSupportedStatementKind(
      RESOLVED_CREATE_PROPERTY_GRAPH_STMT);
  std::vector<std::unique_ptr<const AnalyzerOutput>> artifacts;

  EXPECT_THAT(AddPropertyGraphFromCreatePropertyGraphStmt(R"sql(
      CREATE PROPERTY GRAPH measure_graph
      NODE TABLES (
        person KEY(id)
          LABEL person PROPERTIES (
            MEASURE(SUM(id)) AS total_id,
            MEASURE(ANY_VALUE(total_id)) AS invalid_total
          )
      )
      )sql",
      analyzer_options, artifacts, catalog),
              StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  HasSubstr("Graph measure property `total_id` cannot be "
                            "referenced from another graph measure "
                            "property")));
}

TEST(SimpleCatalogUtilTest, AddPropertyGraphPreservesLabelDeclarationOrder) {
  SimpleCatalog catalog("simple");
  catalog.AddOwnedTable(std::make_unique<SimpleTable>(
      "person",
      std::vector<SimpleTable::NameAndType>{{"id", types::Int64Type()},
                                            {"name", types::StringType()},
                                            {"tier", types::StringType()}}));

  AnalyzerOptions analyzer_options;
  analyzer_options.mutable_language()->EnableLanguageFeature(FEATURE_SQL_GRAPH);
  analyzer_options.mutable_language()->AddSupportedStatementKind(
      RESOLVED_CREATE_PROPERTY_GRAPH_STMT);
  std::vector<std::unique_ptr<const AnalyzerOutput>> artifacts;

  GOOGLESQL_ASSERT_OK(AddPropertyGraphFromCreatePropertyGraphStmt(R"sql(
      CREATE PROPERTY GRAPH labels_graph
      NODE TABLES (
        person KEY(id)
          LABEL base PROPERTIES (
            name AS base_name
          )
          LABEL vip PROPERTIES (
            tier AS vip_tier
          )
      )
      )sql",
      analyzer_options, artifacts, catalog));

  const PropertyGraph* labels_graph = nullptr;
  GOOGLESQL_ASSERT_OK(catalog.FindPropertyGraph({"labels_graph"}, &labels_graph));
  ASSERT_NE(labels_graph, nullptr);
  auto ordered_labels =
      googlesql::GetLabelsInDeclarationOrder(*labels_graph);
  ASSERT_EQ(ordered_labels.size(), 2);
  EXPECT_EQ(ordered_labels[0]->Name(), "base");
  EXPECT_EQ(ordered_labels[1]->Name(), "vip");

  const GraphElementTable* person_element_table = nullptr;
  GOOGLESQL_ASSERT_OK(labels_graph->FindElementTableByName("person",
                                                          person_element_table));
  ASSERT_NE(person_element_table, nullptr);
  auto ordered_element_labels =
      googlesql::GetLabelsInDeclarationOrder(*person_element_table);
  ASSERT_EQ(ordered_element_labels.size(), 2);
  EXPECT_EQ(ordered_element_labels[0]->Name(), "base");
  EXPECT_EQ(ordered_element_labels[1]->Name(), "vip");

  const GraphElementLabel* base_label = nullptr;
  GOOGLESQL_ASSERT_OK(labels_graph->FindLabelByName("base", base_label));
  ASSERT_NE(base_label, nullptr);
  auto base_properties =
      googlesql::GetPropertyDeclarationsInDeclarationOrder(*base_label);
  ASSERT_EQ(base_properties.size(), 1);
  EXPECT_EQ(base_properties[0]->Name(), "base_name");

  const GraphElementLabel* vip_label = nullptr;
  GOOGLESQL_ASSERT_OK(labels_graph->FindLabelByName("vip", vip_label));
  ASSERT_NE(vip_label, nullptr);
  auto vip_properties =
      googlesql::GetPropertyDeclarationsInDeclarationOrder(*vip_label);
  ASSERT_EQ(vip_properties.size(), 1);
  EXPECT_EQ(vip_properties[0]->Name(), "vip_tier");
}

TEST(SimpleCatalogUtilTest, AddPropertyGraphPreservesElementTableOrder) {
  SimpleCatalog catalog("simple");
  catalog.AddOwnedTable(std::make_unique<SimpleTable>(
      "person", std::vector<SimpleTable::NameAndType>{{"id", types::Int64Type()}}));
  catalog.AddOwnedTable(std::make_unique<SimpleTable>(
      "account",
      std::vector<SimpleTable::NameAndType>{{"id", types::Int64Type()}}));
  catalog.AddOwnedTable(std::make_unique<SimpleTable>(
      "owns",
      std::vector<SimpleTable::NameAndType>{{"person_id", types::Int64Type()},
                                            {"account_id", types::Int64Type()}}));

  AnalyzerOptions analyzer_options;
  analyzer_options.mutable_language()->EnableLanguageFeature(FEATURE_SQL_GRAPH);
  analyzer_options.mutable_language()->AddSupportedStatementKind(
      RESOLVED_CREATE_PROPERTY_GRAPH_STMT);
  std::vector<std::unique_ptr<const AnalyzerOutput>> artifacts;

  GOOGLESQL_ASSERT_OK(AddPropertyGraphFromCreatePropertyGraphStmt(R"sql(
      CREATE PROPERTY GRAPH order_graph
      NODE TABLES (
        person KEY(id) LABEL person PROPERTIES NO PROPERTIES,
        account KEY(id) LABEL account PROPERTIES NO PROPERTIES
      )
      EDGE TABLES (
        owns
          SOURCE KEY(person_id) REFERENCES person(id)
          DESTINATION KEY(account_id) REFERENCES account(id)
          LABEL owns PROPERTIES NO PROPERTIES
      )
      )sql",
      analyzer_options, artifacts, catalog));

  const PropertyGraph* order_graph = nullptr;
  GOOGLESQL_ASSERT_OK(catalog.FindPropertyGraph({"order_graph"}, &order_graph));
  ASSERT_NE(order_graph, nullptr);
  auto ordered_nodes =
      googlesql::GetNodeTablesInDeclarationOrder(*order_graph);
  ASSERT_EQ(ordered_nodes.size(), 2);
  EXPECT_EQ(ordered_nodes[0]->Name(), "person");
  EXPECT_EQ(ordered_nodes[1]->Name(), "account");
  auto ordered_edges =
      googlesql::GetEdgeTablesInDeclarationOrder(*order_graph);
  ASSERT_EQ(ordered_edges.size(), 1);
  EXPECT_EQ(ordered_edges[0]->Name(), "owns");
  auto ordered_element_tables =
      googlesql::GetElementTablesInDeclarationOrder(*order_graph);
  ASSERT_EQ(ordered_element_tables.size(), 3);
  EXPECT_EQ(ordered_element_tables[0]->Name(), "person");
  EXPECT_EQ(ordered_element_tables[1]->Name(), "account");
  EXPECT_EQ(ordered_element_tables[2]->Name(), "owns");
  const GraphNodeTableReference& source_ref =
      googlesql::GetSourceNodeTableReference(*ordered_edges[0]);
  const GraphNodeTableReference& destination_ref =
      googlesql::GetDestinationNodeTableReference(*ordered_edges[0]);
  EXPECT_EQ(source_ref.GetReferencedNodeTable()->Name(), "person");
  EXPECT_EQ(destination_ref.GetReferencedNodeTable()->Name(), "account");
  const PropertyGraphElementMetadata edge_metadata =
      googlesql::GetPropertyGraphElementMetadata(*ordered_edges[0]);
  EXPECT_EQ(edge_metadata.kind, GraphElementTable::Kind::kEdge);
  ASSERT_TRUE(edge_metadata.relation.has_value());
  EXPECT_EQ(edge_metadata.relation->source_exposure_name, "person");
  EXPECT_EQ(edge_metadata.relation->destination_exposure_name, "account");
  EXPECT_EQ(edge_metadata.relation->outgoing_exposure_name, "owns");
  EXPECT_EQ(edge_metadata.relation->incoming_exposure_name, "owns");

  const Column* source_column_via_edge = nullptr;
  GOOGLESQL_ASSERT_OK(googlesql::FindPropertyGraphSourceColumn(
      *order_graph, *ordered_edges[0], &source_column_via_edge));
  ASSERT_NE(source_column_via_edge, nullptr);
  EXPECT_EQ(source_column_via_edge->Name(), "order_graph_source_person");
  const Column* source_column_via_catalog = nullptr;
  GOOGLESQL_ASSERT_OK(catalog.FindPropertyGraphSourceColumn(
      *order_graph, *ordered_edges[0], &source_column_via_catalog));
  EXPECT_EQ(source_column_via_catalog, source_column_via_edge);

  const Column* destination_column_via_edge = nullptr;
  GOOGLESQL_ASSERT_OK(googlesql::FindPropertyGraphDestinationColumn(
      *order_graph, *ordered_edges[0], &destination_column_via_edge));
  ASSERT_NE(destination_column_via_edge, nullptr);
  EXPECT_EQ(destination_column_via_edge->Name(),
            "order_graph_destination_account");
  const Column* destination_column_via_catalog = nullptr;
  GOOGLESQL_ASSERT_OK(catalog.FindPropertyGraphDestinationColumn(
      *order_graph, *ordered_edges[0], &destination_column_via_catalog));
  EXPECT_EQ(destination_column_via_catalog, destination_column_via_edge);

  const Column* outgoing_column_via_edge = nullptr;
  GOOGLESQL_ASSERT_OK(googlesql::FindPropertyGraphOutgoingColumn(
      *order_graph, *ordered_edges[0], &outgoing_column_via_edge));
  ASSERT_NE(outgoing_column_via_edge, nullptr);
  EXPECT_EQ(outgoing_column_via_edge->Name(), "order_graph_outgoing_owns");
  const Column* outgoing_column_via_catalog = nullptr;
  GOOGLESQL_ASSERT_OK(catalog.FindPropertyGraphOutgoingColumn(
      *order_graph, *ordered_edges[0], &outgoing_column_via_catalog));
  EXPECT_EQ(outgoing_column_via_catalog, outgoing_column_via_edge);

  const Column* incoming_column_via_edge = nullptr;
  GOOGLESQL_ASSERT_OK(googlesql::FindPropertyGraphIncomingColumn(
      *order_graph, *ordered_edges[0], &incoming_column_via_edge));
  ASSERT_NE(incoming_column_via_edge, nullptr);
  EXPECT_EQ(incoming_column_via_edge->Name(), "order_graph_incoming_owns");
  const Column* incoming_column_via_catalog = nullptr;
  GOOGLESQL_ASSERT_OK(catalog.FindPropertyGraphIncomingColumn(
      *order_graph, *ordered_edges[0], &incoming_column_via_catalog));
  EXPECT_EQ(incoming_column_via_catalog, incoming_column_via_edge);

  PropertyGraphRelationSqlColumns relation_columns;
  GOOGLESQL_ASSERT_OK(googlesql::FindPropertyGraphRelationSqlColumns(
      *order_graph, *ordered_edges[0], relation_columns));
  EXPECT_EQ(relation_columns.source, source_column_via_edge);
  EXPECT_EQ(relation_columns.destination, destination_column_via_edge);
  EXPECT_EQ(relation_columns.outgoing, outgoing_column_via_edge);
  EXPECT_EQ(relation_columns.incoming, incoming_column_via_edge);

  PropertyGraphRelationSqlColumns relation_columns_via_catalog;
  GOOGLESQL_ASSERT_OK(catalog.FindPropertyGraphRelationSqlColumns(
      *order_graph, *ordered_edges[0], relation_columns_via_catalog));
  EXPECT_EQ(relation_columns_via_catalog.source, source_column_via_edge);
  EXPECT_EQ(relation_columns_via_catalog.destination,
            destination_column_via_edge);
  EXPECT_EQ(relation_columns_via_catalog.outgoing, outgoing_column_via_edge);
  EXPECT_EQ(relation_columns_via_catalog.incoming, incoming_column_via_edge);

  const PropertyGraphRelationMetadata relation_metadata =
      googlesql::GetPropertyGraphRelationMetadata(*ordered_edges[0]);
  EXPECT_EQ(relation_metadata.edge_table, ordered_edges[0]);
  ASSERT_NE(relation_metadata.source_node_table, nullptr);
  ASSERT_NE(relation_metadata.destination_node_table, nullptr);
  EXPECT_EQ(relation_metadata.source_node_table->Name(), "person");
  EXPECT_EQ(relation_metadata.destination_node_table->Name(), "account");
  EXPECT_EQ(relation_metadata.source_exposure_name, "person");
  EXPECT_EQ(relation_metadata.destination_exposure_name, "account");
  EXPECT_EQ(relation_metadata.outgoing_exposure_name, "owns");
  EXPECT_EQ(relation_metadata.incoming_exposure_name, "owns");
  EXPECT_FALSE(relation_metadata.source_is_multi);
  EXPECT_FALSE(relation_metadata.destination_is_multi);
  EXPECT_TRUE(relation_metadata.outgoing_is_multi);
  EXPECT_TRUE(relation_metadata.incoming_is_multi);

  const Table* person = nullptr;
  GOOGLESQL_ASSERT_OK(catalog.FindTable({"person"}, &person));
  ASSERT_NE(person, nullptr);
  EXPECT_EQ(person->GetColumn(1)->Name(), "order_graph_outgoing_owns");

  const Table* account = nullptr;
  GOOGLESQL_ASSERT_OK(catalog.FindTable({"account"}, &account));
  ASSERT_NE(account, nullptr);
  EXPECT_EQ(account->GetColumn(1)->Name(), "order_graph_incoming_owns");

  const Table* owns = nullptr;
  GOOGLESQL_ASSERT_OK(catalog.FindTable({"owns"}, &owns));
  ASSERT_NE(owns, nullptr);
  EXPECT_EQ(owns->GetColumn(2)->Name(), "order_graph_source_person");
  EXPECT_EQ(owns->GetColumn(3)->Name(), "order_graph_destination_account");
}

TEST(SimpleCatalogUtilTest,
     AddPropertyGraphPreservesDynamicLabelAndPropertiesOnDeserialize) {
  SimpleCatalog catalog("simple");
  catalog.AddOwnedTable(std::make_unique<SimpleTable>(
      "DynamicGraphNode",
      std::vector<SimpleTable::NameAndType>{{"id", types::Int64Type()},
                                            {"nodeLabelCol", types::StringType()},
                                            {"nodeJsonProp", types::JsonType()}}));

  AnalyzerOptions analyzer_options;
  analyzer_options.mutable_language()->EnableLanguageFeature(FEATURE_SQL_GRAPH);
  analyzer_options.mutable_language()->EnableLanguageFeature(FEATURE_JSON_TYPE);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_SQL_GRAPH_DYNAMIC_LABEL_PROPERTIES_IN_DDL);
  analyzer_options.mutable_language()->AddSupportedStatementKind(
      RESOLVED_CREATE_PROPERTY_GRAPH_STMT);
  std::vector<std::unique_ptr<const AnalyzerOutput>> artifacts;

  GOOGLESQL_ASSERT_OK(AddPropertyGraphFromCreatePropertyGraphStmt(R"sql(
      CREATE PROPERTY GRAPH dyn_graph
      NODE TABLES (
        DynamicGraphNode
          KEY(id)
          DEFAULT LABEL PROPERTIES ALL COLUMNS
          DYNAMIC LABEL (nodeLabelCol)
          DYNAMIC PROPERTIES (nodeJsonProp)
      )
      )sql",
      analyzer_options, artifacts, catalog));

  const PropertyGraph* graph;
  GOOGLESQL_ASSERT_OK(catalog.FindPropertyGraph({"dyn_graph"}, &graph));
  const GraphElementTable* element_table;
  GOOGLESQL_ASSERT_OK(graph->FindElementTableByName("DynamicGraphNode", element_table));

  const GraphDynamicLabel* dynamic_label = nullptr;
  GOOGLESQL_ASSERT_OK(element_table->GetDynamicLabel(dynamic_label));
  ASSERT_NE(dynamic_label, nullptr);
  ASSERT_TRUE(dynamic_label->GetValueExpression().ok());

  const GraphDynamicProperties* dynamic_properties = nullptr;
  GOOGLESQL_ASSERT_OK(element_table->GetDynamicProperties(dynamic_properties));
  ASSERT_NE(dynamic_properties, nullptr);
  ASSERT_TRUE(dynamic_properties->GetValueExpression().ok());

  const Table* dynamic_graph_node;
  GOOGLESQL_ASSERT_OK(catalog.FindTable({"DynamicGraphNode"}, &dynamic_graph_node));
  const Column* dynamic_label_column = nullptr;
  GOOGLESQL_ASSERT_OK(catalog.FindPropertyGraphSqlColumn(
      *dynamic_graph_node, "dyn_graph",
      SimpleCatalog::PropertyGraphSqlColumnKind::kDynamicLabel,
      &dynamic_label_column));
  ASSERT_NE(dynamic_label_column, nullptr);
  ASSERT_TRUE(dynamic_label_column->HasGeneratedExpression());

  const Column* dynamic_properties_column = nullptr;
  GOOGLESQL_ASSERT_OK(catalog.FindPropertyGraphSqlColumn(
      *dynamic_graph_node, "dyn_graph",
      SimpleCatalog::PropertyGraphSqlColumnKind::kDynamicProperties,
      &dynamic_properties_column));
  ASSERT_NE(dynamic_properties_column, nullptr);
  ASSERT_TRUE(dynamic_properties_column->HasGeneratedExpression());

  const Column* dynamic_label_via_graph = nullptr;
  GOOGLESQL_ASSERT_OK(catalog.FindPropertyGraphSqlColumn(
      *graph, *element_table,
      SimpleCatalog::PropertyGraphSqlColumnKind::kDynamicLabel,
      &dynamic_label_via_graph));
  EXPECT_EQ(dynamic_label_via_graph, dynamic_label_column);
  const Column* dynamic_label_via_helper = nullptr;
  GOOGLESQL_ASSERT_OK(googlesql::FindPropertyGraphDynamicLabelColumn(
      *graph, *element_table, &dynamic_label_via_helper));
  EXPECT_EQ(dynamic_label_via_helper, dynamic_label_column);
  const Column* dynamic_label_via_catalog_helper = nullptr;
  GOOGLESQL_ASSERT_OK(catalog.FindPropertyGraphDynamicLabelColumn(
      *graph, *element_table, &dynamic_label_via_catalog_helper));
  EXPECT_EQ(dynamic_label_via_catalog_helper, dynamic_label_column);

  const Column* dynamic_properties_via_helper = nullptr;
  GOOGLESQL_ASSERT_OK(googlesql::FindPropertyGraphDynamicPropertiesColumn(
      *graph, *element_table, &dynamic_properties_via_helper));
  EXPECT_EQ(dynamic_properties_via_helper, dynamic_properties_column);
  const Column* dynamic_properties_via_catalog_helper = nullptr;
  GOOGLESQL_ASSERT_OK(catalog.FindPropertyGraphDynamicPropertiesColumn(
      *graph, *element_table, &dynamic_properties_via_catalog_helper));
  EXPECT_EQ(dynamic_properties_via_catalog_helper, dynamic_properties_column);
  PropertyGraphElementSqlColumns dynamic_sql_columns;
  GOOGLESQL_ASSERT_OK(googlesql::FindPropertyGraphElementSqlColumns(
      *graph, *element_table, dynamic_sql_columns));
  EXPECT_TRUE(dynamic_sql_columns.property_columns.empty());
  EXPECT_EQ(dynamic_sql_columns.dynamic_label, dynamic_label_column);
  EXPECT_EQ(dynamic_sql_columns.dynamic_properties, dynamic_properties_column);
  const PropertyGraphElementMetadata dynamic_metadata =
      googlesql::GetPropertyGraphElementMetadata(*element_table);
  EXPECT_EQ(dynamic_metadata.kind, GraphElementTable::Kind::kNode);
  EXPECT_TRUE(dynamic_metadata.property_declarations.empty());
  EXPECT_TRUE(dynamic_metadata.has_dynamic_label);
  EXPECT_TRUE(dynamic_metadata.has_dynamic_properties);

  FileDescriptorSetMap file_descriptor_set_map;
  SimpleCatalogProto proto;
  GOOGLESQL_ASSERT_OK(catalog.Serialize(&file_descriptor_set_map, &proto));

  std::vector<const google::protobuf::DescriptorPool*> pools(
      file_descriptor_set_map.size());
  for (const auto& [pool, descriptor_set] : file_descriptor_set_map) {
    pools[descriptor_set->descriptor_set_index] = pool;
  }

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SimpleCatalog> restored_catalog,
      SimpleCatalog::Deserialize(proto, pools));
  const PropertyGraph* restored_graph;
  GOOGLESQL_ASSERT_OK(
      restored_catalog->FindPropertyGraph({"dyn_graph"}, &restored_graph));
  const GraphElementTable* restored_element_table;
  GOOGLESQL_ASSERT_OK(
      restored_graph->FindElementTableByName("DynamicGraphNode",
                                             restored_element_table));

  const GraphDynamicLabel* restored_dynamic_label = nullptr;
  GOOGLESQL_ASSERT_OK(
      restored_element_table->GetDynamicLabel(restored_dynamic_label));
  ASSERT_NE(restored_dynamic_label, nullptr);
  ASSERT_TRUE(restored_dynamic_label->GetValueExpression().ok());

  const GraphDynamicProperties* restored_dynamic_properties = nullptr;
  GOOGLESQL_ASSERT_OK(restored_element_table->GetDynamicProperties(
      restored_dynamic_properties));
  ASSERT_NE(restored_dynamic_properties, nullptr);
  ASSERT_TRUE(restored_dynamic_properties->GetValueExpression().ok());

  const Table* restored_dynamic_graph_node;
  GOOGLESQL_ASSERT_OK(
      restored_catalog->FindTable({"DynamicGraphNode"}, &restored_dynamic_graph_node));
  const Column* restored_dynamic_label_column = nullptr;
  GOOGLESQL_ASSERT_OK(restored_catalog->FindPropertyGraphSqlColumn(
      *restored_dynamic_graph_node, "dyn_graph",
      SimpleCatalog::PropertyGraphSqlColumnKind::kDynamicLabel,
      &restored_dynamic_label_column));
  ASSERT_NE(restored_dynamic_label_column, nullptr);

  const Column* restored_dynamic_properties_column = nullptr;
  GOOGLESQL_ASSERT_OK(restored_catalog->FindPropertyGraphSqlColumn(
      *restored_dynamic_graph_node, "dyn_graph",
      SimpleCatalog::PropertyGraphSqlColumnKind::kDynamicProperties,
      &restored_dynamic_properties_column));
  ASSERT_NE(restored_dynamic_properties_column, nullptr);

  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  GOOGLESQL_EXPECT_OK(AnalyzeStatement(
      "SELECT n.dyn_graph_dynamic_label, n.dyn_graph_dynamic_properties "
      "FROM DynamicGraphNode AS n",
      analyzer_options, restored_catalog.get(), restored_catalog->type_factory(),
      &analyzer_output));
}

// Helper function to analyze a CREATE TABLE statement and return the resolved
// statement.
static absl::StatusOr<std::unique_ptr<const AnalyzerOutput>>
AnalyzeCreateTableStmt(absl::string_view sql, AnalyzerOptions& options,
                       Catalog& catalog, TypeFactory& type_factory) {
  options.mutable_language()->AddSupportedStatementKind(
      RESOLVED_CREATE_TABLE_STMT);
  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  GOOGLESQL_RETURN_IF_ERROR(AnalyzeStatement(sql, options, &catalog, &type_factory,
                                   &analyzer_output));
  if (!analyzer_output->resolved_statement()->Is<ResolvedCreateTableStmt>()) {
    return absl::InvalidArgumentError(
        "Statement is not a CREATE TABLE statement");
  }
  return analyzer_output;
}

struct MakeTableFromCreateTableTestParams {
  std::string name;
  std::string create_sql;
  absl::Status expected_analysis_status = absl::OkStatus();
  absl::Status expected_make_table_status = absl::OkStatus();
  std::optional<std::vector<int>> expected_primary_key;
};

class MakeTableFromCreateTableTest
    : public ::testing::TestWithParam<MakeTableFromCreateTableTestParams> {
 protected:
  MakeTableFromCreateTableTest()
      : catalog_("simple"), analyzer_options_([] {
          AnalyzerOptions options;
          options.mutable_language()->AddSupportedStatementKind(
              RESOLVED_CREATE_TABLE_STMT);
          options.mutable_language()->EnableLanguageFeature(
              FEATURE_UNENFORCED_PRIMARY_KEYS);
          return options;
        }()) {}

  SimpleCatalog catalog_;
  AnalyzerOptions analyzer_options_;
  TypeFactory type_factory_;
};

TEST_P(MakeTableFromCreateTableTest, MakeTableFromCreateTable) {
  const MakeTableFromCreateTableTestParams& params = GetParam();

  absl::StatusOr<std::unique_ptr<const AnalyzerOutput>> analyzer_output =
      AnalyzeCreateTableStmt(params.create_sql, analyzer_options_, catalog_,
                             type_factory_);

  if (!params.expected_analysis_status.ok()) {
    EXPECT_THAT(analyzer_output.status(),
                StatusIs(params.expected_analysis_status.code(),
                         HasSubstr(params.expected_analysis_status.message())));
    return;
  }
  GOOGLESQL_ASSERT_OK(analyzer_output);

  const auto* stmt = (*analyzer_output)
                         ->resolved_statement()
                         ->GetAs<ResolvedCreateTableStmt>();

  absl::StatusOr<std::unique_ptr<SimpleTable>> table =
      MakeTableFromCreateTable(*stmt);

  if (!params.expected_make_table_status.ok()) {
    EXPECT_THAT(
        table.status(),
        StatusIs(params.expected_make_table_status.code(),
                 HasSubstr(params.expected_make_table_status.message())));
    return;
  }
  GOOGLESQL_ASSERT_OK(table);

  if (params.expected_primary_key.has_value()) {
    EXPECT_TRUE((*table)->PrimaryKey().has_value());
    EXPECT_THAT((*table)->PrimaryKey().value(),
                testing::ElementsAreArray(params.expected_primary_key.value()));
  } else {
    EXPECT_FALSE((*table)->PrimaryKey().has_value());
  }
}

INSTANTIATE_TEST_SUITE_P(
    MakeTableFromCreateTableTests, MakeTableFromCreateTableTest,
    ::testing::ValuesIn<MakeTableFromCreateTableTestParams>({
        {
            .name = "NoPrimaryKey",
            .create_sql = "CREATE TABLE t (c1 INT64)",
        },
        {
            .name = "PrimaryKeySingleColumn",
            .create_sql =
                "CREATE TABLE t (k1 INT64, v STRING, PRIMARY KEY (k1))",
            .expected_primary_key = std::vector<int>{0},
        },
        {
            .name = "PrimaryKeyMultipleColumns",
            .create_sql = "CREATE TABLE t (c1 INT64, c2 STRING, c3 BOOL, "
                          "PRIMARY KEY (c2, c1))",
            .expected_primary_key = std::vector<int>{1, 0},
        },
        {
            .name = "PrimaryKeyZeroColumns",
            .create_sql = "CREATE TABLE t (c1 INT64, c2 STRING, c3 BOOL, "
                          "PRIMARY KEY ())",
            .expected_primary_key = std::vector<int>{},
        },
        {
            .name = "PrimaryKeyUnenforcedFieldNotAccessed",
            .create_sql =
                "CREATE TABLE t (c1 INT64, PRIMARY KEY (c1) NOT ENFORCED)",
            .expected_make_table_status = absl::UnimplementedError(
                "Unimplemented feature (ResolvedPrimaryKey::unenforced not "
                "accessed and has non-default value)"),
        },
        {
            .name = "PrimaryKeyCaseInsensitive",
            .create_sql = "CREATE TABLE t (ColA INT64, ColB STRING, PRIMARY "
                          "KEY (cOLb, COLa))",
            .expected_primary_key = std::vector<int>{1, 0},
        },
        {
            .name = "PrimaryKeyColumnNotFound",
            .create_sql =
                "CREATE TABLE t (c1 INT64, PRIMARY KEY (non_existent))",
            .expected_analysis_status = absl::InvalidArgumentError(
                "Unsupported primary key column non_existent either does not "
                "exist or is a pseudocolumn"),
        },
    }),
    [](const ::testing::TestParamInfo<MakeTableFromCreateTableTest::ParamType>&
           info) { return info.param.name; });

}  // namespace googlesql
