#
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

load("@rules_cc//cc:cc_binary.bzl", "cc_binary")
load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_cc//cc:cc_test.bzl", "cc_test")

package(
    default_visibility = ["//googlesql/base:googlesql_implementation"],
)

cc_library(
    name = "bird_eval_runner",
    srcs = [
        "bird_eval_rewriter.cc",
        "bird_eval_runner.cc",
        "bird_eval_sqlite.cc",
    ],
    hdrs = [
        "bird_eval_rewriter.h",
        "bird_eval_runner.h",
        "bird_eval_sqlite.h",
    ],
    linkopts = [
        "-lsqlite3",
    ],
    deps = [
        "//googlesql/base:file_util",
        "//googlesql/base:path",
        "//googlesql/base:ret_check",
        "//googlesql/base:status",
        "//googlesql/parser",
        "//googlesql/public:analyzer",
        "//googlesql/public:analyzer_output",
        "//googlesql/public:builtin_function_options",
        "//googlesql/public:evaluator",
        "//googlesql/public:evaluator_table_iterator",
        "//googlesql/public:language_options",
        "//googlesql/public:simple_catalog",
        "//googlesql/public:type",
        "//googlesql/public:value",
        "//googlesql/public/types",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_binary(
    name = "bird_eval",
    srcs = ["bird_eval.cc"],
    visibility = ["//visibility:public"],
    deps = [
        ":bird_eval_runner",
        "//googlesql/base:file_util",
        "//googlesql/base:status",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/flags:parse",
        "@com_google_absl//absl/flags:usage",
        "@com_google_absl//absl/log:initialize",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "bird_eval_runner_test",
    srcs = ["bird_eval_runner_test.cc"],
    linkopts = [
        "-lsqlite3",
    ],
    deps = [
        ":bird_eval_runner",
        "//googlesql/base:file_util",
        "//googlesql/base:path",
        "//googlesql/base/testing:googlesql_gtest_main",
        "//googlesql/base/testing:status_matchers",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
    ],
)
