#!/usr/bin/env python3
"""Run bird_eval one-sample-at-a-time with per-sample timeout.

This script is a pragmatic fallback when a single query can hang inside
GoogleSQL execution and block the whole benchmark run.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
  parser = argparse.ArgumentParser()
  parser.add_argument("--bird_path", required=True, help="Path to BIRD dev json")
  parser.add_argument("--sqlite_db_root", required=True, help="Root of sqlite DBs")
  parser.add_argument(
      "--bird_eval_bin",
      default="./bazel-bin/googlesql/tools/bird_eval/bird_eval",
      help="Path to compiled bird_eval binary",
  )
  parser.add_argument("--sample_limit", type=int, default=10)
  parser.add_argument("--start_index", type=int, default=0)
  parser.add_argument("--timeout_sec", type=int, default=30)
  parser.add_argument("--details_out", required=True, help="Aggregated jsonl output")
  parser.add_argument("--report_out", required=True, help="Aggregated markdown report")
  parser.add_argument("--log_every", type=int, default=10, help="Progress log interval")
  return parser.parse_args()


def default_timeout_result(sample: dict[str, Any], idx: int) -> dict[str, Any]:
  db_id = sample.get("db_id", "")
  return {
      "sample_id": f"{db_id}:{idx}",
      "db_id": db_id,
      "question": sample.get("question", ""),
      "original_sql": sample.get("SQL", ""),
      "normalized_sql": sample.get("SQL", ""),
      "rewrite_rules": [],
      "parse_ok": False,
      "analyze_ok": False,
      "sqlite_execute_ok": False,
      "googlesql_execute_ok": False,
      "result_match": False,
      "failure_stage": "timeout",
      "error_message": "bird_eval subprocess timed out",
  }


def runner_error_result(
    sample: dict[str, Any], idx: int, stage: str, msg: str
) -> dict[str, Any]:
  base = default_timeout_result(sample, idx)
  base["failure_stage"] = stage
  base["error_message"] = msg
  return base


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
  with path.open("w", encoding="utf-8") as f:
    for row in rows:
      f.write(json.dumps(row, ensure_ascii=False))
      f.write("\n")


def summarize(rows: list[dict[str, Any]]) -> str:
  total = len(rows)
  passed = sum(1 for r in rows if r.get("failure_stage") == "ok")
  stage_counts: dict[str, int] = {}
  reason_counts: dict[str, int] = {}
  for row in rows:
    stage = str(row.get("failure_stage", "unknown"))
    stage_counts[stage] = stage_counts.get(stage, 0) + 1
    if stage == "ok":
      continue
    msg = str(row.get("error_message", ""))
    reason = "other"
    if stage == "timeout":
      reason = "timeout"
    elif "Function not found: strftime" in msg or "Function not found: STRFTIME" in msg:
      reason = "missing_strftime"
    elif "Function not found: IIF" in msg:
      reason = "missing_iif"
    elif "No matching signature for operator = for argument types: STRING, INT64" in msg:
      reason = "string_int_equality"
    elif "No matching signature for operator BETWEEN" in msg:
      reason = "between_type_mismatch"
    elif "No matching signature for aggregate function SUM" in msg:
      reason = "sum_bool_expr"
    elif "Out of memory for MemoryAccountant" in msg:
      reason = "oom_intermediate"
    elif "SQLite and GoogleSQL results differ" in msg:
      reason = "result_mismatch"
    reason_counts[reason] = reason_counts.get(reason, 0) + 1

  lines = [
      "# BIRD Eval Timeout Runner Report",
      "",
      f"- total_samples: {total}",
      f"- passed: {passed}",
      f"- pass_rate: {passed / total:.2%}" if total else "- pass_rate: 0.00%",
      "",
      "## Failure Stage Breakdown",
      "",
      "| failure_stage | count |",
      "| --- | ---: |",
  ]
  for stage, count in sorted(stage_counts.items(), key=lambda x: (-x[1], x[0])):
    lines.append(f"| {stage} | {count} |")
  if reason_counts:
    lines.extend(
        [
            "",
            "## Failure Reason Breakdown",
            "",
            "| reason | count |",
            "| --- | ---: |",
        ]
    )
    for reason, count in sorted(reason_counts.items(), key=lambda x: (-x[1], x[0])):
      lines.append(f"| {reason} | {count} |")
  return "\n".join(lines) + "\n"


def main() -> int:
  args = parse_args()
  bird_path = Path(args.bird_path)
  details_out = Path(args.details_out)
  report_out = Path(args.report_out)
  bird_eval_bin = Path(args.bird_eval_bin)

  with bird_path.open("r", encoding="utf-8") as f:
    samples = json.load(f)

  selected = samples[args.start_index : args.start_index + args.sample_limit]
  results: list[dict[str, Any]] = []

  for offset, sample in enumerate(selected):
    idx = args.start_index + offset
    with tempfile.TemporaryDirectory(prefix=f"bird_eval_{idx}_") as temp_dir:
      temp_dir_path = Path(temp_dir)
      one_sample_path = temp_dir_path / "sample.json"
      one_details = temp_dir_path / "details.jsonl"
      one_report = temp_dir_path / "report.md"
      with one_sample_path.open("w", encoding="utf-8") as f:
        json.dump([sample], f, ensure_ascii=False)

      cmd = [
          str(bird_eval_bin),
          f"--bird_path={one_sample_path}",
          f"--sqlite_db_root={args.sqlite_db_root}",
          "--sample_limit=1",
          f"--details_out={one_details}",
          f"--report_out={one_report}",
      ]
      try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=args.timeout_sec,
            check=False,
        )
      except subprocess.TimeoutExpired:
        results.append(default_timeout_result(sample, idx))
        continue

      if proc.returncode != 0:
        err = proc.stderr.strip() or proc.stdout.strip() or "bird_eval failed"
        results.append(runner_error_result(sample, idx, "runner_error", err))
        continue

      if not one_details.exists():
        results.append(
            runner_error_result(
                sample,
                idx,
                "runner_error",
                "bird_eval succeeded but details_out missing",
            )
        )
        continue

      lines = [ln for ln in one_details.read_text(encoding="utf-8").splitlines() if ln]
      if not lines:
        results.append(
            runner_error_result(
                sample,
                idx,
                "runner_error",
                "bird_eval produced empty details_out",
            )
        )
        continue
      row = json.loads(lines[0])
      row["sample_id"] = f"{sample.get('db_id', '')}:{idx}"
      results.append(row)
    done = offset + 1
    if args.log_every > 0 and (done % args.log_every == 0 or done == len(selected)):
      ok_count = sum(1 for r in results if r.get("failure_stage") == "ok")
      print(
          f"[run_with_timeout] progress {done}/{len(selected)} "
          f"ok={ok_count} fail={len(results) - ok_count}",
          flush=True,
      )

  write_jsonl(details_out, results)
  report_out.write_text(summarize(results), encoding="utf-8")
  return 0


if __name__ == "__main__":
  raise SystemExit(main())
