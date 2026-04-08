# BIRD Eval

`bird_eval` runs layered verification for BIRD-Bench SQL against:

- native SQLite execution
- GoogleSQL parse/analyze/execute

The current tool is aimed at validating `gold SQL`, not NL2SQL generation.

## What It Does

For each BIRD sample, the tool:

1. loads the sample from the BIRD JSON file
2. resolves the corresponding SQLite database
3. executes the original SQL in SQLite
4. tries GoogleSQL `parse -> analyze -> execute`
5. applies a small deterministic rewrite set when needed
6. reports the failure stage or result match

Current rewrite rules:

- `ifnull(...) -> coalesce(...)`
- `length(...) -> char_length(...)`
- `LIMIT offset, count -> LIMIT count OFFSET offset`

## Build

```bash
bazelisk build //googlesql/tools/bird_eval:bird_eval
```

## Run

```bash
bazelisk run //googlesql/tools/bird_eval:bird_eval -- \
  --bird_path=/path/to/bird/dev.json \
  --sqlite_db_root=/path/to/bird/dev_databases \
  --sample_limit=50 \
  --details_out=/tmp/bird_eval.jsonl \
  --report_out=/tmp/bird_eval.md
```

Optional flags:

- `--db_filter=<db_id>`
- `--sample_limit=<n>`
- `--details_out=<jsonl>`
- `--report_out=<markdown>`

## Output

The Markdown summary includes aggregate counts for:

- total samples
- parse ok
- analyze ok
- SQLite execute ok
- GoogleSQL execute ok
- result match

The JSONL detail file includes per-sample fields such as:

- `sample_id`
- `db_id`
- `original_sql`
- `normalized_sql`
- `rewrite_rules`
- `failure_stage`
- `error_message`
