---
name: dolphindb-dos-skill-framework
description: Build and maintain DolphinDB-driven skills where core business logic is written in Dolphin Script (.dos) and executed with the DolphinDB Python API. Use when creating new domain skills on DolphinDB, designing reusable .dos modules, adding Python execution wrappers, passing runtime parameters, validating outputs, or troubleshooting connection/session/auth errors.
---

# DolphinDB DOS Skill Framework

Use this skill to standardize how Codex builds task-specific DolphinDB skills.
Keep business logic in `.dos` files and run them through Python wrappers for repeatable execution and testing.

## Architecture Rule

- Keep exactly one Python gateway: `scripts/run_dos.py`.
- Do not create new task-specific Python scripts for downstream DolphinDB skills.
- Add new capabilities by adding `.dos` files and corresponding `SKILL.md` instructions only.
- Let all DolphinDB execution go through `scripts/run_dos.py`.

## Workflow

1. Confirm task contract.
- Define input fields, expected output schema, and idempotency requirements.
- Define whether the task is query-only, compute-only, or mixed.

2. Create task layout.
- Follow this structure for each downstream domain skill:
```text
<domain-skill>/
  SKILL.md
  scripts/
  dos/
  tests/
  references/
```
- Keep all executable Dolphin logic in `dos/`.

3. Implement `.dos` modules.
- Write deterministic functions with explicit parameters.
- Return scalar, table, or dictionary outputs with stable column names and types.
- Read style and naming rules in `references/dos-style-guide.md`.

4. Wire Python execution.
- Use only `scripts/run_dos.py` to connect and execute `.dos` files.
- Pass runtime parameters with `--set key=value`.
- Do not add new Python wrappers for task logic.

5. Validate quickly.
- Run a connection smoke test first.
- Run representative `.dos` scripts with realistic parameters.
- Add at least one positive test and one edge-case test in `tests/`.

6. Package downstream skill content.
- Keep SKILL.md concise and task-focused.
- Move detailed tables, schemas, and business rules into `references/`.

## Runtime Pattern

Execute a script file:

```powershell
python scripts/run_dos.py `
  --host 127.0.0.1 `
  --port 8848 `
  --user admin `
  --password 123456 `
  --script dos/main.dos `
  --set start_date=2025-01-01 `
  --set end_date=2025-01-31
```

Run ad-hoc code:

```powershell
python scripts/run_dos.py `
  --host 127.0.0.1 `
  --port 8848 `
  --user admin `
  --password 123456 `
  --eval "1+1"
```

Inspect current connection catalog info (no extra Python script):

```powershell
python scripts/run_dos.py `
  --host 192.168.100.43 `
  --port 7305 `
  --user admin `
  --password 123456 `
  --ssl `
  --script references/list-catalog.dos
```

Inspect one table schema:

```powershell
python scripts/run_dos.py `
  --host 192.168.100.43 `
  --port 7305 `
  --user admin `
  --password 123456 `
  --ssl `
  --script references/show-table-schema.dos `
  --set dbUrl=dfs://sample_demo `
  --set tableName=stock_daily
```

## Resource Navigation

- Use `scripts/run_dos.py` for generic script execution via Python API.
- Use `scripts/smoke_test_connection.py` for fast connectivity checks.
- Use `references/list-catalog.dos` to inspect current connection catalog.
- Use `references/show-table-schema.dos` to inspect one table schema.
- Read `references/dos-style-guide.md` before writing new `.dos` modules.
- Read `references/downstream-skill-template.md` when scaffolding a new domain skill from this framework.

## Output Requirements

- Keep business rules in `.dos`; keep orchestration in Python.
- Keep scripts parameterized; avoid hardcoded credentials, dates, and symbols.
- Keep result schema stable across runs.
