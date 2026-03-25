---
name: ddb-show-table-schema
description: Show DolphinDB table schema for a specified DFS database and table by executing Dolphin Script (.dos). Use when users ask to inspect table columns, column types, partition information, engine type, or table metadata, while reusing the shared Python gateway run_dos.py.
---

# DDB Show Table Schema

Use this skill to query schema metadata for one DolphinDB table.
Keep logic in `.dos` and execute through the shared framework gateway.

## Inputs

- `dbUrl`: database path, for example `dfs://sample_demo`
- `tableName`: table name, for example `stock_daily`

## Workflow

1. Validate connection and parameters.
- Ensure `dbUrl` and `tableName` are provided.

2. Run schema query script.
- Execute `references/show-table-schema.dos`.

3. Optional: output only column definition table.
- Execute `references/show-coldefs-only.dos`.

## Commands

Get full schema object:

```powershell
python ../dolphindb-dos-skill-framework/scripts/run_dos.py `
  --host 192.168.100.43 `
  --port 7305 `
  --user admin `
  --password 123456 `
  --ssl `
  --script references/show-table-schema.dos `
  --set dbUrl=dfs://sample_demo `
  --set tableName=stock_daily
```

Get columns only:

```powershell
python ../dolphindb-dos-skill-framework/scripts/run_dos.py `
  --host 192.168.100.43 `
  --port 7305 `
  --user admin `
  --password 123456 `
  --ssl `
  --script references/show-coldefs-only.dos `
  --set dbUrl=dfs://sample_demo `
  --set tableName=stock_daily
```

## Resources

- `references/show-table-schema.dos`: return full `schema(loadTable(...))`
- `references/show-coldefs-only.dos`: return only `colDefs` part for quick field checks
