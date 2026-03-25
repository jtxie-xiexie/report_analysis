---
name: ddb-list-catalog
description: List DolphinDB cluster catalog information (DFS databases and tables) by executing Dolphin Script (.dos). Use when users ask to view current DolphinDB library/database info, table lists, or per-database table counts, while reusing a shared Python gateway instead of creating new task-specific Python scripts.
---

# DDB List Catalog

Use this skill to inspect current connected DolphinDB catalog quickly.
Keep logic in `.dos` files and execute through the shared framework gateway.

## Workflow

1. Check connection.
- Use `../dolphindb-dos-skill-framework/scripts/smoke_test_connection.py` when needed.
- Add `--ssl` if target server requires SSL.

2. List all databases and tables.
- Run `references/list-catalog.dos`.

3. Get per-database counts.
- Run `references/list-catalog-summary.dos`.

## Commands

List all DB-table pairs:

```powershell
python ../dolphindb-dos-skill-framework/scripts/run_dos.py `
  --host 192.168.100.43 `
  --port 7305 `
  --user admin `
  --password 123456 `
  --ssl `
  --script references/list-catalog.dos
```

List per-database table counts:

```powershell
python ../dolphindb-dos-skill-framework/scripts/run_dos.py `
  --host 192.168.100.43 `
  --port 7305 `
  --user admin `
  --password 123456 `
  --ssl `
  --script references/list-catalog-summary.dos
```

## Resources

- `references/list-catalog.dos`: full db/table list
- `references/list-catalog-summary.dos`: grouped table counts by db
