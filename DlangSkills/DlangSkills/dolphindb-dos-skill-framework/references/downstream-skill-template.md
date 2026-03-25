# Downstream Skill Template

Use this template to create a domain skill that relies on DolphinDB.

## Folder Layout

```text
my-domain-skill/
  SKILL.md
  dos/
    main.dos
    common.dos
  tests/
    test_smoke.py
  references/
    schema.md
```

## Minimal `SKILL.md` Frontmatter Example

```yaml
---
name: my-domain-skill
description: Execute <domain task> with DolphinDB .dos scripts and Python wrappers. Use when generating or running <domain task> logic against DolphinDB.
---
```

## `dos/main.dos` Example

```dos
// Expect Python to upload startDate/endDate as variables.
if(!exists("startDate")) throw "Missing required parameter: startDate";
if(!exists("endDate")) throw "Missing required parameter: endDate";

t = table(2025.01.01 2025.01.02 as tradeDate, 10.2 10.6 as value);
select * from t where tradeDate >= date(startDate), tradeDate <= date(endDate);
```

## Run Example (use framework gateway only)

```powershell
python <framework-root>/scripts/run_dos.py `
  --host 127.0.0.1 `
  --port 8848 `
  --user admin `
  --password 123456 `
  --script dos/main.dos `
  --set startDate=2025-01-01 `
  --set endDate=2025-01-31
```

## Test Baseline

- Add one smoke test that checks script returns non-empty result.
- Add one edge-case test for empty time range or invalid symbol.
- Keep downstream Python code optional; avoid adding task-specific Python unless absolutely required.
