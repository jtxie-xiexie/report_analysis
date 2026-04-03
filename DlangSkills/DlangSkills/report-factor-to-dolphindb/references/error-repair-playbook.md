# Error Repair Playbook

When execution fails, repair the code before giving up.

## Must Repair

- `EXECUTE_ERROR`
- syntax errors
- missing fields
- type mismatches
- wrong time column handling
- wrong grouping
- slide-window misuse
- partition limitations
- missing required output columns
- mismatched `factorname`

## Repair Process

1. Read the error carefully.
2. Identify whether the issue is:
- wrong field mapping
- wrong type or frequency assumption
- wrong grouping or sliding-window logic
- MR incompatibility
- output-shape violation
3. Modify the code directly.
4. Re-run the same execution path.
5. If the table is fundamentally unusable, move to the next data source.

## Never Do

- do not use `try-catch`
- do not claim success without a successful run in validate mode
- do not fabricate columns to bypass an error
