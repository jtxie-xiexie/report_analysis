# Output Contract

## Result Table Schema

Whether using `mrExecute` or `execute`, the final result must contain exactly these columns:

- `tradeTime`
- `securityId`
- `factorname`
- `value`

The `factorname` values must equal the input `factor`.

## Final Summary Fields

The final plain-text summary must include:

- `factor_chinese_name`
- `hypothesis`
- `description`
- `formulation`
- `feedback`
- `code`
- `executeType`
- `hypothesis_feedback`

## File Materialization

Persist the final runnable code with `scripts/write_dos.py`.

- `execute` output file: `<factor>.dos`
- `mrExecute` output file: `<factor>_mr.dos`

- Save the final conclusion payload as `result.json`
- Save the evaluation dictionary as `evaluation.json`
