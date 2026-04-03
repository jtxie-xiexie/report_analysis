---
name: report-factor-to-dolphindb
description: Reconstruct alpha factors from research-report PDFs or extracted report text into runnable DolphinDB scripts. Use when users want Codex to read a research report, infer factor logic, discover candidate DolphinDB data sources, inspect schemas and samples, decide between mrExecute and execute through mrEligible, repair execution errors, and produce final DolphinDB factor code plus a reproducible implementation summary.
---

# Report Factor To DolphinDB

Use this skill to turn a research report into runnable DolphinDB factor code.
Keep the workflow strict, field-driven, and reproducible.

## Inputs

Accept either of these entry forms:

- `pdfPath`: local path to a research-report PDF
- JSON object with:
  - `content`
  - `factor`
  - `dataSources`
  - `forceMR`

Optional runtime fields:

- `mode`: `generate_only` or `generate_and_validate`
- `outputPath`: directory for generated `.dos` files

If both `pdfPath` and `content` are present, prefer `content` and treat the PDF as fallback source material.

## Available Tools

Only use these task tools:

- `coldefs(dbname, tbname)`
- `testsql(dbname, tbname)`
- `mrEligible(dbName, tbName, targetFreq, isCS, isSlide, computeFreq = NULL, windowSize = NULL, forceMR = NULL)`
- `mrExecute(mapFuncString, dbName, tbName)`
- `execute(code)`
- `getAllAvailableData()`

Read [references/tool-contract.md](references/tool-contract.md) before using them.

## Workflow

1. Normalize input.
- If given a PDF path, run `scripts/normalize_input.py` to extract text and build the task JSON.
- If given JSON, validate and normalize it with the same script.
- If `dataSources` is missing or empty, mark the task for data-source discovery.

2. Discover data sources when needed.
- Only if `dataSources` is absent or empty, call `getAllAvailableData()`.
- Use [references/datasource-discovery.md](references/datasource-discovery.md) to rank discovered tables.
- `getAllAvailableData()` never replaces later schema/sample inspection.
- Prefer `getAllAvailableData()` over generic catalog listing because it returns the curated tables intended for AI factor tasks.
- Use generic catalog scripts only when debugging connectivity or when `getAllAvailableData()` itself fails unexpectedly.

3. Perform Step 0 before any tool call.
- Output factor understanding in plain text only.
- Include Chinese explanation, economic meaning, formula, field needs, and frequency/CS/slide guess.
- Stay faithful to the report text.

4. Evaluate candidate tables strictly in order.
- For each candidate source run:
  - `coldefs`
  - `testsql`
  - `mrEligible`
- Do not skip steps.
- Do not choose `mrExecute` or `execute` before `mrEligible`.

5. Generate and optionally validate code.
- If `mrEligible.isEligible = true`, use `mrExecute`.
- If `mrEligible.isEligible = false` but the table can still support the factor, use `execute`.
- If execution fails, fix the code and retry.
- Keep trying until success or until the current data source is clearly unusable.

6. Materialize the final script.
- Save the final runnable code with `scripts/write_dos.py`.
- For `execute`, save `<factor>.dos`.
- For `mrExecute`, save `<factor>_mr.dos`.

7. Finish with the final summary.
- Output plain text only.
- Include the final implementation summary and the full successful code.

## Output Rules

- Follow [references/output-contract.md](references/output-contract.md) for result schema and summary fields.
- Follow [references/workflow-contract.md](references/workflow-contract.md) for the required Step 0 to Step 5 sequence.
- Follow [references/error-repair-playbook.md](references/error-repair-playbook.md) when execution fails.
- Follow [references/field-mapping-rules.md](references/field-mapping-rules.md) when mapping real fields.
- Persist three artifacts for each completed run: the final `.dos` file, the final `result.json`, and the final `evaluation.json`.

## Execution Notes

- In `generate_only` mode, stop after producing the final code and summary.
- In `generate_and_validate` mode, run the relevant DolphinDB execution tool and repair failures before finalizing.
- Never invent fields, never fake success, and never mix plain text with tool JSON in the same response.
- When materializing `.dos` or JSON fixtures on Windows, avoid BOM-prefixed writes for executable DolphinDB code.
- Prefer `scripts/write_dos.py` or Python UTF-8 writes over PowerShell `Set-Content -Encoding UTF8` for final executable scripts.

## Helper Scripts

- `scripts/normalize_input.py`: normalize PDF or JSON input into one task JSON
- `scripts/write_dos.py`: persist final DolphinDB code into a `.dos` file
- `scripts/run_factor_pipeline.py`: run a reusable end-to-end pipeline that emits code, result, and evaluation artifacts
- `dos/factor_pipeline_core.dos`: DolphinDB-first pipeline core that handles discovery, source inspection, mode selection, code generation, execution, and evaluation

## Demo

- Use `demos/return_volatility20_task.json` for a minimal structured demo input.
- Read [references/demo-run.md](references/demo-run.md) for the demo flow.
- Run `scripts/run_factor_pipeline.py` as the single end-to-end entrypoint for discovery, inspection, mode selection, code generation, file materialization, execution preview, and evaluation.
- Read [references/dolphindb-pipeline-design.md](references/dolphindb-pipeline-design.md) for the recommended DolphinDB-first architecture and function split.

## Example Commands

Normalize a PDF:

```powershell
python scripts/normalize_input.py `
  --pdf path\to\report.pdf `
  --factor Return_Volatility_Ratio `
  --mode generate_only
```

Normalize a JSON request:

```powershell
python scripts/normalize_input.py `
  --json path\to\task.json `
  --mode generate_and_validate `
  --output-path outputs
```

Write the final script:

```powershell
python scripts/write_dos.py `
  --factor Return_Volatility_Ratio `
  --execute-type execute `
  --output-dir outputs `
  --source final_code.dos.txt
```
