# Tool Contract

Use only these tools:

- `coldefs(dbname, tbname)`
- `testsql(dbname, tbname)`
- `mrEligible(dbName, tbName, targetFreq, isCS, isSlide, computeFreq = NULL, windowSize = NULL, forceMR = NULL)`
- `mrExecute(mapFuncString, dbName, tbName)`
- `execute(code)`
- `getAllAvailableData()`

## Usage Rules

- Use exactly one tool call per response when operating in the strict agent protocol.
- Output plain text only for Step 0 and Step 5.
- Output tool-call JSON only for Steps 1 to 4.
- Never mix explanation text with a tool JSON payload.

## Discovery Rule

- Only call `getAllAvailableData()` when the normalized input has no `dataSources` or an empty `dataSources` list.
- Treat its result as candidate discovery only.
- You must still call `coldefs`, `testsql`, and `mrEligible` before generating executable code for any discovered table.
- Prefer the direct `getAllAvailableData()` function for the skill workflow; reserve generic catalog inspection for framework debugging.

## Mode Selection Rule

- Never choose `mrExecute` or `execute` before `mrEligible`.
- `mrEligible` remains the only legal mode selector, even when `forceMR` is present.
- Pass through the incoming `forceMR` value exactly.
