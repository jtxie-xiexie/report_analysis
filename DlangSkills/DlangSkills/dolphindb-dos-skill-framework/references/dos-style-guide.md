# Dolphin Script Style Guide

Use this guide when writing `.dos` files in downstream skills.

## 1. Module Structure

- Put each business capability in an isolated `.dos` file.
- Group shared utility functions in `dos/common.dos`.
- Keep script top-level side effects minimal.

## 2. Function Design

- Use explicit input arguments instead of implicit globals.
- Return deterministic outputs for the same inputs.
- Prefer table output with stable column names for analytics tasks.
- Use dictionary output for multi-part results:
  - `result["data"]`: primary data
  - `result["meta"]`: execution info (time range, version, row count)

## 3. Naming Conventions

- Use `lowerCamelCase` for functions and local variables.
- Use nouns for data tables and verbs for transformation functions.
- Prefix reusable utility functions with `util`.

## 4. Parameter Handling

- Receive runtime parameters from Python-uploaded variables.
- Validate required parameters at script start.
- Provide clear error messages with the parameter name and expected type.

## 5. Result Stability

- Keep output schema unchanged unless a version upgrade is intentional.
- Add new fields at the end for backward compatibility.
- Sort key outputs when order matters for tests.

## 6. Debugging Conventions

- Add temporary debug prints only during troubleshooting.
- Remove debug-only code before finalizing the script.
- Isolate risky expressions into named intermediate variables to simplify diagnosis.
