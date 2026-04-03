# DolphinDB Pipeline Design

This document describes the recommended split for a DolphinDB-first factor pipeline.

## Design Goal

Move business logic into DolphinDB and keep Python as a thin shell for:

- PDF text extraction
- local file output
- optional plotting

Keep these business capabilities inside DolphinDB:

- data-source discovery
- schema inspection
- factor-family classification
- field mapping
- mode selection through `mrEligible`
- factor execution
- factor evaluation
- structured result assembly

## Recommended Split

### Python shell

Python should do only:

- normalize external input
- call one DolphinDB entry function
- write `.dos`, `result.json`, and `evaluation.json`
- optionally render plots later

### DolphinDB core

DolphinDB should own:

- `normalizeTask`
- `discoverSources`
- `inspectSource`
- `classifyFactor`
- `buildFactorCode`
- `chooseExecuteMode`
- `choosePriceSource`
- `runFactorCode`
- `runEvaluation`
- `buildResultSummary`
- `runFactorPipeline`

## Function Responsibilities

### `normalizeTask`

Input:

- task dictionary with `content`, `factor`, `dataSources`, `forceMR`

Output:

- validated task dictionary with defaults filled

### `discoverSources`

Input:

- normalized task

Behavior:

- if `dataSources` is empty, call `getAllAvailableData`
- return ordered candidate sources

### `inspectSource`

Input:

- `dbName`, `tbName`

Behavior:

- fetch `colDefs`
- fetch top sample rows
- infer security field, time field, price field, volume field
- infer frequency

Output:

- dictionary with schema, samples, mappings, and frequency

### `classifyFactor`

Input:

- factor name
- report text

Behavior:

- classify into families such as:
  - `volatility`
  - `momentum`
  - `price_volume`
  - `cross_sectional`
  - `intraday`

Output:

- family name plus template config such as `targetFreq`, `isCS`, `isSlide`, `computeFreq`, `windowSize`

### `buildFactorCode`

Input:

- factor family
- field mapping
- template config

Behavior:

- generate final DolphinDB code string

Output:

- `code`
- template metadata

### `chooseExecuteMode`

Input:

- source info
- template config
- `forceMR`

Behavior:

- call `mrEligible`
- choose `execute` or `mrExecute`

### `choosePriceSource`

Behavior:

- if factor source is daily, reuse it
- if factor source is minute or higher frequency, find a daily price table from candidates

Output:

- `priceDbname`
- `priceTbname`
- `priceDate`
- `priceSymbol`
- `priceCol`

### `runFactorCode`

Behavior:

- execute generated factor code
- return `factorResult`

### `runEvaluation`

Behavior:

- call `generateAnalysisReport`
- return evaluation dictionary

### `buildResultSummary`

Behavior:

- assemble final result dictionary
- include selected source, mode, formula, code, and evaluation source

### `runFactorPipeline`

Behavior:

- orchestrate the full chain

Output:

- `ret["code"]`
- `ret["result"]`
- `ret["evaluation"]`
- `ret["meta"]`

## Suggested Migration Path

1. Keep PDF parsing in Python.
2. Move source discovery, source inspection, factor classification, and evaluation into DolphinDB.
3. Keep Python only for writing artifacts and later plotting.
4. Once stable, optionally expose the DolphinDB entry function directly inside your platform.
