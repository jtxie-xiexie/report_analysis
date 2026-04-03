# Data Source Discovery

Use this guide only when `dataSources` is missing or empty.

## Entry Point

Call:

`{"function":"getAllAvailableData","args":{}}`

Treat its returned list as the only automatic discovery source.
Do not replace it with a generic full-catalog listing unless you are debugging the platform or `getAllAvailableData()` fails.

## Ranking Rules

Prioritize tables that most likely support price-volume factor reconstruction:

1. Tables with obvious security identifier semantics
2. Tables with a clear time column
3. Tables with price columns such as close, open, high, low, last price
4. Tables with volume or amount columns
5. Tables whose granularity matches the report logic

## Tie Breaking

Prefer:

- stock minute or daily market tables over generic demo tables
- tables containing both price and volume fields over partial tables
- tables whose sampling frequency is closer to the report definition

## After Discovery

- Build an ordered internal `dataSources` list
- Evaluate each source strictly with `coldefs`, `testsql`, and `mrEligible`
- Once one data source succeeds, stop switching
- Preserve the order returned by `getAllAvailableData()` unless schema or sample inspection gives a concrete reason to reorder by fitness for the factor

## Failure Handling

If discovery returns no obviously usable tables:

- say which key field categories are missing
- explain why the factor cannot be reproduced faithfully
- suggest the closest feasible fallback without inventing a source
