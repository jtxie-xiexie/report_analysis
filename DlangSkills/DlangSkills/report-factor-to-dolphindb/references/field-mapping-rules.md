# Field Mapping Rules

Only use real fields confirmed by `coldefs` and `testsql`.

## Allowed Synonym Mapping

Map carefully when actual fields confirm the meaning, for example:

- security field: `securityId`, `SecurityID`, `symbol`, `ticker`
- time field: `tradeTime`, `datetime`, `date`, `DateTime`, `TradeTime`
- price field: `close`, `Close`, `lastPrice`, `LastPx`
- volume field: `volume`, `Volume`, `vol`, `totalVolume`
- amount field: `amount`, `turnover`, `value`

## Rules

- Prefer exact field names from the table.
- Use synonyms only after verifying with schema and samples.
- Never invent a field because it is common in another dataset.
- If the field mapping is ambiguous, keep exploring instead of guessing.
