# Workflow Contract

Follow the workflow exactly.

## Step 0

Before any tool call, output plain text with:

- factor Chinese explanation
- economic meaning
- mathematical expression
- initial field requirements
- initial guess for target frequency, cross-sectional behavior, and slide window behavior

Do not call any tool before this step.

## Step 1

For each candidate data source, call:

`{"function":"coldefs","args":{"dbname":"xxx","tbname":"xxx"}}`

Use it to confirm possible security, time, price, volume, and amount fields.

## Step 2

After Step 1, call:

`{"function":"testsql","args":{"dbname":"xxx","tbname":"xxx"}}`

Use it to verify sample values, field presence, time granularity, and whether the table is realistic for the factor.

## Step 3

After Steps 1 and 2, call:

`{"function":"mrEligible","args":{"dbName":"xxx","tbName":"xxx","targetFreq":"DAY","isCS":false,"isSlide":true,"computeFreq":"DAY","windowSize":20,"forceMR":null}}`

This is the only legal mode-decision point.

## Step 4

If MR is eligible, use `mrExecute`.
If the table is usable but MR is not, use `execute`.
If execution fails, repair and retry.

## Step 5

After successful execution or final code completion, output plain text with:

- `factor_chinese_name`
- `hypothesis`
- `description`
- `formulation`
- `feedback`
- `code`
- `executeType`
- `hypothesis_feedback`

Do not output tool JSON in the final step.
