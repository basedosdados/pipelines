# gb_eric_ess — coverage and profiling notes

One wide table per ESS round (design B), partitioned by `year`. Observation level:
individual respondent. Logical key: `[country_id, respondent_id]` (respondent id is
unique within country and round) — verified unique with zero duplicates in every round.

Categorical variables keep their original numeric ESS codes (STRING), decoded via the
`dictionary` table. Stata defined-missing values (No answer / Refusal / Don't know) are
read as null.

## Sparse columns (>95% null)

ESS integrated files stack all countries; country-specific items (party voted, education,
region, language) are null for respondents of other countries, so a large share of columns
is legitimately sparse. This is why `not_null_proportion_multiple_columns` is not emitted
(all round tables exceed ~450 columns, which also fails BigQuery query planning). Null
rates were verified locally instead.

| Table | Rows | Columns | Sparse (>95% null) |
|---|---|---|---|
| round_01 | 42,359 | 567 | 134 |
| round_02 | 47,537 | 604 | 165 |
| round_03 | 43,000 | 519 | 146 |
| round_04 | 56,752 | 674 | 314 |
| round_05 | 52,458 | 675 | 278 |
| round_06 | 54,673 | 625 | 299 |
| round_07 | 40,185 | 602 | 187 |
| round_08 | 44,387 | 535 | 197 |
| round_09 | 49,519 | 575 | 260 |
| round_10 | 37,611 | 621 | 184 |
| round_11 | 50,116 | 691 | 261 |
