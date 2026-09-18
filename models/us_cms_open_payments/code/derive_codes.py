"""Recover the two code lists CMS uses in the summary reports but never publishes.

The summary reports group by `Nature_Of_Payment_Type_Code` (1-19) but never
give the labels; the detail files carry the labels but no code. The two
describe the same payments, so the mapping can be recovered by aggregating
the detail table exactly as CMS aggregated the summary and joining on a key
distinctive enough that a collision is implausible: program year, reporting
entity, transaction count and the total amount to the cent.

Every code resolves to exactly one label with hundreds to thousands of
independent matching groups, and no code has a runner-up worth reporting.

    uv run --with duckdb python derive_codes.py
"""

import constants as c

QUERY = """
WITH detail AS (
    SELECT year, reporting_entity_id, payment_nature,
           count(*) AS n, round(sum(CAST(payment_amount_total AS DOUBLE)), 2) AS amount
    FROM read_parquet('{output}/general/*/data.parquet')
    GROUP BY 1, 2, 3
),
summary AS (
    SELECT year, reporting_entity_id, payment_nature_code,
           CAST(transaction_count AS BIGINT) AS n,
           round(CAST(amount_total AS DOUBLE), 2) AS amount
    FROM read_parquet('{output}/summary_by_entity_nature/*/data.parquet')
)
SELECT s.payment_nature_code AS code, d.payment_nature AS label, count(*) AS matches
FROM summary s
JOIN detail d
  ON s.year = d.year
 AND s.reporting_entity_id = d.reporting_entity_id
 AND s.n = d.n
 AND s.amount = d.amount
GROUP BY 1, 2
ORDER BY CAST(code AS INT), matches DESC
"""


def _resolve(query: str) -> dict[str, tuple[str, int, int]]:
    """code -> (label, matches for that label, matches for the runner-up)."""
    import duckdb

    con = duckdb.connect(
        config={"memory_limit": "8GB", "preserve_insertion_order": "false"}
    )
    con.execute(f"SET temp_directory='{c.DATA_ROOT / 'duckdb_tmp'}'")
    rows = con.execute(query.format(output=c.OUTPUT_DIR)).fetchall()
    con.close()

    candidates: dict[str, list[tuple[str, int]]] = {}
    for code, label, matches in rows:
        candidates.setdefault(code, []).append((label, matches))
    resolved = {}
    for code, options in candidates.items():
        options.sort(key=lambda pair: -pair[1])
        runner_up = options[1][1] if len(options) > 1 else 0
        resolved[code] = (options[0][0], options[0][1], runner_up)
    return resolved


def derive() -> dict[str, tuple[str, int, int]]:
    return _resolve(QUERY)


# recipient_type is text in three of the four summary reports and a code in
# summary_by_entity_recipient_nature. Rolling that report up over reporting
# entities reproduces summary_by_recipient_nature exactly, so joining on year,
# recipient, nature, transaction count and amount to the cent recovers the
# labels. A join on recipient alone is not enough: recipient_id is only unique
# within a recipient type, so a physician profile id and a teaching hospital
# CCN can collide numerically.
RECIPIENT_QUERY = """
WITH rolled AS (
    SELECT year, recipient_id, recipient_type AS code, payment_nature_code,
           sum(CAST(transaction_count AS BIGINT)) AS n,
           round(sum(CAST(amount_total AS DOUBLE)), 2) AS amount
    FROM read_parquet('{output}/summary_by_entity_recipient_nature/*/data.parquet')
    GROUP BY 1, 2, 3, 4
),
named AS (
    SELECT year, recipient_id, recipient_type AS label, payment_nature_code,
           CAST(transaction_count AS BIGINT) AS n,
           round(CAST(amount_total AS DOUBLE), 2) AS amount
    FROM read_parquet('{output}/summary_by_recipient_nature/*/data.parquet')
)
SELECT r.code AS code, n.label AS label, count(*) AS matches
FROM rolled r
JOIN named n
  ON r.year = n.year
 AND r.recipient_id = n.recipient_id
 AND r.payment_nature_code = n.payment_nature_code
 AND r.n = n.n
 AND r.amount = n.amount
GROUP BY 1, 2
ORDER BY CAST(code AS INT), matches DESC
"""


def derive_recipient_types() -> dict[str, tuple[str, int, int]]:
    return _resolve(RECIPIENT_QUERY)


if __name__ == "__main__":
    print("nature of payment")
    for code, (label, matches, runner_up) in sorted(
        derive().items(), key=lambda kv: int(kv[0])
    ):
        margin = "" if runner_up == 0 else f"  (runner-up {runner_up:,})"
        print(f"  {code:>3s}  {matches:>9,} matches  {label}{margin}")
    print("\nrecipient type")
    for code, (label, matches, runner_up) in sorted(
        derive_recipient_types().items(), key=lambda kv: int(kv[0])
    ):
        margin = "" if runner_up == 0 else f"  (runner-up {runner_up:,})"
        print(f"  {code:>3s}  {matches:>9,} matches  {label}{margin}")
