"""Build the ``dicionario`` table for au_treasury_budget.

The coded columns hold readable slugs rather than opaque codes, so the dictionary
is not there to decode them -- it is there to *define* them. "Underlying cash
balance" and "headline cash balance" differ by one term, "fiscal balance" is not
the cash balance under another name, and net debt is not gross debt less
something obvious. A reader who cannot tell them apart will pick the wrong series,
and the definition is the part Treasury buries in footnotes.

Definitions are authored here; **coverage is derived**. The keys are taken from
the rows the extractors actually produced, so a value that appears in the data
with no definition stops the build. That is the failure mode this guards: a future
release adds a measure, the extractor maps it, and it reaches the table with no
explanation and nobody notices.
"""

from __future__ import annotations

DEFINITIONS: dict[tuple[str, str], str] = {
    # --- aggregate.release_type -------------------------------------------
    ("release_type", "budget"): (
        "Budget, delivered with Budget Paper No. 1 and containing forward "
        "estimates and medium-term projections"
    ),
    ("release_type", "final_budget_outcome"): (
        "Final Budget Outcome, published around September after the financial "
        "year closes and containing realised outcomes only"
    ),
    ("release_type", "myefo"): (
        "Mid-Year Economic and Fiscal Outlook, published in December"
    ),
    ("release_type", "pre_election_fiscal_outlook"): (
        "Pre-election Economic and Fiscal Outlook, published by the secretaries "
        "of Treasury and Finance after an election is called"
    ),
    # --- aggregate.sector --------------------------------------------------
    ("sector", "general_government"): (
        "General government sector: departments and agencies providing "
        "non-market services, funded mainly by taxation"
    ),
    ("sector", "public_non_financial_corporations"): (
        "Public non-financial corporations: government-owned trading "
        "enterprises that recover most of their costs from sales"
    ),
    ("sector", "non_financial_public_sector"): (
        "Non-financial public sector: the general government sector and public "
        "non-financial corporations consolidated"
    ),
    # --- aggregate.estimate_type -------------------------------------------
    (
        "estimate_type",
        "outcome",
    ): "Realised outcome for a completed financial year",
    ("estimate_type", "estimate"): (
        "Estimate or projection, marked (e) by Treasury in the source table"
    ),
    # --- aggregate.measure -------------------------------------------------
    ("measure", "receipts"): "Total cash receipts of the sector",
    ("measure", "payments"): "Total cash payments of the sector",
    ("measure", "taxation_receipts"): "Cash receipts from taxation",
    ("measure", "non_taxation_receipts"): (
        "Cash receipts other than taxation, such as dividends, interest and "
        "sales of goods and services"
    ),
    ("measure", "total_receipts"): (
        "Taxation and non-taxation cash receipts combined"
    ),
    ("measure", "underlying_cash_balance"): (
        "Receipts less payments, less net Future Fund earnings. The headline "
        "measure of the budget position"
    ),
    ("measure", "headline_cash_balance"): (
        "Receipts less payments, including net cash flows from investments in "
        "financial assets for policy purposes. Unlike the underlying cash "
        "balance it is not adjusted for those investments"
    ),
    ("measure", "net_future_fund_earnings"): (
        "Earnings of the Future Fund net of its expenses, excluded from the "
        "underlying cash balance"
    ),
    ("measure", "net_cash_flows_investments_policy_purposes"): (
        "Net cash flows from investments in financial assets made for policy "
        "rather than liquidity-management reasons, such as concessional loans"
    ),
    ("measure", "net_debt"): (
        "Selected gross financial liabilities less selected financial assets. "
        "Not the face value of debt on issue"
    ),
    ("measure", "net_interest_payments"): (
        "Interest paid on gross debt less interest received on financial assets"
    ),
    ("measure", "ags_on_issue_total"): (
        "Face value of Australian Government Securities on issue at the end of "
        "the year, on a gross basis"
    ),
    ("measure", "ags_on_issue_subject_to_treasurers_direction"): (
        "Face value of Australian Government Securities on issue that count "
        "against the Treasurer's Direction"
    ),
    ("measure", "interest_paid"): (
        "Interest paid on Australian Government Securities"
    ),
    ("measure", "revenue"): "Accrual revenue of the sector",
    ("measure", "expenses"): "Accrual expenses of the sector",
    ("measure", "taxation_revenue"): "Accrual revenue from taxation",
    ("measure", "non_taxation_revenue"): "Accrual revenue other than taxation",
    ("measure", "total_revenue"): (
        "Taxation and non-taxation accrual revenue combined"
    ),
    (
        "measure",
        "net_operating_balance",
    ): "Revenue less expenses, on an accrual basis",
    ("measure", "net_capital_investment"): (
        "Purchases of non-financial assets less sales less depreciation"
    ),
    ("measure", "fiscal_balance"): (
        "Net operating balance less net capital investment. The accrual "
        "counterpart of the underlying cash balance, and not equal to it"
    ),
    ("measure", "net_worth"): "Total assets less total liabilities",
    ("measure", "net_financial_worth"): (
        "Financial assets less total liabilities, excluding non-financial assets"
    ),
    ("measure", "cash_surplus"): (
        "Cash surplus of a sector outside general government, on the same basis "
        "as the underlying cash balance but without the Future Fund adjustment"
    ),
    # --- payment_growth.payment_program ------------------------------------
    ("payment_program", "ndis"): (
        "National Disability Insurance Scheme, the Australian Government's "
        "contribution to payments for participant supports"
    ),
    ("payment_program", "ndis_medium_term_only"): (
        "National Disability Insurance Scheme over the medium term alone, "
        "measured on a shorter window than the other programs and stated in the "
        "chart note of the release that publishes it"
    ),
    ("payment_program", "medical_benefits"): (
        "Medical benefits, principally the Medicare Benefits Schedule"
    ),
    ("payment_program", "aged_care"): "Aged care payments",
    (
        "payment_program",
        "hospitals",
    ): "Australian Government payments for public hospitals",
    ("payment_program", "defence"): "Defence funding",
    ("payment_program", "age_pension"): "Age Pension payments",
    ("payment_program", "child_care_subsidy"): "Child Care Subsidy payments",
    ("payment_program", "public_debt_interest"): (
        "Interest payments on Australian Government Securities. Labelled "
        "Interest before the 2026-27 Budget and PDI from it"
    ),
    # --- payment_growth.growth_basis ---------------------------------------
    ("growth_basis", "nominal"): (
        "Nominal growth, not adjusted for inflation. The chart note compares "
        "these programs against nominal GDP growth"
    ),
    # --- igr_projection.scenario -------------------------------------------
    ("scenario", "baseline"): (
        "Central projection on the report's baseline assumptions"
    ),
    ("scenario", "population_higher"): (
        "Higher population: long-run net overseas migration of 285,000 a year "
        "and a total fertility rate of 1.72, against 235,000 and 1.62 in the "
        "baseline"
    ),
    ("scenario", "population_lower"): (
        "Lower population: long-run net overseas migration of 185,000 a year "
        "and a total fertility rate of 1.52"
    ),
    ("scenario", "participation_higher"): (
        "Higher participation: a labour force participation rate of 65.8 per "
        "cent by 2062-63, against 63.8 per cent in the baseline"
    ),
    ("scenario", "participation_lower"): (
        "Lower participation: a labour force participation rate of 61.8 per "
        "cent by 2062-63"
    ),
    ("scenario", "productivity_higher"): (
        "Higher productivity: labour productivity growth of 1.5 per cent a "
        "year, against 1.2 per cent in the baseline"
    ),
    ("scenario", "productivity_lower"): (
        "Lower productivity: labour productivity growth of 0.9 per cent a year"
    ),
    # --- igr_projection.measure_category -----------------------------------
    ("measure_category", "demographic"): (
        "Population, life expectancy, fertility and migration"
    ),
    ("measure_category", "economic"): (
        "Output, income, productivity and labour force participation"
    ),
    (
        "measure_category",
        "fiscal",
    ): "Budget balances, debt and net financial worth",
    ("measure_category", "payments"): (
        "Projected spending on major Australian Government payment programs"
    ),
}

#: Which column of which table each coded column belongs to.
CODED_COLUMNS: tuple[tuple[str, str], ...] = (
    ("aggregate", "release_type"),
    ("aggregate", "sector"),
    ("aggregate", "measure"),
    ("aggregate", "estimate_type"),
    ("payment_growth", "payment_program"),
    ("payment_growth", "growth_basis"),
    ("igr_projection", "scenario"),
    ("igr_projection", "measure_category"),
)


class MissingDefinitionError(KeyError):
    """A coded value present in the data that nothing defines."""


def build(
    *,
    aggregate: list[dict],
    payment_growth: list[dict],
    igr_projection: list[dict],
) -> list[dict]:
    """Derive dictionary entries from the values the tables actually contain."""
    sources = {
        "aggregate": aggregate,
        "payment_growth": payment_growth,
        "igr_projection": igr_projection,
    }

    rows: list[dict] = []
    undefined: list[str] = []
    for table, column in CODED_COLUMNS:
        values = sorted(
            {row[column] for row in sources[table] if row.get(column)}
        )
        for value in values:
            definition = DEFINITIONS.get((column, value))
            if definition is None:
                undefined.append(f"{table}.{column} = {value!r}")
                continue
            rows.append(
                {
                    "id_tabela": table,
                    "nome_coluna": column,
                    "chave": value,
                    "cobertura_temporal": None,
                    "valor": definition,
                }
            )

    if undefined:
        raise MissingDefinitionError(
            "Coded values present in the data with no definition:\n  "
            + "\n  ".join(undefined)
            + "\nAdd them to dictionary.DEFINITIONS. A value that reaches the "
            "published table undefined is a measure nobody can interpret."
        )

    unused = sorted(
        f"{column} = {value!r}"
        for (column, value) in DEFINITIONS
        if not any(
            row["nome_coluna"] == column and row["chave"] == value
            for row in rows
        )
    )
    if unused:
        print(f"  note: {len(unused)} definitions match no value in the data")
        for entry in unused:
            print(f"    {entry}")
    print(f"  {len(rows)} dictionary entries")
    return rows
