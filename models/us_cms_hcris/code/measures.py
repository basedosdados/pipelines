"""The worksheet-line-column mapping behind ``hospital_financial``.

HCRIS is not a rectangular file. It is a long triple of (report, worksheet,
line, column, value), and turning it into named measures requires a mapping
from cell address to meaning. That mapping is the whole job, and it is **not
invented here**. Every entry below is taken from a published, maintained
mapping:

* ``asacarny`` — Adam Sacarny, `hospital-cost-reports
  <https://github.com/asacarny/hospital-cost-reports>`_, ``lookup.xlsx``,
  read at commit fetched 2026-09-09. 47 measures across both form versions.
* ``imccart`` — Ian McCarthy, `HCRIS <https://github.com/imccart/HCRIS>`_,
  ``data-code/H1_HCRISv1996.R`` and ``H2_HCRISv2010.R``. 39 measures.
* ``cms`` — CMS's own ``HOSP2010_README.txt`` (in
  ``hospital2010-documentation.zip``), which documents the bed count at
  S-3 Part I line 14 column 2 and the ICU line ranges directly.

Where the two research mappings overlap they agree cell for cell, which is the
cross-check that let both be merged rather than one being picked. ``source``
records which of them an entry came from, and it is published in the column's
``observations`` so a user can trace any measure back to a maintained mapping
rather than to this repository's judgement.

Two structural facts drive the shape of this file:

* **The two form versions put the same measure in different cells.** CMS
  Form 2552-96 applies to fiscal years up to roughly 2011 and 2552-10 from
  2010 on, and the forms are renumbered throughout. Each measure therefore
  carries a location per form, and a measure introduced by one form has no
  location in the other — ``tot_uncomp_care_charges`` does not exist before
  2552-10, ``uncompensated_care_charges`` not after it.
* **Column numbers are 4 characters wide in 2552-96 and 5 in 2552-10.** CMS
  documents this in ``HCRIS_DataDictionary.csv`` ("xxyyyy" for HOSP10, "xxyy"
  for all others). The literals below are written exactly as the source files
  store them, so they can be pasted into a query against ``report_value``
  without adjustment.

Some measures sum a run of lines rather than reading one: CMS subscripts lines
for repeated cost centres, so intensive-care beds are lines 00800 through 00899
rather than line 00800 alone. ``line_end`` carries that, and a measure may hold
several runs — other special care beds occupy six disjoint ranges under 2552-96.
"""

from dataclasses import dataclass

FORM_1996 = "2552-96"
FORM_2010 = "2552-10"


@dataclass(frozen=True)
class Cell:
    """One cell address, or a run of lines summed into one value.

    Args:
        worksheet: HCRIS worksheet code, e.g. ``"G300000"``.
        line: Line number, as stored — always 5 characters.
        column: Column number, as stored — 4 characters under 2552-96, 5 under
            2552-10.
        line_end: Last line of an inclusive run, when the measure sums a
            subscripted range. ``None`` reads ``line`` alone.
    """

    worksheet: str
    line: str
    column: str
    line_end: str | None = None


@dataclass(frozen=True)
class Measure:
    """One named measure of ``hospital_financial``.

    Args:
        name: Column name in the published table.
        kind: ``"numeric"`` or ``"alpha"`` — which value of the cell is read.
        unit: Data Basis measurement unit. Empty for a ratio or a label.
        by_form: Cells to read, per form version. A form absent from the
            mapping means the measure is not collected on that form.
        source: Which published mapping the addresses come from.
        absolute: Read as an absolute value. Discounts and adjustments are
            meant to be entered positive but are signed negative by a minority
            of filers; both published mappings take the absolute value.
        en: English description.
        pt: Portuguese description.
        es: Spanish description.
        note: Extra caveat, appended to ``observations``.
    """

    name: str
    kind: str
    unit: str
    by_form: dict[str, list[Cell]]
    source: str
    en: str
    pt: str
    es: str
    absolute: bool = False
    note: str = ""


def _c(
    worksheet: str, line: str, column: str, line_end: str | None = None
) -> list[Cell]:
    """Shorthand for a single-cell location."""
    return [Cell(worksheet, line, column, line_end)]


def _s10(worksheet: str, line: str, column: str) -> list[Cell]:
    """Locations of one Worksheet S-10 cell under CMS Form 2552-10.

    CMS split Worksheet S-10 in two for cost reporting periods beginning on or
    after 1 October 2022 (Pub. 15-2 section 4014): what was ``S100000`` became
    ``S100001`` (Part I), and a new ``S100002`` (Part II) collects the same
    items for inpatient and outpatient services billable under the hospital CCN
    alone — "a subset of the data reported on Part I".

    Neither published mapping covers the split, because both predate it. Read
    against ``S100000`` alone, every uncompensated care measure silently goes
    null from fiscal year 2023 while the rest of the table carries on, which is
    what this dataset's validation pass caught. ``S100000`` and ``S100001`` are
    one continuous whole-complex series: no report in the 538 million published
    values carries both, so summing the two is the same as coalescing them.

    ``S100002`` is deliberately excluded. It is a narrower measure, not more of
    the same one, and adding it here would inflate the whole-complex series for
    the hospitals that file it. It remains available in ``report_value`` for
    anyone who wants the hospital-CCN-only subset.

    Args:
        worksheet: Always ``"S100000"``; named for symmetry with :func:`_c`.
        line: Line number.
        column: Column number.

    Returns:
        The cell on both the pre-2023 and the post-2022 worksheet code.
    """
    return [Cell(worksheet, line, column), Cell("S100001", line, column)]


# --------------------------------------------------------------------------
# Worksheet S-2 Part I — identity, from the ALPHA file
# --------------------------------------------------------------------------

IDENTITY = [
    Measure(
        "hospital_name",
        "alpha",
        "",
        {
            FORM_2010: _c("S200001", "00300", "00100"),
            FORM_1996: _c("S200000", "00200", "0100"),
        },
        "asacarny, imccart",
        "Hospital name as reported on Worksheet S-2",
        "Nome do hospital conforme declarado na planilha S-2",
        "Nombre del hospital según se declara en la hoja S-2",
    ),
    Measure(
        "street_address",
        "alpha",
        "",
        {
            FORM_2010: _c("S200001", "00100", "00100"),
            FORM_1996: _c("S200000", "00100", "0100"),
        },
        "imccart",
        "Street address of the hospital",
        "Logradouro do hospital",
        "Dirección del hospital",
    ),
    Measure(
        "city",
        "alpha",
        "",
        {
            FORM_2010: _c("S200001", "00200", "00100"),
            FORM_1996: _c("S200000", "00101", "0100"),
        },
        "imccart",
        "City of the hospital address",
        "Cidade do endereço do hospital",
        "Ciudad de la dirección del hospital",
    ),
    Measure(
        "reported_state_abbreviation",
        "alpha",
        "",
        {
            FORM_2010: _c("S200001", "00200", "00200"),
            FORM_1996: _c("S200000", "00101", "0200"),
        },
        "imccart",
        "State abbreviation as typed on the cost report",
        "Sigla do estado conforme digitada no relatório de custos",
        "Sigla del estado tal como se escribió en el informe de costos",
        note=(
            "Free text entered by the hospital. The state used for the "
            "geographic observation level is report.state_id, resolved from "
            "the SSA state code embedded in the CCN, which is complete and "
            "consistent; this column is kept as filed"
        ),
    ),
    Measure(
        "zip_code",
        "alpha",
        "",
        {
            FORM_2010: _c("S200001", "00200", "00300"),
            FORM_1996: _c("S200000", "00101", "0300"),
        },
        "imccart",
        "ZIP code of the hospital address",
        "Código postal do endereço do hospital",
        "Código postal de la dirección del hospital",
    ),
    Measure(
        "county_name",
        "alpha",
        "",
        {
            FORM_2010: _c("S200001", "00200", "00400"),
            FORM_1996: _c("S200000", "00101", "0400"),
        },
        "imccart",
        "County of the hospital address as typed on the cost report",
        "Condado do endereço do hospital conforme digitado no relatório de custos",
        "Condado de la dirección del hospital tal como se escribió en el informe",
        note=(
            "Free text entered by the hospital, not a FIPS code, and not "
            "linked to the county directory: the spellings do not resolve "
            "reliably"
        ),
    ),
    Measure(
        "chain_organization_name",
        "alpha",
        "",
        {
            FORM_2010: _c("S200001", "14100", "00100"),
            FORM_1996: _c("S200000", "04001", "0100"),
        },
        "asacarny",
        "Name of the chain organization the hospital belongs to",
        "Nome da organização da rede à qual o hospital pertence",
        "Nombre de la organización de la cadena a la que pertenece el hospital",
    ),
]


# --------------------------------------------------------------------------
# Worksheet G-3 — statement of revenues and expenses
# --------------------------------------------------------------------------

INCOME_STATEMENT = [
    Measure(
        "total_patient_charges",
        "numeric",
        "usd",
        {
            FORM_2010: _c("G300000", "00100", "00100"),
            FORM_1996: _c("G300000", "00100", "0100"),
        },
        "imccart",
        "Total gross patient charges before allowances and discounts",
        "Total bruto de cobranças de pacientes antes de deduções e descontos",
        "Total bruto de cargos a pacientes antes de deducciones y descuentos",
    ),
    Measure(
        "contractual_allowances_discounts",
        "numeric",
        "usd",
        {
            FORM_2010: _c("G300000", "00200", "00100"),
            FORM_1996: _c("G300000", "00200", "0100"),
        },
        "imccart",
        "Contractual allowances and discounts on patient accounts",
        "Deduções contratuais e descontos sobre contas de pacientes",
        "Deducciones contractuales y descuentos sobre cuentas de pacientes",
        absolute=True,
    ),
    Measure(
        "net_patient_revenue",
        "numeric",
        "usd",
        {
            FORM_2010: _c("G300000", "00300", "00100"),
            FORM_1996: _c("G300000", "00300", "0100"),
        },
        "asacarny, imccart",
        "Net patient revenue, total charges less allowances and discounts",
        "Receita líquida de pacientes, cobranças totais menos deduções e descontos",
        "Ingreso neto de pacientes, cargos totales menos deducciones y descuentos",
    ),
    Measure(
        "total_operating_expense",
        "numeric",
        "usd",
        {
            FORM_2010: _c("G300000", "00400", "00100"),
            FORM_1996: _c("G300000", "00400", "0100"),
        },
        "asacarny, imccart",
        "Total operating expenses",
        "Despesas operacionais totais",
        "Gastos operativos totales",
    ),
    Measure(
        "donations",
        "numeric",
        "usd",
        {
            FORM_2010: _c("G300000", "00600", "00100"),
            FORM_1996: _c("G300000", "00600", "0100"),
        },
        "asacarny",
        "Unrestricted contributions and donations",
        "Contribuições e doações sem restrição",
        "Contribuciones y donaciones sin restricción",
    ),
    Measure(
        "investment_income",
        "numeric",
        "usd",
        {
            FORM_2010: _c("G300000", "00700", "00100"),
            FORM_1996: _c("G300000", "00700", "0100"),
        },
        "asacarny",
        "Income from investments",
        "Receita de investimentos",
        "Ingresos por inversiones",
    ),
    Measure(
        "other_income",
        "numeric",
        "usd",
        {
            FORM_2010: _c("G300000", "02500", "00100"),
            FORM_1996: _c("G300000", "02500", "0100"),
        },
        "asacarny",
        "Total other income",
        "Outras receitas totais",
        "Otros ingresos totales",
    ),
    Measure(
        "total_other_expense",
        "numeric",
        "usd",
        {
            FORM_2010: _c("G300000", "02800", "00100"),
            FORM_1996: _c("G300000", "03000", "0100"),
        },
        "asacarny",
        "Total other expenses",
        "Outras despesas totais",
        "Otros gastos totales",
    ),
]


# --------------------------------------------------------------------------
# Worksheet G-2 — patient revenues, inpatient and outpatient
# --------------------------------------------------------------------------

PATIENT_REVENUE = [
    Measure(
        "inpatient_hospital_revenue",
        "numeric",
        "usd",
        {
            FORM_2010: _c("G200000", "00100", "00100"),
            FORM_1996: _c("G200000", "00100", "0100"),
        },
        "asacarny, imccart",
        "Inpatient revenue of the hospital cost centre",
        "Receita de internação do centro de custo hospitalar",
        "Ingreso de hospitalización del centro de costo hospitalario",
    ),
    Measure(
        "inpatient_general_routine_revenue",
        "numeric",
        "usd",
        {
            FORM_2010: _c("G200000", "01000", "00100"),
            FORM_1996: _c("G200000", "00900", "0100"),
        },
        "asacarny",
        "Total inpatient general routine care revenue",
        "Receita total de internação em cuidados gerais de rotina",
        "Ingreso total de hospitalización en cuidados generales de rutina",
    ),
    Measure(
        "inpatient_intensive_care_revenue",
        "numeric",
        "usd",
        {
            FORM_2010: _c("G200000", "01600", "00100"),
            FORM_1996: _c("G200000", "01500", "0100"),
        },
        "asacarny, imccart",
        "Total inpatient intensive care type revenue",
        "Receita total de internação em cuidados intensivos",
        "Ingreso total de hospitalización en cuidados intensivos",
    ),
    Measure(
        "inpatient_routine_care_revenue",
        "numeric",
        "usd",
        {
            FORM_2010: _c("G200000", "01700", "00100"),
            FORM_1996: _c("G200000", "01600", "0100"),
        },
        "asacarny",
        "Total inpatient routine care revenue, general plus intensive care",
        "Receita total de internação em cuidados de rotina, gerais mais intensivos",
        "Ingreso total de hospitalización en cuidados de rutina, generales más intensivos",
    ),
    Measure(
        "inpatient_ancillary_revenue",
        "numeric",
        "usd",
        {
            FORM_2010: _c("G200000", "01800", "00100"),
            FORM_1996: _c("G200000", "01700", "0100"),
        },
        "asacarny, imccart",
        "Inpatient ancillary services revenue",
        "Receita de serviços auxiliares de internação",
        "Ingreso de servicios auxiliares de hospitalización",
    ),
    Measure(
        "inpatient_outpatient_service_revenue",
        "numeric",
        "usd",
        {
            FORM_2010: _c("G200000", "01900", "00100"),
            FORM_1996: _c("G200000", "01800", "0100"),
        },
        "asacarny",
        "Inpatient revenue from outpatient service cost centres",
        "Receita de internação em centros de custo de serviços ambulatoriais",
        "Ingreso de hospitalización en centros de costo de servicios ambulatorios",
    ),
    Measure(
        "inpatient_total_revenue",
        "numeric",
        "usd",
        {
            FORM_2010: _c("G200000", "02800", "00100"),
            FORM_1996: _c("G200000", "02500", "0100"),
        },
        "asacarny",
        "Total inpatient patient revenue",
        "Receita total de pacientes internados",
        "Ingreso total de pacientes hospitalizados",
    ),
    Measure(
        "outpatient_ancillary_revenue",
        "numeric",
        "usd",
        {
            FORM_2010: _c("G200000", "01800", "00200"),
            FORM_1996: _c("G200000", "01700", "0200"),
        },
        "asacarny",
        "Outpatient ancillary services revenue",
        "Receita de serviços auxiliares ambulatoriais",
        "Ingreso de servicios auxiliares ambulatorios",
    ),
    Measure(
        "outpatient_outpatient_service_revenue",
        "numeric",
        "usd",
        {
            FORM_2010: _c("G200000", "01900", "00200"),
            FORM_1996: _c("G200000", "01800", "0200"),
        },
        "asacarny",
        "Outpatient revenue from outpatient service cost centres",
        "Receita ambulatorial em centros de custo de serviços ambulatoriais",
        "Ingreso ambulatorio en centros de costo de servicios ambulatorios",
    ),
    Measure(
        "outpatient_total_revenue",
        "numeric",
        "usd",
        {
            FORM_2010: _c("G200000", "02800", "00200"),
            FORM_1996: _c("G200000", "02500", "0200"),
        },
        "asacarny",
        "Total outpatient patient revenue",
        "Receita total de pacientes ambulatoriais",
        "Ingreso total de pacientes ambulatorios",
    ),
    Measure(
        "total_patient_revenue",
        "numeric",
        "usd",
        {
            FORM_2010: _c("G200000", "02800", "00300"),
            FORM_1996: _c("G200000", "02500", "0300"),
        },
        "asacarny",
        "Total patient revenue, inpatient plus outpatient",
        "Receita total de pacientes, internados mais ambulatoriais",
        "Ingreso total de pacientes, hospitalizados más ambulatorios",
    ),
]


# --------------------------------------------------------------------------
# Worksheet S-10 — uncompensated care, charity care and bad debt
# --------------------------------------------------------------------------
# Reporting of uncompensated care changed substantially with 2552-10, and the
# two forms are not reconcilable line for line. The measures below are kept as
# each form defines them rather than harmonised into one series.

UNCOMPENSATED_CARE = [
    Measure(
        "cost_to_charge_ratio",
        "numeric",
        "",
        {
            FORM_2010: _s10("S100000", "00100", "00100"),
            FORM_1996: _c("S100000", "02400", "0100"),
        },
        "asacarny, imccart",
        "Hospital-wide cost-to-charge ratio",
        "Razão custo-cobrança do hospital",
        "Razón costo-cargo del hospital",
        note="A ratio, so it has no measurement unit",
    ),
    Measure(
        "uncompensated_care_charges",
        "numeric",
        "usd",
        {FORM_1996: _c("S100000", "03000", "0100")},
        "asacarny, imccart",
        "Charges for uncompensated care",
        "Cobranças de atendimento não remunerado",
        "Cargos por atención no remunerada",
        note="Collected on CMS Form 2552-96 only",
    ),
    Measure(
        "uncompensated_care_cost",
        "numeric",
        "usd",
        {FORM_1996: _c("S100000", "03100", "0100")},
        "asacarny",
        "Cost of uncompensated care",
        "Custo do atendimento não remunerado",
        "Costo de la atención no remunerada",
        note=(
            "Collected on CMS Form 2552-96 only. asacarny maps this cell but "
            "leaves it disabled by default; it is published here as filed and "
            "should be checked before use"
        ),
    ),
    Measure(
        "total_initial_charity_care_charges",
        "numeric",
        "usd",
        {FORM_2010: _s10("S100000", "02000", "00300")},
        "asacarny, imccart",
        "Total initial obligation of patients approved for charity care",
        "Obrigação inicial total dos pacientes aprovados para atendimento filantrópico",
        "Obligación inicial total de los pacientes aprobados para atención de caridad",
        note="Collected on CMS Form 2552-10 only",
    ),
    Measure(
        "charity_care_partial_payments",
        "numeric",
        "usd",
        {FORM_2010: _s10("S100000", "02200", "00300")},
        "asacarny, imccart",
        "Partial payments by patients approved for charity care",
        "Pagamentos parciais de pacientes aprovados para atendimento filantrópico",
        "Pagos parciales de pacientes aprobados para atención de caridad",
        note="Collected on CMS Form 2552-10 only",
    ),
    Measure(
        "cost_of_initial_charity_care",
        "numeric",
        "usd",
        {FORM_2010: _s10("S100000", "02100", "00300")},
        "asacarny",
        "Cost of patients approved for charity care and uninsured discounts",
        "Custo dos pacientes aprovados para atendimento filantrópico e descontos a não segurados",
        "Costo de los pacientes aprobados para atención de caridad y descuentos a no asegurados",
        note=(
            "Collected on CMS Form 2552-10 only. asacarny maps this cell but "
            "leaves it disabled by default; it is published here as filed and "
            "should be checked before use"
        ),
    ),
    Measure(
        "cost_of_charity_care",
        "numeric",
        "usd",
        {FORM_2010: _s10("S100000", "02300", "00300")},
        "asacarny",
        "Cost of charity care",
        "Custo do atendimento filantrópico",
        "Costo de la atención de caridad",
        note="Collected on CMS Form 2552-10 only",
    ),
    Measure(
        "total_bad_debt_expense",
        "numeric",
        "usd",
        {FORM_2010: _s10("S100000", "02600", "00100")},
        "asacarny",
        "Total bad debt expense",
        "Despesa total com créditos incobráveis",
        "Gasto total por deudas incobrables",
        note=(
            "Collected on CMS Form 2552-10 only. asacarny maps this cell but "
            "leaves it disabled by default; it is published here as filed and "
            "should be checked before use"
        ),
    ),
    Measure(
        "medicare_reimbursable_bad_debt",
        "numeric",
        "usd",
        {FORM_2010: _s10("S100000", "02700", "00100")},
        "asacarny",
        "Medicare reimbursable bad debt",
        "Créditos incobráveis reembolsáveis pelo Medicare",
        "Deudas incobrables reembolsables por Medicare",
        note=(
            "Collected on CMS Form 2552-10 only. asacarny maps this cell but "
            "leaves it disabled by default; it is published here as filed and "
            "should be checked before use"
        ),
    ),
    Measure(
        "non_medicare_bad_debt_expense",
        "numeric",
        "usd",
        {FORM_2010: _s10("S100000", "02800", "00100")},
        "asacarny, imccart",
        "Non-Medicare bad debt expense",
        "Despesa com créditos incobráveis não Medicare",
        "Gasto por deudas incobrables no Medicare",
        note="Collected on CMS Form 2552-10 only",
    ),
    Measure(
        "non_reimbursable_bad_debt_cost",
        "numeric",
        "usd",
        {FORM_2010: _s10("S100000", "02900", "00100")},
        "asacarny",
        "Cost of non-Medicare and non-reimbursable Medicare bad debt expense",
        "Custo de créditos incobráveis não Medicare e Medicare não reembolsáveis",
        "Costo de deudas incobrables no Medicare y Medicare no reembolsables",
        note=(
            "Collected on CMS Form 2552-10 only. asacarny maps this cell but "
            "leaves it disabled by default; it is published here as filed and "
            "should be checked before use"
        ),
    ),
    Measure(
        "cost_of_uncompensated_care",
        "numeric",
        "usd",
        {FORM_2010: _s10("S100000", "03000", "00100")},
        "asacarny",
        "Cost of uncompensated care",
        "Custo do atendimento não remunerado",
        "Costo de la atención no remunerada",
        note="Collected on CMS Form 2552-10 only",
    ),
]


# --------------------------------------------------------------------------
# Worksheet S-3 Part I — beds, bed days, discharges
# --------------------------------------------------------------------------
# Line 1 is the adults-and-pediatrics routine care line. Intensive care beds
# occupy subscripted line runs, which CMS documents in HOSP2010_README.txt
# section 5.2: "if you want ICU beds, you should extract Worksheet Code
# S300001, Line Numbers 00800 through 00899, Column 00200".

UTILIZATION = [
    Measure(
        "beds_adult_pediatric",
        "numeric",
        "bed",
        {
            FORM_2010: _c("S300001", "00100", "00200"),
            FORM_1996: _c("S300001", "00100", "0100"),
        },
        "asacarny",
        "Beds in the adults and pediatrics routine care unit",
        "Leitos na unidade de cuidados de rotina de adultos e pediatria",
        "Camas en la unidad de cuidados de rutina de adultos y pediatría",
    ),
    Measure(
        "available_bed_days_adult_pediatric",
        "numeric",
        "bed_day",
        {
            FORM_2010: _c("S300001", "00100", "00300"),
            FORM_1996: _c("S300001", "00100", "0200"),
        },
        "asacarny",
        "Bed days available in the reporting period, adults and pediatrics",
        "Leitos-dia disponíveis no período do relatório, adultos e pediatria",
        "Camas-día disponibles en el período del informe, adultos y pediatría",
    ),
    Measure(
        "inpatient_bed_days_adult_pediatric",
        "numeric",
        "bed_day",
        {
            FORM_2010: _c("S300001", "00100", "00800"),
            FORM_1996: _c("S300001", "00100", "0600"),
        },
        "asacarny",
        "Inpatient bed days utilized, adults and pediatrics",
        "Leitos-dia de internação utilizados, adultos e pediatria",
        "Camas-día de hospitalización utilizadas, adultos y pediatría",
    ),
    Measure(
        "discharges_adult_pediatric",
        "numeric",
        "discharge",
        {
            FORM_2010: _c("S300001", "00100", "01500"),
            FORM_1996: _c("S300001", "00100", "1500"),
        },
        "asacarny, imccart",
        "Total inpatient discharges, adults and pediatrics",
        "Total de altas de internação, adultos e pediatria",
        "Total de altas de hospitalización, adultos y pediatría",
    ),
    Measure(
        "discharges_medicare_adult_pediatric",
        "numeric",
        "discharge",
        {
            FORM_2010: _c("S300001", "00100", "01300"),
            FORM_1996: _c("S300001", "00100", "1300"),
        },
        "imccart",
        "Medicare (Title XVIII) inpatient discharges, adults and pediatrics",
        "Altas de internação do Medicare (Título XVIII), adultos e pediatria",
        "Altas de hospitalización de Medicare (Título XVIII), adultos y pediatría",
    ),
    Measure(
        "discharges_medicaid_adult_pediatric",
        "numeric",
        "discharge",
        {
            FORM_2010: _c("S300001", "00100", "01400"),
            FORM_1996: _c("S300001", "00100", "1400"),
        },
        "imccart",
        "Medicaid (Title XIX) inpatient discharges, adults and pediatrics",
        "Altas de internação do Medicaid (Título XIX), adultos e pediatria",
        "Altas de hospitalización de Medicaid (Título XIX), adultos y pediatría",
    ),
    Measure(
        "beds_total_adult_pediatric_swing",
        "numeric",
        "bed",
        {
            FORM_2010: _c("S300001", "00700", "00200"),
            FORM_1996: _c("S300001", "00500", "0100"),
        },
        "asacarny",
        "Total adults and pediatrics beds including swing beds",
        "Total de leitos de adultos e pediatria, incluindo leitos flexíveis",
        "Total de camas de adultos y pediatría, incluidas las camas flexibles",
    ),
    Measure(
        "beds_intensive_care_unit",
        "numeric",
        "bed",
        {
            FORM_2010: _c("S300001", "00800", "00200", "00899"),
            FORM_1996: _c("S300001", "02600", "0100", "02619"),
        },
        "asacarny, cms",
        "Intensive care unit beds, summed over the subscripted lines",
        "Leitos de unidade de terapia intensiva, somados sobre as linhas subscritas",
        "Camas de unidad de cuidados intensivos, sumadas sobre las líneas subscritas",
    ),
    Measure(
        "beds_coronary_care_unit",
        "numeric",
        "bed",
        {
            FORM_2010: _c("S300001", "00900", "00200", "00999"),
            FORM_1996: _c("S300001", "02700", "0100", "02719"),
        },
        "asacarny",
        "Coronary care unit beds, summed over the subscripted lines",
        "Leitos de unidade coronariana, somados sobre as linhas subscritas",
        "Camas de unidad coronaria, sumadas sobre las líneas subscritas",
    ),
    Measure(
        "beds_burn_intensive_care_unit",
        "numeric",
        "bed",
        {
            FORM_2010: _c("S300001", "01000", "00200", "01099"),
            FORM_1996: _c("S300001", "02800", "0100", "02819"),
        },
        "asacarny",
        "Burn intensive care unit beds, summed over the subscripted lines",
        "Leitos de UTI de queimados, somados sobre as linhas subscritas",
        "Camas de UCI de quemados, sumadas sobre las líneas subscritas",
    ),
    Measure(
        "beds_surgical_intensive_care_unit",
        "numeric",
        "bed",
        {
            FORM_2010: _c("S300001", "01100", "00200", "01199"),
            FORM_1996: _c("S300001", "02900", "0100", "02919"),
        },
        "asacarny",
        "Surgical intensive care unit beds, summed over the subscripted lines",
        "Leitos de UTI cirúrgica, somados sobre as linhas subscritas",
        "Camas de UCI quirúrgica, sumadas sobre las líneas subscritas",
    ),
    Measure(
        "beds_other_special_care",
        "numeric",
        "bed",
        {
            FORM_2010: _c("S300001", "01200", "00200", "01299"),
            FORM_1996: [
                Cell("S300001", "02040", "0100", "02059"),
                Cell("S300001", "02060", "0100", "02079"),
                Cell("S300001", "02080", "0100", "02099"),
                Cell("S300001", "02120", "0100", "02139"),
                Cell("S300001", "02140", "0100", "02159"),
                Cell("S300001", "02180", "0100", "02199"),
            ],
        },
        "asacarny",
        "Other special care unit beds, summed over the subscripted lines",
        "Leitos de outras unidades de cuidados especiais, somados sobre as linhas subscritas",
        "Camas de otras unidades de cuidados especiales, sumadas sobre las líneas subscritas",
        note=(
            "Under CMS Form 2552-96 these cost centres are cost-center coded "
            "and occupy six disjoint line runs, all six of which are summed"
        ),
    ),
    Measure(
        "beds_total",
        "numeric",
        "bed",
        {
            FORM_2010: _c("S300001", "01400", "00200"),
            FORM_1996: _c("S300001", "01200", "0100"),
        },
        "asacarny, imccart, cms",
        "Total hospital beds, including swing and special care beds",
        "Total de leitos hospitalares, incluindo leitos flexíveis e de cuidados especiais",
        "Total de camas hospitalarias, incluidas las flexibles y de cuidados especiales",
        note=(
            "CMS names this cell explicitly in HOSP2010_README.txt as the "
            "number of hospital beds: Worksheet S-3 Part I, line 14, column 2"
        ),
    ),
    Measure(
        "beds_grand_total",
        "numeric",
        "bed",
        {
            FORM_2010: _c("S300001", "02700", "00200"),
            FORM_1996: _c("S300001", "02500", "0100"),
        },
        "asacarny",
        "Grand total beds, including subprovider, skilled nursing and hospice units",
        "Total geral de leitos, incluindo subprovedores, enfermagem especializada e hospice",
        "Total general de camas, incluidos subproveedores, enfermería especializada y hospicio",
    ),
]


# --------------------------------------------------------------------------
# Worksheet G — balance sheet
# --------------------------------------------------------------------------

BALANCE_SHEET = [
    Measure(
        "cash",
        "numeric",
        "usd",
        {
            FORM_2010: _c("G000000", "00100", "00100"),
            FORM_1996: _c("G000000", "00100", "0100"),
        },
        "imccart",
        "Cash on hand and in banks",
        "Caixa e bancos",
        "Efectivo en caja y bancos",
    ),
    Measure(
        "total_current_assets",
        "numeric",
        "usd",
        {
            FORM_2010: _c("G000000", "01100", "00100"),
            FORM_1996: _c("G000000", "01100", "0100"),
        },
        "imccart",
        "Total current assets",
        "Ativo circulante total",
        "Activo corriente total",
    ),
    Measure(
        "total_fixed_assets",
        "numeric",
        "usd",
        {
            FORM_2010: _c("G000000", "03000", "00100"),
            FORM_1996: _c("G000000", "02100", "0100"),
        },
        "imccart",
        "Total fixed assets",
        "Ativo imobilizado total",
        "Activo fijo total",
    ),
    Measure(
        "total_current_liabilities",
        "numeric",
        "usd",
        {
            FORM_2010: _c("G000000", "04500", "00100"),
            FORM_1996: _c("G000000", "03600", "0100"),
        },
        "imccart",
        "Total current liabilities",
        "Passivo circulante total",
        "Pasivo corriente total",
    ),
    Measure(
        "accumulated_depreciation",
        "numeric",
        "usd",
        {
            FORM_2010: [
                Cell("G000000", "01400", "00100"),
                Cell("G000000", "01600", "00100"),
                Cell("G000000", "01800", "00100"),
                Cell("G000000", "02000", "00100"),
                Cell("G000000", "02200", "00100"),
                Cell("G000000", "02400", "00100"),
                Cell("G000000", "02600", "00100"),
                Cell("G000000", "02800", "00100"),
            ],
            FORM_1996: [
                Cell("G000000", "01301", "0100"),
                Cell("G000000", "01401", "0100"),
                Cell("G000000", "01501", "0100"),
                Cell("G000000", "01601", "0100"),
                Cell("G000000", "01701", "0100"),
                Cell("G000000", "01801", "0100"),
                Cell("G000000", "01901", "0100"),
            ],
        },
        "imccart",
        "Accumulated depreciation, summed over the asset class lines",
        "Depreciação acumulada, somada sobre as linhas de classes de ativos",
        "Depreciación acumulada, sumada sobre las líneas de clases de activos",
        absolute=True,
        note=(
            "Summed over land improvements, buildings, leasehold improvements, "
            "fixed equipment, automobiles, major and minor movable equipment, "
            "and (2552-10 only) health information technology. Each line is "
            "taken as an absolute value first, following imccart: the "
            "worksheet expects positive entries and a minority of filers sign "
            "them negative"
        ),
    ),
    Measure(
        "new_capital_assets",
        "numeric",
        "usd",
        {
            FORM_2010: _c("A700001", "01000", "00200"),
            FORM_1996: _c("A700002", "00900", "0200"),
        },
        "imccart",
        "Acquisitions of capital assets in the reporting period",
        "Aquisições de ativos de capital no período do relatório",
        "Adquisiciones de activos de capital en el período del informe",
    ),
]


# --------------------------------------------------------------------------
# Worksheets E Part A, D-1, D-3/D-4 — Medicare payments and program costs
# --------------------------------------------------------------------------

MEDICARE = [
    Measure(
        "total_medicare_payment",
        "numeric",
        "usd",
        {
            FORM_2010: _c("E00A18A", "05900", "00100"),
            FORM_1996: _c("E00A18A", "01600", "0100"),
        },
        "imccart",
        "Total Medicare payment for inpatient hospital services",
        "Pagamento total do Medicare por serviços hospitalares de internação",
        "Pago total de Medicare por servicios hospitalarios de hospitalización",
    ),
    Measure(
        "secondary_medicare_payment",
        "numeric",
        "usd",
        {
            FORM_2010: _c("E00A18A", "06000", "00100"),
            FORM_1996: _c("E00A18A", "01700", "0100"),
        },
        "imccart",
        "Medicare secondary payer payment",
        "Pagamento do Medicare como pagador secundário",
        "Pago de Medicare como pagador secundario",
    ),
    Measure(
        "value_based_purchasing_adjustment",
        "numeric",
        "usd",
        {FORM_2010: _c("E00A18A", "07093", "00100")},
        "imccart",
        "Hospital Value-Based Purchasing payment adjustment",
        "Ajuste de pagamento do programa Hospital Value-Based Purchasing",
        "Ajuste de pago del programa Hospital Value-Based Purchasing",
        note="Collected on CMS Form 2552-10 only; the programme began in FY2013",
    ),
    Measure(
        "readmissions_reduction_adjustment",
        "numeric",
        "usd",
        {FORM_2010: _c("E00A18A", "07094", "00100")},
        "imccart",
        "Hospital Readmissions Reduction Program payment adjustment",
        "Ajuste de pagamento do programa Hospital Readmissions Reduction",
        "Ajuste de pago del programa Hospital Readmissions Reduction",
        absolute=True,
        note="Collected on CMS Form 2552-10 only; the programme began in FY2013",
    ),
    Measure(
        "medicare_inpatient_total_cost",
        "numeric",
        "usd",
        {
            FORM_2010: _c("D10A181", "04900", "00100"),
            FORM_1996: _c("D10A181", "04900", "0100"),
        },
        "imccart",
        "Total Medicare inpatient cost",
        "Custo total de internação do Medicare",
        "Costo total de hospitalización de Medicare",
    ),
    Measure(
        "medicare_inpatient_operating_cost",
        "numeric",
        "usd",
        {
            FORM_2010: _c("D10A181", "05300", "00100"),
            FORM_1996: _c("D10A181", "05300", "0100"),
        },
        "asacarny, imccart",
        "Medicare inpatient program operating cost",
        "Custo operacional do programa de internação do Medicare",
        "Costo operativo del programa de hospitalización de Medicare",
    ),
    Measure(
        "medicare_inpatient_routine_charges",
        "numeric",
        "usd",
        {
            FORM_2010: _c("D30A180", "03000", "00200", "03599"),
            FORM_1996: _c("D40A180", "02500", "0200", "03099"),
        },
        "asacarny",
        "Medicare inpatient program routine service charges",
        "Cobranças de serviços de rotina do programa de internação do Medicare",
        "Cargos de servicios de rutina del programa de hospitalización de Medicare",
    ),
    Measure(
        "medicare_inpatient_ancillary_net_charges",
        "numeric",
        "usd",
        {
            FORM_2010: _c("D30A180", "20200", "00200"),
            FORM_1996: _c("D40A180", "10300", "0200"),
        },
        "asacarny",
        "Medicare inpatient program ancillary service net charges",
        "Cobranças líquidas de serviços auxiliares do programa de internação do Medicare",
        "Cargos netos de servicios auxiliares del programa de hospitalización de Medicare",
        note=(
            "The worksheet is D-3 under CMS Form 2552-10 and D-4 under "
            "2552-96, hence the different worksheet codes"
        ),
    ),
]

MEASURES: list[Measure] = [
    *IDENTITY,
    *INCOME_STATEMENT,
    *PATIENT_REVENUE,
    *UNCOMPENSATED_CARE,
    *UTILIZATION,
    *BALANCE_SHEET,
    *MEDICARE,
]

_names = [m.name for m in MEASURES]
assert len(_names) == len(set(_names)), "duplicate measure name"
assert all(m.kind in ("numeric", "alpha") for m in MEASURES)
assert all(
    set(m.by_form) <= {FORM_1996, FORM_2010} and m.by_form for m in MEASURES
)


def provenance(measure: Measure) -> str:
    """Render a measure's cell addresses as one human-readable line.

    This string is published in the column's ``observations``, so every value
    in ``hospital_financial`` can be traced back to the exact cells it was read
    from without leaving the dataset.

    Args:
        measure: The measure to describe.

    Returns:
        A sentence naming the source mapping and every cell address per form.
    """
    parts = []
    for form in (FORM_2010, FORM_1996):
        cells = measure.by_form.get(form)
        if not cells:
            continue
        rendered = "; ".join(
            f"worksheet {c.worksheet} line {c.line}"
            + (f"-{c.line_end}" if c.line_end else "")
            + f" column {c.column}"
            for c in cells
        )
        parts.append(f"CMS Form {form}: {rendered}")
    body = ". ".join(parts)
    tail = f". {measure.note}" if measure.note else ""
    return (
        f"Read from the {measure.kind} value of {body}"
        f". Mapping published by {measure.source}{tail}"
    )


if __name__ == "__main__":
    for m in MEASURES:
        print(f"{m.name:<42} {m.kind:<8} {m.unit:<10} {sorted(m.by_form)}")
    print(f"\n{len(MEASURES)} measures")
