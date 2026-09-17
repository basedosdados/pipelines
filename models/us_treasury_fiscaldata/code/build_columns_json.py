#!/usr/bin/env python3
"""Emit columns_json payloads for mcp__databasis__bulk_upsert_columns, one per table.

Reads the architecture CSVs (English descriptions + type/dictionary/unit flags)
and attaches Portuguese and Spanish translations from TRANSLATIONS, so columns
register directly with no Google Sheet. English comes from the architecture CSV;
this file supplies PT and ES only. A ``name:table`` key overrides a plain
``name`` key where the same column means something table-specific (record_date).

Usage:
    uv run python models/us_treasury_fiscaldata/code/build_columns_json.py
"""

import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARCH = ROOT / "code" / "architecture"
OUT = ROOT / "code" / "columns_json"

# name (or name:table) -> (description_pt, description_es)
TRANSLATIONS = {
    "year": (
        "Ano civil da data de registro",
        "Año civil de la fecha de registro",
    ),
    "month": (
        "Mês civil da data de registro",
        "Mes civil de la fecha de registro",
    ),
    "fiscal_year": (
        "Ano fiscal federal dos Estados Unidos da data de registro, de outubro a setembro",
        "Año fiscal federal de los Estados Unidos de la fecha de registro, de octubre a septiembre",
    ),
    "record_date": (
        "Último dia do mês que o demonstrativo reporta",
        "Último día del mes que reporta el estado",
    ),
    "record_date:debt_outstanding": (
        "Data em que o valor da dívida foi reportado, publicada em dias úteis",
        "Fecha en que se reportó el valor de la deuda, publicada en días hábiles",
    ),
    "record_date:monthly_treasury_statement": (
        "Último dia do mês que o demonstrativo reporta",
        "Último día del mes que reporta el estado",
    ),
    "record_date:average_interest_rate": (
        "Último dia do mês que a taxa média de juros reporta",
        "Último día del mes que reporta la tasa de interés promedio",
    ),
    "record_date:exchange_rate": (
        "Último dia do trimestre que a taxa de câmbio reporta",
        "Último día del trimestre que reporta el tipo de cambio",
    ),
    "record_date:historical_debt_outstanding": (
        "Data que o valor da dívida reporta, o último dia do ano fiscal",
        "Fecha que reporta el valor de la deuda, el último día del año fiscal",
    ),
    "debt_outstanding": (
        "Dívida pública total em aberto do governo dos EUA no final do ano fiscal",
        "Deuda pública total pendiente del gobierno de EE. UU. al final del año fiscal",
    ),
    "total_public_debt_outstanding": (
        "Dívida pública total em aberto na data de registro, a soma da dívida em poder do público e das participações intragovernamentais",
        "Deuda pública total pendiente en la fecha de registro, la suma de la deuda en poder del público y las tenencias intragubernamentales",
    ),
    "debt_held_by_public": (
        "Parcela da dívida pública total em poder do público, e não de contas do governo",
        "Parte de la deuda pública total en poder del público, no de cuentas del gobierno",
    ),
    "intragovernmental_holdings": (
        "Parcela da dívida pública total em poder de fundos fiduciários, fundos rotativos e fundos especiais do governo",
        "Parte de la deuda pública total en poder de fondos fiduciarios, fondos rotatorios y fondos especiales del gobierno",
    ),
    "table_nbr": (
        "Número da tabela do Demonstrativo Mensal do Tesouro de onde a linha vem, de 1 a 9, identificando receitas, despesas por agência, meios de financiamento ou resumo",
        "Número de la tabla del Estado Mensual del Tesoro de donde proviene la fila, del 1 al 9, que identifica ingresos, gastos por agencia, medios de financiamiento o resumen",
    ),
    "src_line_nbr": (
        "Número da linha de origem dentro da tabela do MTS, estável entre publicações para um mesmo item",
        "Número de línea de origen dentro de la tabla del MTS, estable entre publicaciones para un mismo ítem",
    ),
    "parent_id": (
        "Identificador de classificação da linha superior na hierarquia do demonstrativo, vazio para linhas de nível mais alto",
        "Identificador de clasificación de la fila superior en la jerarquía del estado, vacío para las filas de nivel más alto",
    ),
    "classification_id": (
        "Identificador de classificação da linha dentro de uma única publicação, não estável entre publicações",
        "Identificador de clasificación de la fila dentro de una sola publicación, no estable entre publicaciones",
    ),
    "classification_desc": (
        "Nome da fonte de receita, agência de despesa, função orçamentária ou categoria de financiamento que a linha reporta",
        "Nombre de la fuente de ingreso, agencia de gasto, función presupuestaria o categoría de financiamiento que reporta la fila",
    ),
    "sequence_level_nbr": (
        "Profundidade da linha na hierarquia do demonstrativo, de 1 no topo até os componentes mais internos",
        "Profundidad de la fila en la jerarquía del estado, de 1 en la cima hasta los componentes más internos",
    ),
    "line_item": (
        "Nome do indicador derretido que identifica qual valor a linha reporta, como despesa bruta do mês corrente ou receita líquida acumulada no ano fiscal",
        "Nombre del indicador desnormalizado que identifica qué monto reporta la fila, como gasto bruto del mes corriente o ingreso neto acumulado del año fiscal",
    ),
    "amount": (
        "Valor em dólares para a linha de classificação e o indicador",
        "Monto en dólares para la fila de clasificación y el indicador",
    ),
    "security_type": (
        "Classe ampla de título do Tesouro, como Negociável, Não negociável ou Dívida com juros",
        "Clase amplia de título del Tesoro, como Negociable, No negociable o Deuda con intereses",
    ),
    "security_desc": (
        "Título específico do Tesouro ao qual a taxa se aplica, como Notas do Tesouro ou Títulos do Tesouro",
        "Título específico del Tesoro al que se aplica la tasa, como Notas del Tesoro o Bonos del Tesoro",
    ),
    "avg_interest_rate": (
        "Taxa de juros média que o Tesouro paga sobre o título em aberto, como percentual anual",
        "Tasa de interés promedio que el Tesoro paga sobre el título pendiente, como porcentaje anual",
    ),
    "effective_date": (
        "Data em que a taxa de câmbio entra em vigor para fins de reporte",
        "Fecha en que el tipo de cambio entra en vigor para fines de reporte",
    ),
    "country": (
        "País cuja moeda a taxa converte",
        "País cuya moneda convierte la tasa",
    ),
    "currency": (
        "Moeda que a taxa converte para dólares dos Estados Unidos",
        "Moneda que la tasa convierte a dólares de los Estados Unidos",
    ),
    "country_currency_desc": (
        "País e moeda combinados, conforme publicado pelo Tesouro",
        "País y moneda combinados, según lo publicado por el Tesoro",
    ),
    "exchange_rate": (
        "Unidades da moeda estrangeira por um dólar dos Estados Unidos, a taxa de reporte do Tesouro",
        "Unidades de la moneda extranjera por un dólar de los Estados Unidos, la tasa de reporte del Tesoro",
    ),
    "id_tabela": (
        "Slug da tabela de us_treasury_fiscaldata que a entrada do dicionário descreve",
        "Slug de la tabla de us_treasury_fiscaldata que describe la entrada del diccionario",
    ),
    "nome_coluna": (
        "Nome da coluna que a entrada do dicionário descreve",
        "Nombre de la columna que describe la entrada del diccionario",
    ),
    "chave": (
        "Valor codificado (chave) exatamente como armazenado nos dados",
        "Valor codificado (clave) exactamente como se almacena en los datos",
    ),
    "cobertura_temporal": (
        "Cobertura temporal da chave",
        "Cobertura temporal de la clave",
    ),
    "valor": (
        "Rótulo legível correspondente ao valor codificado",
        "Etiqueta legible correspondiente al valor codificado",
    ),
}


# MTS wide amount columns are period x measure; compose PT/ES from fragments
# rather than repeating ~27 near-duplicate literals.
_MTS_PERIOD = {
    "current_month": ("no mês corrente", "en el mes corriente"),
    "current_fytd": (
        "no ano fiscal corrente até a data",
        "en el año fiscal corriente hasta la fecha",
    ),
    "prior_fytd": (
        "no ano fiscal anterior até a data",
        "en el año fiscal anterior hasta la fecha",
    ),
}
_MTS_MEASURE = {
    "gross_receipts": (
        "Receitas brutas da fonte",
        "Ingresos brutos de la fuente",
    ),
    "refunds": (
        "Reembolsos de receitas da fonte",
        "Reembolsos de ingresos de la fuente",
    ),
    "net_receipts": (
        "Receitas líquidas da fonte",
        "Ingresos netos de la fuente",
    ),
    "gross_outlays": (
        "Despesas brutas da agência",
        "Gastos brutos de la agencia",
    ),
    "applicable_receipts": (
        "Receitas aplicadas contra as despesas da agência",
        "Ingresos aplicados contra los gastos de la agencia",
    ),
    "net_outlays": (
        "Despesas líquidas da agência",
        "Gastos netos de la agencia",
    ),
    "net_transactions": (
        "Transações líquidas de financiamento",
        "Transacciones netas de financiamiento",
    ),
}
# amount columns that are not period x measure
_MTS_SPECIAL = {
    "current_month_gross_receipts": (
        "Receitas totais do governo dos EUA no mês corrente",
        "Ingresos totales del gobierno de EE. UU. en el mes corriente",
    ),
    "current_month_gross_outlays": (
        "Despesas totais do governo dos EUA no mês corrente",
        "Gastos totales del gobierno de EE. UU. en el mes corriente",
    ),
    "current_month_deficit_or_surplus": (
        "Déficit (negativo) ou superávit (positivo) orçamentário no mês corrente",
        "Déficit (negativo) o superávit (positivo) presupuestario en el mes corriente",
    ),
    "beginning_year_balance": (
        "Saldo da conta no início do ano fiscal",
        "Saldo de la cuenta al inicio del año fiscal",
    ),
    "beginning_month_balance": (
        "Saldo da conta no início do mês",
        "Saldo de la cuenta al inicio del mes",
    ),
    "closing_month_balance": (
        "Saldo da conta no fim do mês",
        "Saldo de la cuenta al cierre del mes",
    ),
}


def _mts_amount_tr(name, table):
    """Compose (pt, es) for a period x measure MTS amount column, else None.

    The ``_MTS_SPECIAL`` "total government" wording is correct only for
    ``mts_summary`` (Table 1); in the detail tables the same column name means
    "from the source"/"of the agency", so compose from fragments there instead.
    """
    if table == "mts_summary" and name in _MTS_SPECIAL:
        return _MTS_SPECIAL[name]
    if name in _MTS_SPECIAL and name not in (
        "current_month_gross_receipts",
        "current_month_gross_outlays",
    ):
        return _MTS_SPECIAL[name]  # balances, deficit — table-independent
    for period, (ppt, pes) in _MTS_PERIOD.items():
        if name.startswith(period + "_"):
            measure = name[len(period) + 1 :]
            if measure in _MTS_MEASURE:
                mpt, mes = _MTS_MEASURE[measure]
                return (f"{mpt} {ppt}", f"{mes} {pes}")
    return None


def tr(name, table):
    hit = TRANSLATIONS.get(f"{name}:{table}") or TRANSLATIONS.get(name)
    if hit:
        return hit
    composed = _mts_amount_tr(name, table)
    if composed:
        return composed
    raise KeyError(f"No translation for column {name!r} (table {table})")


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    for csv_path in sorted(ARCH.glob("*.csv")):
        table = csv_path.stem
        cols = []
        with open(csv_path, newline="") as fh:
            rows = list(csv.DictReader(fh))
        for r in rows:
            pt, es = tr(r["name"], table)
            col = {
                "name": r["name"],
                "bigquery_type": r["bigquery_type"],
                "description_pt": pt,
                "description_en": r["description"],
                "description_es": es,
                "covered_by_dictionary": r["covered_by_dictionary"]
                .strip()
                .lower()
                == "yes",
                "has_sensitive_data": r["has_sensitive_data"].strip().lower()
                == "yes",
            }
            if r["directory_column"].strip():
                col["directory_column"] = r["directory_column"].strip()
            if r["measurement_unit"].strip():
                col["measurement_unit"] = r["measurement_unit"].strip()
            cols.append(col)
        (OUT / f"{table}.json").write_text(
            json.dumps(cols, ensure_ascii=False, indent=2)
        )
        print(
            f"{table}: {len(cols)} columns -> code/columns_json/{table}.json"
        )


if __name__ == "__main__":
    main()
