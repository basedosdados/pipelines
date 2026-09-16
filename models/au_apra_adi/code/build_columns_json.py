#!/usr/bin/env python3
"""Emit columns_json payloads for mcp__databasis__bulk_upsert_columns, one per
table. Reads the architecture CSVs (English descriptions + type/dictionary/unit
flags) and attaches Portuguese and Spanish translations from TRANSLATIONS below,
so columns can be registered directly (no Google Sheet).

Usage:
    uv run python models/au_apra_adi/code/build_columns_json.py
"""

import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARCH = ROOT / "code" / "architecture"
OUT = ROOT / "code" / "columns_json"

# name -> (description_pt, description_es). English comes from the architecture CSV.
TRANSLATIONS = {
    # keys / fixed columns (long tables share these)
    "year": (
        "Ano de referência da observação (fim do trimestre)",
        "Año de referencia de la observación (fin del trimestre)",
    ),
    "quarter": (
        "Trimestre de referência da observação, de 1 a 4",
        "Trimestre de referencia de la observación, de 1 a 4",
    ),
    "institution_type": (
        "Tipo de instituição autorizada a receber depósitos (ADI) ou agrupamento",
        "Tipo de institución autorizada para recibir depósitos (ADI) o agrupamiento",
    ),
    "measure": (
        "Medida codificada dentro da demonstração (veja o rótulo no dicionário)",
        "Medida codificada dentro del estado (ver la etiqueta en el diccionario)",
    ),
    "unit": (
        "Unidade do valor: aud_million, proportion ou unit",
        "Unidad del valor: aud_million, proportion o unit",
    ),
    "value": (
        "Valor reportado da medida (unidade dada pela coluna unit; NULO quando não aplicável ou mascarado por confidencialidade)",
        "Valor reportado de la medida (unidad dada por la columna unit; NULO cuando no es aplicable o está enmascarado por confidencialidad)",
    ),
    # financial_performance — income statement
    "interest_income": (
        "Receita de juros",
        "Ingresos por intereses",
    ),
    "cash_and_liquid_assets": (
        "Caixa e ativos líquidos",
        "Efectivo y activos líquidos",
    ),
    "loans_and_advances": (
        "Empréstimos e adiantamentos",
        "Préstamos y anticipos",
    ),
    "housing_loans": (
        "Empréstimos habitacionais",
        "Préstamos para vivienda",
    ),
    "term_loans": (
        "Empréstimos a prazo",
        "Préstamos a plazo",
    ),
    "interest_income__other": (
        "Outros (receita de juros)",
        "Otros (ingresos por intereses)",
    ),
    "other_interest_earning_assets": (
        "Outros ativos remunerados por juros",
        "Otros activos que devengan intereses",
    ),
    "interest_expense": (
        "Despesa de juros",
        "Gastos por intereses",
    ),
    "deposits": (
        "Depósitos",
        "Depósitos",
    ),
    "borrowings": (
        "Empréstimos tomados",
        "Préstamos obtenidos",
    ),
    "other_interest_bearing_liabilities": (
        "Outros passivos remunerados por juros",
        "Otros pasivos que devengan intereses",
    ),
    "net_interest_income": (
        "Receita líquida de juros",
        "Ingresos netos por intereses",
    ),
    "other_operating_income": (
        "Outras receitas operacionais",
        "Otros ingresos operativos",
    ),
    "fee_and_commission": (
        "Tarifas e comissões",
        "Comisiones y tarifas",
    ),
    "lending": (
        "Concessão de crédito",
        "Concesión de crédito",
    ),
    "transaction_deposit_account_service_fee": (
        "Tarifa de serviço de conta transacional/de depósito",
        "Tarifa de servicio de cuenta transaccional/de depósito",
    ),
    "other_fee_based_activities": (
        "Outras atividades baseadas em tarifas",
        "Otras actividades basadas en comisiones",
    ),
    "other_operating_income__other": (
        "Outros (outras receitas operacionais)",
        "Otros (otros ingresos operativos)",
    ),
    "total_operating_income": (
        "Receita operacional total",
        "Ingresos operativos totales",
    ),
    "charge_for_bad_or_doubtful_debts": (
        "Provisão para créditos incobráveis ou de liquidação duvidosa",
        "Cargo por créditos incobrables o de dudosa recuperación",
    ),
    "total_operating_expenses": (
        "Despesas operacionais totais",
        "Gastos operativos totales",
    ),
    "personnel": (
        "Pessoal",
        "Personal",
    ),
    "fees_and_commissions": (
        "Tarifas e comissões",
        "Comisiones y tarifas",
    ),
    "total_operating_expenses__other": (
        "Outros (despesas operacionais)",
        "Otros (gastos operativos)",
    ),
    "profit_before_tax": (
        "Lucro antes de impostos",
        "Beneficio antes de impuestos",
    ),
    "income_tax": (
        "Imposto de renda",
        "Impuesto a la renta",
    ),
    "net_profit_after_taxa": (
        "Lucro (prejuízo) líquido após impostos",
        "Beneficio (pérdida) neto después de impuestos",
    ),
    "number_of_entities": (
        "Número de entidades",
        "Número de entidades",
    ),
    "operating_expenses__other": (
        "Outros (despesas operacionais)",
        "Otros (gastos operativos)",
    ),
    # financial_position — balance sheet
    "securities": (
        "Títulos e valores mobiliários",
        "Valores",
    ),
    "acceptances_of_customers": (
        "Aceites de clientes",
        "Aceptaciones de clientes",
    ),
    "gross_loans_and_advances": (
        "Empréstimos e adiantamentos brutos",
        "Préstamos y anticipos brutos",
    ),
    "total_housing": (
        "Habitação total",
        "Vivienda total",
    ),
    "term": (
        "A prazo",
        "A plazo",
    ),
    "gross_loans_and_advances__other": (
        "Outros (empréstimos e adiantamentos brutos)",
        "Otros (préstamos y anticipos brutos)",
    ),
    "lending_provisions": (
        "Provisões para crédito",
        "Provisiones para préstamos",
    ),
    "net_loans_and_advances": (
        "Empréstimos e adiantamentos líquidos",
        "Préstamos y anticipos netos",
    ),
    "fixed_assets": (
        "Ativos fixos",
        "Activos fijos",
    ),
    "intangible_assets": (
        "Ativos intangíveis",
        "Activos intangibles",
    ),
    "other_assets": (
        "Outros ativos",
        "Otros activos",
    ),
    "total_assets": (
        "Ativos totais",
        "Activos totales",
    ),
    "acceptances": (
        "Aceites",
        "Aceptaciones",
    ),
    "call_on_demand": (
        "À vista/exigível",
        "A la vista/exigible",
    ),
    "term_deposits": (
        "Depósitos a prazo",
        "Depósitos a plazo",
    ),
    "certificates_of_deposit": (
        "Certificados de depósito",
        "Certificados de depósito",
    ),
    "income_tax_liability": (
        "Passivo de imposto de renda",
        "Pasivo por impuesto a la renta",
    ),
    "provisions": (
        "Provisões",
        "Provisiones",
    ),
    "employee_entitlements": (
        "Direitos de empregados",
        "Derechos de los empleados",
    ),
    "provisions__other": (
        "Outros (provisões)",
        "Otros (provisiones)",
    ),
    "other_short_term_borrowings": (
        "Outros empréstimos de curto prazo",
        "Otros préstamos a corto plazo",
    ),
    "long_term_borrowings": (
        "Empréstimos de longo prazo",
        "Préstamos a largo plazo",
    ),
    "creditors_and_other_liabilities": (
        "Credores e outros passivos",
        "Acreedores y otros pasivos",
    ),
    "total_liabilities": (
        "Passivos totais",
        "Pasivos totales",
    ),
    "share_capital": (
        "Capital social",
        "Capital social",
    ),
    "reserves": (
        "Reservas",
        "Reservas",
    ),
    "retained_profits": (
        "Lucros retidos",
        "Beneficios retenidos",
    ),
    "provisions__other__2": (
        "Outros (provisões)",
        "Otros (provisiones)",
    ),
    "total_shareholders_equity": (
        "Patrimônio líquido total dos acionistas",
        "Patrimonio neto total de los accionistas",
    ),
    "gross_loans_and_advances__intra_group": (
        "Intragrupo (empréstimos e adiantamentos brutos)",
        "Intragrupo (préstamos y anticipos brutos)",
    ),
    "due_from_overseas_operations_of_the_adi": (
        "Valores a receber de operações no exterior da ADI",
        "Cuentas por cobrar de operaciones en el exterior de la ADI",
    ),
    "due_from_non_residents": (
        "Valores a receber de não residentes (excluindo transações intragrupo)",
        "Cuentas por cobrar de no residentes (excluyendo transacciones intragrupo)",
    ),
    "deposits__intra_group": (
        "Intragrupo (depósitos)",
        "Intragrupo (depósitos)",
    ),
    "due_to_overseas_operations_of_the_adi": (
        "Valores a pagar a operações no exterior da ADI",
        "Cuentas por pagar a operaciones en el exterior de la ADI",
    ),
    "due_to_non_residents": (
        "Valores a pagar a não residentes (excluindo transações intragrupo)",
        "Cuentas por pagar a no residentes (excluyendo transacciones intragrupo)",
    ),
    "other_investments": (
        "Outros investimentos",
        "Otras inversiones",
    ),
    "due_to_clearing_houses_and_financial_institutions": (
        "Valores a pagar a câmaras de compensação e instituições financeiras",
        "Cuentas por pagar a cámaras de compensación e instituciones financieras",
    ),
    # performance_ratios
    "net_profit_after_tax": (
        "Lucro (prejuízo) líquido após impostos",
        "Beneficio (pérdida) neto después de impuestos",
    ),
    "average_total_assets": (
        "Ativos totais médios",
        "Activos totales promedio",
    ),
    "average_total_shareholders_equity": (
        "Patrimônio líquido total médio dos acionistas",
        "Patrimonio neto total promedio de los accionistas",
    ),
    "net_interest_income_to_assets": (
        "Receita líquida de juros sobre ativos",
        "Ingresos netos por intereses sobre activos",
    ),
    "operating_income_to_assets": (
        "Receita operacional sobre ativos",
        "Ingresos operativos sobre activos",
    ),
    "operating_expenses_to_assets": (
        "Despesas operacionais sobre ativos",
        "Gastos operativos sobre activos",
    ),
    "profit_margin": (
        "Margem de lucro",
        "Margen de beneficio",
    ),
    "return_on_assets": (
        "Retorno sobre ativos (após impostos)",
        "Rentabilidad sobre activos (después de impuestos)",
    ),
    "return_on_equity": (
        "Retorno sobre patrimônio líquido (após impostos)",
        "Rentabilidad sobre el patrimonio neto (después de impuestos)",
    ),
    "fee_and_commission_income": (
        "Receita de tarifas e comissões",
        "Ingresos por comisiones y tarifas",
    ),
    "total_operating_income__2": (
        "Receita operacional total",
        "Ingresos operativos totales",
    ),
    "operating_expenses__2": (
        "Despesas operacionais",
        "Gastos operativos",
    ),
    "personnel_expenses": (
        "Despesas com pessoal",
        "Gastos de personal",
    ),
    "non_interest_income_share": (
        "Participação da receita não decorrente de juros",
        "Participación de los ingresos no financieros",
    ),
    "fee_income_to_total_operating_income": (
        "Receita de tarifas sobre receita operacional total",
        "Ingresos por comisiones sobre ingresos operativos totales",
    ),
    "cost_to_income": (
        "Índice de eficiência (custo sobre receita)",
        "Ratio de eficiencia (coste sobre ingresos)",
    ),
    "personnel_to_operating_expenses": (
        "Despesas com pessoal sobre despesas operacionais",
        "Gastos de personal sobre gastos operativos",
    ),
    "average_net_loans_and_advances": (
        "Empréstimos e adiantamentos líquidos médios",
        "Préstamos y anticipos netos promedio",
    ),
    "average_deposits": (
        "Depósitos médios",
        "Depósitos promedio",
    ),
    "growth_in_total_assets": (
        "Crescimento dos ativos totais",
        "Crecimiento de los activos totales",
    ),
    "net_loans_to_deposits": (
        "Empréstimos líquidos sobre depósitos",
        "Préstamos netos sobre depósitos",
    ),
    "deposits_to_assets": (
        "Depósitos sobre ativos",
        "Depósitos sobre activos",
    ),
    "equity_to_deposits": (
        "Patrimônio líquido sobre depósitos",
        "Patrimonio neto sobre depósitos",
    ),
    "total_capital_base": (
        "Base de capital total",
        "Base de capital total",
    ),
    "total_risk_weighted_assets": (
        "Ativos ponderados pelo risco totais",
        "Activos ponderados por riesgo totales",
    ),
    "total_capital_ratio": (
        "Índice de capital total",
        "Ratio de capital total",
    ),
    "total_impaired_facilities": (
        "Operações problemáticas totais",
        "Facilidades deterioradas totales",
    ),
    "impaired_facilities_to_loans_and_advances": (
        "Operações problemáticas sobre empréstimos e adiantamentos",
        "Facilidades deterioradas sobre préstamos y anticipos",
    ),
    "total_non_performing_exposures": (
        "Exposições inadimplentes totais",
        "Exposiciones morosas totales",
    ),
    "non_performing_to_loans_and_advances": (
        "Inadimplência sobre empréstimos e adiantamentos",
        "Exposiciones morosas sobre préstamos y anticipos",
    ),
    "total_lcr_liquid_assets": (
        "Ativos líquidos totais do LCR",
        "Activos líquidos totales del LCR",
    ),
    "net_cash_outflows": (
        "Saídas líquidas de caixa",
        "Salidas netas de efectivo",
    ),
    "liquidity_coverage_ratio": (
        "Índice de cobertura de liquidez (LCR)",
        "Ratio de cobertura de liquidez (LCR)",
    ),
    "total_adjusted_minimum_liquidity_holdings": (
        "Reservas mínimas de liquidez ajustadas totais (MLH)",
        "Tenencias mínimas de liquidez ajustadas totales (MLH)",
    ),
    "adjusted_liability_base": (
        "Base de passivos ajustada",
        "Base de pasivos ajustada",
    ),
    "mlh_ratio": (
        "Índice MLH",
        "Ratio MLH",
    ),
    "capital_ratio": (
        "Índice de capital",
        "Ratio de capital",
    ),
    "total_loans_and_advances": (
        "Empréstimos e adiantamentos totais",
        "Préstamos y anticipos totales",
    ),
    # dicionario
    "id_tabela": (
        "Slug da tabela de au_apra_adi que a entrada do dicionário descreve",
        "Slug de la tabla de au_apra_adi que describe la entrada del diccionario",
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


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    missing = set()
    for csv_path in sorted(ARCH.glob("*.csv")):
        table = csv_path.stem
        with open(csv_path, newline="") as fh:
            rows = list(csv.DictReader(fh))
        cols = []
        for r in rows:
            if r["name"] not in TRANSLATIONS:
                missing.add(r["name"])
                continue
            pt, es = TRANSLATIONS[r["name"]]
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
    if missing:
        raise SystemExit(f"MISSING TRANSLATIONS: {sorted(missing)}")


if __name__ == "__main__":
    main()
