#!/usr/bin/env python3
"""Emit columns_json payloads for mcp__databasis__bulk_upsert_columns, one per
table. Reads the architecture CSVs (English descriptions + type/dictionary/unit
flags) and attaches Portuguese and Spanish translations from TRANSLATIONS below,
so columns can be registered directly (no Google Sheet).

Usage:
    uv run python models/au_apra_superannuation/code/build_columns_json.py
"""

import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARCH = ROOT / "code" / "architecture"
OUT = ROOT / "code" / "columns_json"

# name -> (description_pt, description_es). English comes from the architecture CSV.
TRANSLATIONS = {
    # keys
    "year": (
        "Ano de referência da observação (fim do trimestre)",
        "Año de referencia de la observación (fin del trimestre)",
    ),
    "quarter": (
        "Trimestre de referência da observação, de 1 a 4",
        "Trimestre de referencia de la observación, de 1 a 4",
    ),
    "fund_type": (
        "Tipo de fundo de previdência: todos (indústria total), corporativo, setorial (industry), setor público ou varejo (retail)",
        "Tipo de fondo de jubilación: todos (industria total), corporativo, sectorial (industry), sector público o minorista (retail)",
    ),
    # performance
    "net_assets_beginning": (
        "Ativos líquidos disponíveis para pagar benefícios no início do trimestre",
        "Activos netos disponibles para pagar beneficios al inicio del trimestre",
    ),
    "total_contributions": (
        "Total de contribuições recebidas no trimestre",
        "Total de contribuciones recibidas en el trimestre",
    ),
    "employer_contributions": (
        "Contribuições patronais recebidas no trimestre",
        "Contribuciones del empleador recibidas en el trimestre",
    ),
    "employer_defined_benefit_contributions": (
        "Contribuições patronais para planos de benefício definido no trimestre",
        "Contribuciones del empleador a planes de beneficio definido en el trimestre",
    ),
    "employer_super_guarantee_contributions": (
        "Contribuições patronais feitas sob a Garantia de Previdência compulsória (Superannuation Guarantee) no trimestre",
        "Contribuciones del empleador realizadas bajo la Garantía de Jubilación obligatoria (Superannuation Guarantee) en el trimestre",
    ),
    "employer_salary_sacrifice_contributions": (
        "Contribuições patronais feitas por meio de acordos de salary sacrifice no trimestre",
        "Contribuciones del empleador realizadas mediante acuerdos de salary sacrifice en el trimestre",
    ),
    "member_contributions": (
        "Contribuições dos participantes recebidas no trimestre",
        "Contribuciones de los afiliados recibidas en el trimestre",
    ),
    "member_personal_contributions": (
        "Contribuições pessoais feitas pelos participantes no trimestre",
        "Contribuciones personales realizadas por los afiliados en el trimestre",
    ),
    "government_co_contributions": (
        "Contrapartidas do governo (co-contributions) recebidas no trimestre",
        "Aportes de contrapartida del gobierno (co-contributions) recibidos en el trimestre",
    ),
    "low_income_super_contributions": (
        "Contribuições previdenciárias para baixa renda recebidas no trimestre",
        "Contribuciones de jubilación para bajos ingresos recibidas en el trimestre",
    ),
    "other_member_contributions": (
        "Outras contribuições dos participantes recebidas no trimestre",
        "Otras contribuciones de los afiliados recibidas en el trimestre",
    ),
    "contribution_tax_and_surcharge": (
        "Imposto e sobretaxa sobre contribuições no trimestre",
        "Impuesto y recargo sobre las contribuciones en el trimestre",
    ),
    "net_benefit_transfers": (
        "Transferências líquidas de benefícios para dentro ou fora do tipo de fundo no trimestre",
        "Transferencias netas de beneficios hacia o desde el tipo de fondo en el trimestre",
    ),
    "net_rollovers_to_from_smsf": (
        "Rollovers líquidos para ou de fundos autogeridos (SMSFs) no trimestre",
        "Traspasos netos hacia o desde fondos autogestionados (SMSFs) en el trimestre",
    ),
    "benefit_transfers_inward": (
        "Transferências de benefícios recebidas (entrada) no trimestre",
        "Transferencias de beneficios recibidas (entrada) en el trimestre",
    ),
    "benefit_transfers_outward": (
        "Transferências de benefícios pagas (saída) no trimestre",
        "Transferencias de beneficios pagadas (salida) en el trimestre",
    ),
    "benefit_payments": (
        "Total de pagamentos de benefícios no trimestre",
        "Total de pagos de beneficios en el trimestre",
    ),
    "lump_sum_benefits": (
        "Pagamentos de benefícios em parcela única no trimestre",
        "Pagos de beneficios en suma única en el trimestre",
    ),
    "pension_benefits": (
        "Pagamentos de benefícios em forma de pensão no trimestre",
        "Pagos de beneficios en forma de pensión en el trimestre",
    ),
    "other_members_benefit_flows": (
        "Outros fluxos de benefícios dos participantes no trimestre",
        "Otros flujos de beneficios de los afiliados en el trimestre",
    ),
    "net_contribution_flows": (
        "Fluxos líquidos de contribuições no trimestre (contribuições e transferências menos pagamentos de benefícios)",
        "Flujos netos de contribuciones en el trimestre (contribuciones y transferencias menos pagos de beneficios)",
    ),
    "net_insurance_flows": (
        "Fluxos líquidos de seguros no trimestre",
        "Flujos netos de seguros en el trimestre",
    ),
    "insurance_flows_inward": (
        "Fluxos de seguros recebidos (entrada) no trimestre",
        "Flujos de seguros recibidos (entrada) en el trimestre",
    ),
    "insurance_flows_outward": (
        "Fluxos de seguros pagos (saída) no trimestre",
        "Flujos de seguros pagados (salida) en el trimestre",
    ),
    "investment_income": (
        "Receita de investimentos no trimestre",
        "Ingresos por inversiones en el trimestre",
    ),
    "investment_income_after_impairment": (
        "Receita de investimentos após despesa de impairment no trimestre",
        "Ingresos por inversiones después del gasto por deterioro en el trimestre",
    ),
    "total_gains_losses_on_investments": (
        "Ganhos ou perdas totais, realizados e não realizados, em investimentos no trimestre",
        "Ganancias o pérdidas totales, realizadas y no realizadas, en inversiones en el trimestre",
    ),
    "foreign_exchange_gains_losses": (
        "Ganhos ou perdas cambiais no trimestre",
        "Ganancias o pérdidas cambiarias en el trimestre",
    ),
    "investment_expenses": (
        "Despesas de investimento no trimestre",
        "Gastos de inversión en el trimestre",
    ),
    "operating_income": (
        "Receita operacional no trimestre",
        "Ingresos operativos en el trimestre",
    ),
    "administration_and_operating_expenses": (
        "Despesas administrativas e operacionais no trimestre",
        "Gastos administrativos y operativos en el trimestre",
    ),
    "net_earnings": (
        "Resultado líquido no trimestre",
        "Resultado neto en el trimestre",
    ),
    "income_tax_expense_benefit": (
        "Despesa ou benefício de imposto de renda no trimestre",
        "Gasto o beneficio por impuesto a la renta en el trimestre",
    ),
    "net_earnings_after_tax": (
        "Resultado líquido após impostos no trimestre",
        "Resultado neto después de impuestos en el trimestre",
    ),
    "net_operating_performance_after_tax": (
        "Desempenho operacional líquido após impostos no trimestre",
        "Desempeño operativo neto después de impuestos en el trimestre",
    ),
    "other_changes": (
        "Outras variações nos ativos líquidos no trimestre",
        "Otras variaciones en los activos netos en el trimestre",
    ),
    "net_assets_end": (
        "Ativos líquidos disponíveis para pagar benefícios no fim do trimestre",
        "Activos netos disponibles para pagar beneficios al final del trimestre",
    ),
    "number_of_entities": (
        "Número de entidades de previdência no tipo de fundo",
        "Número de entidades de jubilación en el tipo de fondo",
    ),
    # position
    "receivables": (
        "Valores a receber no fim do trimestre",
        "Cuentas por cobrar al final del trimestre",
    ),
    "investments": (
        "Total de investimentos no fim do trimestre",
        "Total de inversiones al final del trimestre",
    ),
    "securities_purchased_under_resale_agreements": (
        "Títulos comprados sob acordos de revenda e títulos tomados em empréstimo no fim do trimestre",
        "Valores comprados bajo acuerdos de reventa y valores tomados en préstamo al final del trimestre",
    ),
    "tax_assets": (
        "Ativos fiscais no fim do trimestre",
        "Activos fiscales al final del trimestre",
    ),
    "other_assets": (
        "Outros ativos no fim do trimestre",
        "Otros activos al final del trimestre",
    ),
    "total_assets": (
        "Total de ativos no fim do trimestre",
        "Total de activos al final del trimestre",
    ),
    "securities_sold_under_repurchase_agreements": (
        "Títulos vendidos sob acordos de recompra e títulos emprestados no fim do trimestre",
        "Valores vendidos bajo acuerdos de recompra y valores prestados al final del trimestre",
    ),
    "tax_liabilities": (
        "Passivos fiscais no fim do trimestre",
        "Pasivos fiscales al final del trimestre",
    ),
    "other_liabilities": (
        "Outros passivos no fim do trimestre",
        "Otros pasivos al final del trimestre",
    ),
    "total_liabilities": (
        "Total de passivos no fim do trimestre",
        "Total de pasivos al final del trimestre",
    ),
    "liability_for_allocated_accrued_benefits": (
        "Passivo por benefícios acumulados alocados no fim do trimestre",
        "Pasivo por beneficios devengados asignados al final del trimestre",
    ),
    "liability_for_members_benefits": (
        "Passivo por benefícios dos participantes no fim do trimestre",
        "Pasivo por beneficios de los afiliados al final del trimestre",
    ),
    "defined_contribution_members_benefits": (
        "Benefícios de participantes de contribuição definida no fim do trimestre",
        "Beneficios de afiliados de contribución definida al final del trimestre",
    ),
    "defined_benefit_members_benefits": (
        "Benefícios de participantes de benefício definido no fim do trimestre",
        "Beneficios de afiliados de beneficio definido al final del trimestre",
    ),
    "unallocated_benefits": (
        "Benefícios não alocados no fim do trimestre",
        "Beneficios no asignados al final del trimestre",
    ),
    "reserves_including_unallocated_benefits": (
        "Reservas incluindo benefícios não alocados no fim do trimestre",
        "Reservas incluyendo beneficios no asignados al final del trimestre",
    ),
    "reserves": (
        "Reservas no fim do trimestre",
        "Reservas al final del trimestre",
    ),
    "excess_deficiency_of_assets": (
        "Excesso ou deficiência de ativos no fim do trimestre",
        "Exceso o deficiencia de activos al final del trimestre",
    ),
    "surplus_deficit_in_net_assets": (
        "Superávit ou déficit nos ativos líquidos no fim do trimestre",
        "Superávit o déficit en los activos netos al final del trimestre",
    ),
    "net_assets_available_to_pay_benefits": (
        "Ativos líquidos disponíveis para pagar benefícios aos participantes no fim do trimestre",
        "Activos netos disponibles para pagar beneficios a los afiliados al final del trimestre",
    ),
    "defined_benefit_interests": (
        "Ativos líquidos atribuíveis a participações de benefício definido no fim do trimestre",
        "Activos netos atribuibles a participaciones de beneficio definido al final del trimestre",
    ),
    # ratios
    "net_cash_flows": (
        "Fluxos de caixa líquidos no trimestre, insumo do cálculo da taxa de retorno",
        "Flujos de caja netos en el trimestre, insumo del cálculo de la tasa de retorno",
    ),
    "cash_flow_adjusted_net_assets": (
        "Ativos líquidos ajustados pelo fluxo de caixa, insumo do cálculo da taxa de retorno",
        "Activos netos ajustados por el flujo de caja, insumo del cálculo de la tasa de retorno",
    ),
    "investment_expense": (
        "Despesa de investimento usada no cálculo da taxa de retorno",
        "Gasto de inversión usado en el cálculo de la tasa de retorno",
    ),
    "administration_and_operating_expense": (
        "Despesa administrativa e operacional usada no cálculo da taxa de retorno",
        "Gasto administrativo y operativo usado en el cálculo de la tasa de retorno",
    ),
    "rate_of_return": (
        "Taxa de retorno trimestral, expressa como proporção (por exemplo, 0,056 = 5,6%)",
        "Tasa de retorno trimestral, expresada como proporción (por ejemplo, 0,056 = 5,6%)",
    ),
    "five_year_annualised_rate_of_return": (
        "Taxa de retorno anualizada de cinco anos, expressa como proporção",
        "Tasa de retorno anualizada de cinco años, expresada como proporción",
    ),
    # dicionario
    "id_tabela": (
        "Slug da tabela de au_apra_superannuation que a entrada do dicionário descreve",
        "Slug de la tabla de au_apra_superannuation que describe la entrada del diccionario",
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
