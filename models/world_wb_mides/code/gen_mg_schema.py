"""Generate `models/world_wb_mides/mg/schema.yml` from the 43 MG-only models.

Column descriptions come from `mg_column_glossary.py`; table descriptions are in
TABLES below. Re-run after changing either, or after adding/removing a model:

    python models/world_wb_mides/code/gen_mg_schema.py
    uv run pre-commit run --files models/world_wb_mides/mg/schema.yml

The second step is not optional bookkeeping: pre-commit reflows the long
description lines, so generating alone leaves a diff that the hook will rewrite.
Generate-then-format round-trips to a fixed point, so running both is idempotent.
"""

from __future__ import annotations

import os
import pathlib
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# pyrefly: ignore [missing-import]  # sibling module via sys.path
import mg_column_glossary as glossary

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
MG = os.path.join(ROOT, "mg")
OUT = os.path.join(MG, "schema.yml")

SUFFIX = (
    "Publicado apenas para Minas Gerais, a partir dos arquivos do SICOM/TCE-MG; "
    "a Coverage da tabela registra sigla_uf = MG."
)

TABLES: dict[str, str] = {
    "alteracao_orcamentaria": "Alterações orçamentárias (créditos adicionais, suplementações, anulações e transposições) autorizadas por decreto municipal.",
    "contrato": "Contratos administrativos firmados pelos municípios, com objeto, vigência, signatário e valores empenhados, liquidados e pagos.",
    "contrato_apostilamento": "Apostilamentos registrados sobre contratos administrativos, com o respectivo valor e data.",
    "contrato_contabilizacao": "Vínculo entre cada contrato e os empenhos que o executam orçamentariamente.",
    "contrato_credito": "Créditos orçamentários (dotações) vinculados a cada contrato administrativo.",
    "contrato_item": "Itens contratados em cada contrato administrativo, com quantidade, unidade de medida e preço unitário.",
    "contrato_rescisao": "Rescisões de contratos administrativos, com motivo, data e valor rescindido.",
    "contrato_termo_aditivo": "Termos aditivos de contratos administrativos, com tipo, vigência e valor acrescido ou reduzido.",
    "contrato_termo_aditivo_item": "Itens alterados por termo aditivo, com quantidade e valor acrescidos ou reduzidos.",
    "decreto": "Decretos municipais que autorizam alterações orçamentárias.",
    "despesa_dotacao": "Dotações orçamentárias da despesa, com classificação funcional-programática e fonte de recursos.",
    "dispensa": "Processos de dispensa e inexigibilidade de licitação, com objeto, natureza e fundamentação.",
    "dispensa_cotacao": "Cotações de preço coletadas em cada processo de dispensa de licitação.",
    "dispensa_credenciado": "Fornecedores credenciados em processos de credenciamento.",
    "dispensa_dotacao": "Dotações orçamentárias vinculadas a cada processo de dispensa de licitação.",
    "dispensa_fornecedor": "Fornecedores contratados em cada processo de dispensa de licitação.",
    "dispensa_item": "Itens objeto de cada processo de dispensa de licitação.",
    "dispensa_responsavel": "Responsáveis designados para cada processo de dispensa de licitação.",
    "empenho_credor": "Credores vinculados a cada empenho, identificados por CPF ou CNPJ.",
    "empenho_fonte": "Decomposição de cada empenho por fonte de recursos e dotação orçamentária.",
    "lei_decreto": "Leis municipais que autorizam os decretos de alteração orçamentária.",
    "licitacao_comissao": "Membros da comissão de licitação designados em cada processo licitatório.",
    "licitacao_cotacao": "Cotações de preço apresentadas para cada item licitado.",
    "licitacao_dotacao": "Dotações orçamentárias vinculadas a cada processo licitatório.",
    "licitacao_homologacao": "Homologação e adjudicação de cada item licitado, com o vencedor e o valor homologado.",
    "licitacao_julgamento": "Propostas julgadas para cada item licitado, com licitante, valor e classificação.",
    "licitacao_parecer": "Pareceres técnicos e jurídicos emitidos em cada processo licitatório.",
    "licitacao_quadro_societario": "Quadro societário dos participantes de processos licitatórios.",
    "licitacao_responsavel": "Responsáveis designados para cada processo licitatório.",
    "liquidacao_fonte": "Decomposição de cada liquidação por fonte de recursos.",
    "liquidacao_nota_fiscal": "Notas fiscais vinculadas a cada liquidação de despesa.",
    "nota_fiscal": "Notas fiscais recebidas pelos municípios, com emitente, série, chave e valores.",
    "nota_fiscal_item": "Itens discriminados em cada nota fiscal, com quantidade e valor unitário.",
    "pagamento_movimento": "Movimentações bancárias associadas a cada pagamento, com instituição financeira, agência e conta.",
    "registro_preco_adesao": "Adesões a atas de registro de preços gerenciadas por outro órgão.",
    "registro_preco_adesao_cotacao": "Cotações de preço coletadas para cada adesão a ata de registro de preços.",
    "registro_preco_adesao_item": "Itens objeto de cada adesão a ata de registro de preços.",
    "registro_preco_adesao_vencedor": "Fornecedores vencedores em cada adesão a ata de registro de preços.",
    "restos_pagar": "Saldos de restos a pagar processados e não processados inscritos por exercício de origem.",
    "restos_pagar_credor": "Credores vinculados a cada inscrição de restos a pagar.",
    "restos_pagar_movimentacao": "Movimentações de restos a pagar: pagamentos, anulações, cancelamentos e outras baixas.",
    "restos_pagar_movimentacao_credor": "Credores vinculados a cada movimentação de restos a pagar.",
    "restos_pagar_movimentacao_fonte": "Decomposição de cada movimentação de restos a pagar por fonte de recursos.",
}

DIRECTORY_TESTS: dict[str, tuple[str, str]] = {
    "ano": ("br_bd_diretorios_data_tempo__ano", "ano.ano"),
    "id_municipio": ("br_bd_diretorios_brasil__municipio", "id_municipio"),
    "sigla_uf": ("br_bd_diretorios_brasil__uf", "sigla"),
}


def columns_of(path: str) -> list[str]:
    """Published column names, in the model's own order.

    DEPTH-AWARE ON PURPOSE. `sqlfmt` (via pre-commit) reflows a long
    `safe_cast(concat(...)) as id_x_bd,` across a dozen lines, so a line-anchored
    regex silently under-counts and the caller's own-key assertion is the only
    thing that notices. Parse by parenthesis depth instead: capture the final
    top-level select list, split it on depth-zero commas, and take each item's
    trailing alias.
    """
    text = pathlib.Path(path).read_text(encoding="utf-8")
    lines = [line.split("--")[0] for line in text.split("\n")]

    # Find the final top-level `select` and the `from` that closes it.
    depth = 0
    start = end = None
    for i, line in enumerate(lines):
        stripped = line.strip()
        if depth == 0 and stripped == "select":
            start, end = i + 1, None
        elif (
            depth == 0
            and start is not None
            and end is None
            # sqlfmt puts a bare `from` on its own line
            and (stripped == "from" or stripped.startswith("from "))
        ):
            end = i
        depth += line.count("(") - line.count(")")
    if start is None or end is None:
        raise AssertionError(f"{path}: could not locate the final select list")

    # Split that block on depth-zero commas.
    items, buf, depth = [], [], 0
    for line in lines[start:end]:
        for char in line:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            if char == "," and depth == 0:
                items.append("".join(buf))
                buf = []
                continue
            buf.append(char)
        buf.append(" ")
    if "".join(buf).strip():
        items.append("".join(buf))

    names = []
    for item in items:
        match = re.search(r"\bas\s+([a-z_0-9]+)\s*$", item.strip())
        if match:
            names.append(match.group(1))
    return names


def block(table: str, columns: list[str]) -> list[str]:
    key = f"id_{table}_bd"
    if key not in columns:
        raise AssertionError(f"{table}: own key {key} is not among {columns}")
    out = [
        f"  - name: world_wb_mides__{table}",
        "    description: >",
        f"      {TABLES[table]} {SUFFIX}",
        "    tests:",
        "      - dbt_utils.unique_combination_of_columns:",
        f"          combination_of_columns: [ano, {key}]",
        "      - not_null_proportion_multiple_columns:",
        "          at_least: 0.05",
        "          config:",
        "            where: __most_recent_year__",
        "    columns:",
    ]
    for column in columns:
        out.append(f"      - name: {column}")
        out.append(
            f"        description: {glossary.build_description(column, table)}"
        )
        tests: list[str] = []
        if column in ("ano", "sigla_uf", "id_municipio", key):
            tests.append("not_null")
        if column in DIRECTORY_TESTS:
            out.append("        tests:")
            for t in tests:
                out.append(f"          - {t}")
            ref, field = DIRECTORY_TESTS[column]
            out.append("          - relationships:")
            out.append(f"              to: ref('{ref}')")
            out.append(f"              field: {field}")
        elif tests:
            out.append(f"        tests: [{', '.join(tests)}]")
    return out


def main() -> None:
    files = sorted(f for f in os.listdir(MG) if f.endswith(".sql"))
    lines = [
        "---",
        "# GENERATED by code/gen_mg_schema.py -- edit the glossary or TABLES there,",
        "# then re-run. Hand edits here are overwritten.",
        "version: 2",
        "models:",
    ]
    for fn in files:
        table = fn[len("world_wb_mides__") : -len(".sql")]
        if table not in TABLES:
            raise AssertionError(f"no description for table {table}")
        lines += block(table, columns_of(os.path.join(MG, fn)))
    with open(OUT, "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")
    missed = glossary.unresolved()
    print(f"wrote {OUT}: {len(files)} models")
    print("columns with a mechanical fallback description:", missed or "none")


if __name__ == "__main__":
    main()
