"""Build the br_sfb_sicar dictionary parquet (all-string) → OUTPUT/dicionario/.

The dictionary maps every coded column's key to its label, per table:

* ``status`` (AT/PE/SU/CA) for every theme table;
* ``tipo`` — for ``area_imovel`` the property type (IRU/AST/PCT), and for each
  overlay theme the ``cod_tema`` codes.

The overlay ``tipo`` labels are the SICAR national vocabulary (fixed across UFs;
their ``cod_tema -> nom_tema`` in the source shapefiles). They are embedded here,
accent-corrected to proper Portuguese (the source ``nom_tema`` is ASCII-stripped),
so the generator is reproducible without re-reading every state's zip. The set was
harvested from the shapefiles and verified to match — exactly, zero gaps — the
distinct ``tipo`` values across all 27 UFs in the materialized tables.
"""

import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

SCRATCH = Path(
    os.environ.get(
        "CAR_DATA", str(Path.home() / "Downloads" / "br_sfb_sicar_data")
    )
)
OUTPUT = SCRATCH / "output"

STATUS = {"AT": "Ativo", "PE": "Pendente", "SU": "Suspenso", "CA": "Cancelado"}

TIPO_IMOVEL = {
    "IRU": "Imóvel Rural",
    "AST": "Assentamento de Reforma Agrária",
    "PCT": "Povos e Comunidades Tradicionais",
}

# Overlay-theme `tipo` codes -> label (SICAR national vocabulary).
TIPO_OVERLAY: dict[str, dict[str, str]] = {
    "app": {
        "APP_AREA_AC": "Área de Preservação Permanente em área consolidada",
        "APP_AREA_ALTITUDE_SUPERIOR_1800": "Área de Preservação Permanente de áreas com altitude superior a 1.800 metros",
        "APP_AREA_DECLIVIDADE_MAIOR_45": "Área de Preservação Permanente de áreas com declividade superior a 45 graus",
        "APP_AREA_TOPO_MORRO": "Área de Preservação Permanente de topo de morro",
        "APP_AREA_VN": "Área de Preservação Permanente em área de vegetação nativa",
        "APP_BANHADO": "Área de Preservação Permanente de banhado",
        "APP_BORDA_CHAPADA": "Área de Preservação Permanente de borda de chapada",
        "APP_ESCADINHA": "Área de Preservação Permanente a recompor segundo o art. 61-A da Lei nº 12.651/2012",
        "APP_ESCADINHA_LAGO_NATURAL": "Área de Preservação Permanente a recompor de lagos e lagoas naturais",
        "APP_ESCADINHA_NASCENTE_OLHO_DAGUA": "Área de Preservação Permanente a recompor de nascentes ou olhos d'água perenes",
        "APP_ESCADINHA_RIO_10_A_50": "Área de Preservação Permanente a recompor de rios de 10 a 50 metros",
        "APP_ESCADINHA_RIO_200_A_600": "Área de Preservação Permanente a recompor de rios de 200 a 600 metros",
        "APP_ESCADINHA_RIO_50_A_200": "Área de Preservação Permanente a recompor de rios de 50 a 200 metros",
        "APP_ESCADINHA_RIO_ACIMA_600": "Área de Preservação Permanente a recompor de rios com mais de 600 metros",
        "APP_ESCADINHA_RIO_ATE_10": "Área de Preservação Permanente a recompor de rios de até 10 metros",
        "APP_ESCADINHA_VEREDA": "Área de Preservação Permanente a recompor de veredas",
        "APP_LAGO_NATURAL": "Área de Preservação Permanente de lagos e lagoas naturais",
        "APP_MANGUEZAL": "Área de Preservação Permanente de manguezais",
        "APP_NASCENTE_OLHO_DAGUA": "Área de Preservação Permanente de nascentes ou olhos d'água perenes",
        "APP_RESERVATORIO_ARTIFICIAL_DECORRENTE_BARRAMENTO": "Área de Preservação Permanente de reservatório artificial decorrente de barramento de cursos d'água",
        "APP_RESTINGA": "Área de Preservação Permanente de restingas",
        "APP_RIO_10_A_50": "Área de Preservação Permanente de rios de 10 a 50 metros",
        "APP_RIO_200_A_600": "Área de Preservação Permanente de rios de 200 a 600 metros",
        "APP_RIO_50_A_200": "Área de Preservação Permanente de rios de 50 a 200 metros",
        "APP_RIO_ACIMA_600": "Área de Preservação Permanente de rios com mais de 600 metros",
        "APP_RIO_ATE_10": "Área de Preservação Permanente de rios de até 10 metros",
        "APP_TOTAL": "Área de Preservação Permanente total",
        "APP_VAZIO": "Área de Preservação Permanente em área antropizada não declarada como área consolidada",
        "APP_VEREDA": "Área de Preservação Permanente de veredas",
        "AREA_ALTITUDE_SUPERIOR_1800": "Área com altitude superior a 1.800 metros",
        "AREA_DECLIVIDADE_MAIOR_45": "Área de declividade maior que 45 graus",
        "AREA_TOPO_MORRO": "Área de topo de morro",
        "BANHADO": "Banhado",
        "BORDA_CHAPADA": "Borda de chapada",
        "MANGUEZAL": "Manguezal",
        "RESTINGA": "Restinga",
        "VEREDA": "Vereda",
    },
    "reserva_legal": {
        "ARL_APROVADA_NAO_AVERBADA": "Reserva Legal aprovada e não averbada",
        "ARL_AVERBADA": "Reserva Legal averbada",
        "ARL_PROPOSTA": "Reserva Legal proposta",
    },
    "vegetacao_nativa": {
        "VEGETACAO_NATIVA": "Remanescente de vegetação nativa",
    },
    "area_consolidada": {
        "AREA_CONSOLIDADA": "Área consolidada",
    },
    "area_pousio": {
        "AREA_POUSIO": "Área de pousio",
    },
    "uso_restrito": {
        "AREA_USO_RESTRITO_DECLIVIDADE_25_A_45": "Área de uso restrito para declividade de 25 a 45 graus",
        "AREA_USO_RESTRITO_PANTANEIRA": "Área de uso restrito para regiões pantaneiras",
    },
    "servidao_administrativa": {
        "AREA_ENTORNO_RESERVATORIO_ENERGIA": "Entorno de reservatório para abastecimento ou geração de energia",
        "AREA_INFRAESTRUTURA_PUBLICA": "Infraestrutura pública",
        "AREA_SERVIDAO_ADMINISTRATIVA_TOTAL": "Área de servidão administrativa total",
        "AREA_UTILIDADE_PUBLICA": "Utilidade pública",
        "RESERVATORIO_ENERGIA": "Reservatório para abastecimento ou geração de energia",
    },
    "hidrografia": {
        "LAGO_NATURAL": "Lago ou lagoa natural",
        "RESERVATORIO_ARTIFICIAL_DECORRENTE_BARRAMENTO": "Reservatório artificial decorrente de barramento ou represamento de cursos d'água naturais",
        "RIO_10_A_50": "Curso d'água natural de 10 a 50 metros",
        "RIO_200_A_600": "Curso d'água natural de 200 a 600 metros",
        "RIO_50_A_200": "Curso d'água natural de 50 a 200 metros",
        "RIO_ACIMA_600": "Curso d'água natural acima de 600 metros",
        "RIO_ATE_10": "Curso d'água natural de até 10 metros",
    },
}

# Every theme table (status coverage is universal).
TABLES = [
    "area_imovel",
    "app",
    "reserva_legal",
    "vegetacao_nativa",
    "area_consolidada",
    "area_pousio",
    "uso_restrito",
    "servidao_administrativa",
    "hidrografia",
]

COLUMNS = ["id_tabela", "nome_coluna", "chave", "cobertura_temporal", "valor"]


def build_rows() -> list[tuple[str, str, str, str, str]]:
    rows: list[tuple[str, str, str, str, str]] = []
    # status for every table
    for table in TABLES:
        for k, v in STATUS.items():
            rows.append((table, "status", k, "", v))
    # tipo: area_imovel property type
    for k, v in TIPO_IMOVEL.items():
        rows.append(("area_imovel", "tipo", k, "", v))
    # tipo: overlay themes
    for table, labels in TIPO_OVERLAY.items():
        for k, v in labels.items():
            rows.append((table, "tipo", k, "", v))
    # de-dup, stable order
    seen: set[tuple[str, str, str, str, str]] = set()
    out: list[tuple[str, str, str, str, str]] = []
    for r in rows:
        if r not in seen:
            seen.add(r)
            out.append(r)
    return out


def main() -> None:
    rows = build_rows()
    root = OUTPUT / "dicionario"
    if root.exists():
        import shutil

        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([(c, pa.string()) for c in COLUMNS])
    table = pa.table(
        {c: [r[i] for r in rows] for i, c in enumerate(COLUMNS)}, schema=schema
    )
    pq.write_table(
        table, str(root / "dicionario.parquet"), compression="snappy"
    )
    print(f"dicionario rows: {len(rows)} -> {root}")


if __name__ == "__main__":
    main()
