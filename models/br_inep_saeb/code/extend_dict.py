# -*- coding: utf-8 -*-
# Script para alterar o formato da cobertura temporal do dicionario saeb
# O formato será expandido, cada linha será um ano
import itertools
import os
import re

import basedosdados as bd
import pandas as pd

ROOT = os.path.join("models", "br_inep_saeb")
INPUT = os.path.join(ROOT, "input")
OUTPUT = os.path.join(ROOT, "output")

os.makedirs(OUTPUT, exist_ok=True)

df = pd.read_csv(
    os.path.join(INPUT, "staging_br_inep_saeb_dicionario_dicionario.csv")
)

df = df.loc[
    (df["cobertura_temporal"] != "1") & (df["cobertura_temporal"] != "D"),
]


def parse_temporal_coverage(temporal_coverage: str) -> list[dict[str, int]]:
    def parse_common(value: str) -> dict[str, int]:
        # single value
        # (y)
        if value[0] == "(":
            return dict(temporal_unit=int(value[1]))

        # single year
        if len(value) == 4:
            return dict(single_year=int(value))

        # x(y) or x(y)z
        if "(" in value:
            pattern_temporal_unit = r"\((\d+)\)"
            # Split and drop empty strings
            parts: list[str] = [
                i for i in re.split(pattern_temporal_unit, value) if len(i) > 0
            ]

            assert len(parts) <= 3, f"Error: {temporal_coverage=}"

            # x(y), 2005(2)
            if len(parts) == 2:
                return dict(
                    start_year=int(parts[0]), temporal_unit=int(parts[1])
                )

            return dict(
                start_year=int(parts[0]),
                temporal_unit=int(parts[1]),
                end_year=int(parts[2]),
            )

        raise Exception(f"Failed to parse {temporal_coverage=}")

    if "," in temporal_coverage:
        return [parse_common(i.strip()) for i in temporal_coverage.split(",")]
    else:
        return [parse_common(temporal_coverage)]


# Examples:
# {'start_year': 2013, 'temporal_unit': 2, 'end_year': 2017}
def build_date_range(
    temporal_coverage: dict[str, int], start_year: int, latest_year: int
):
    if (
        "start_year" in temporal_coverage
        and "temporal_unit" in temporal_coverage
        and "end_year" in temporal_coverage
    ):
        return list(
            range(
                temporal_coverage["start_year"],
                temporal_coverage["end_year"]
                + temporal_coverage["temporal_unit"],
                temporal_coverage["temporal_unit"],
            )
        )
    elif (
        "start_year" in temporal_coverage
        and "temporal_unit" in temporal_coverage
    ):
        return list(
            range(
                temporal_coverage["start_year"],
                latest_year + temporal_coverage["temporal_unit"],
                temporal_coverage["temporal_unit"],
            )
        )
    elif "temporal_unit" in temporal_coverage:
        return list(
            range(
                start_year,
                latest_year + temporal_coverage["temporal_unit"],
                temporal_coverage["temporal_unit"],
            )
        )
    elif "single_year" in temporal_coverage:
        return [temporal_coverage["single_year"]]


dfs = dict(
    [
        # Table id is wrong
        (table_id.replace("aluno_ef_2_ano", "aluno_ef_2ano"), df_by_table)
        for (table_id, df_by_table) in df.groupby("id_tabela")
    ]
)

backend = bd.Backend(
    graphql_url="https://staging.backend.basedosdados.org/api/v1/graphql"
)


def transform_df(table_id: str, df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    table_slug = backend._get_table_id_from_name(
        gcp_dataset_id="br_inep_saeb", gcp_table_id=table_id
    )
    if not isinstance(table_slug, str):
        raise Exception(f"Not found slug fo {table_id=}")

    response = backend._execute_query(
        query="""
    query($table_id: ID) {
    allTable(id: $table_id) {
        edges {
        node {
            name,
            coverages {
            edges {
                node {
                datetimeRanges {
                    edges {
                    node {
                        id,
                        startYear,
                        endYear
                    }
                    }
                }
                }
            }
            }
        }
        }
    }
    }

    """,
        variables={"table_id": table_slug},
    )

    payload = backend._simplify_graphql_response(response)["allTable"][0][
        "coverages"
    ][0]["datetimeRanges"][0]

    latest_year = payload["endYear"]
    start_year = payload["startYear"]

    d["temporal_coverage_parsed"] = d["cobertura_temporal"].apply(
        lambda x: list(
            itertools.chain(
                *[  # type: ignore
                    build_date_range(
                        i, start_year=start_year, latest_year=latest_year
                    )
                    for i in parse_temporal_coverage(x)
                ]
            )
        )
    )
    return d


new_dict = {
    table_id: transform_df(table_id, df_by_table)
    for (table_id, df_by_table) in dfs.items()
}

OUTPUT_FILE = os.path.join(OUTPUT, "dicionario.csv")

dict_output = (
    pd.concat(new_dict.values())
    .drop(columns=["cobertura_temporal"])
    .explode("temporal_coverage_parsed")
    .rename(
        columns={"temporal_coverage_parsed": "cobertura_temporal"},
        errors="raise",
    )
)

dict_output["id_tabela"].unique()

dict_output["id_tabela"] = dict_output["id_tabela"].replace(
    {"aluno_ef_2_ano": "aluno_ef_2ano"}
)

dict_output.to_csv(OUTPUT_FILE, index=False)

tb = bd.Table(dataset_id="br_inep_saeb", table_id="dicionario")

tb.create(
    OUTPUT_FILE,
    if_table_exists="replace",
    if_storage_data_exists="replace",
)
