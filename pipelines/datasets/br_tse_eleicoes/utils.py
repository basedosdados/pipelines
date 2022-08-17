# -*- coding: utf-8 -*-
"""
General purpose functions for the br_tse_eleicoes project
"""
# pylint: disable=invalid-name,line-too-long
import re

import pandas as pd
from unidecode import unidecode
import basedosdados as bd


def get_id_candidato_bd(df: pd.DataFrame) -> pd.DataFrame:
    """
    Uses nome, cpf and titulo_eleitor to generate an id_candidato_bd
    """

    data = df.copy()

    df[["cpf", "titulo_eleitoral"]] = df[["cpf", "titulo_eleitoral"]].fillna(value=0)

    df["id_candidato_bd"] = (
        df.set_index(["cpf", "titulo_eleitoral"]).index.factorize()[-0] + 1
    )

    df["id_candidato_bd"] = df.groupby(["cpf"], dropna=False).id_candidato_bd.transform(
        "max"
    )
    df["id_candidato_bd"] = df.groupby(
        ["titulo_eleitoral"], dropna=False
    ).id_candidato_bd.transform("max")

    df["id_candidato_bd_str"] = "1 " + df.id_candidato_bd.astype(str)

    aux_cpf = pd.pivot_table(
        data=df, index=["id_candidato_bd"], values=["cpf"], aggfunc=pd.Series.nunique
    )
    aux_te = pd.pivot_table(
        data=df,
        index=["id_candidato_bd"],
        values=["titulo_eleitoral"],
        aggfunc=pd.Series.nunique,
    )

    df["N_cpf"] = df["id_candidato_bd"].map(dict(zip(aux_cpf.index, aux_cpf.cpf)))
    df["N_TE"] = df["id_candidato_bd"].map(
        dict(zip(aux_te.index, aux_te.titulo_eleitoral))
    )

    para_2a_rodada = (
        df[(df["N_cpf"] > 1) | (df["N_TE"] > 1)]
        .sort_values("N_cpf")
        .reset_index(drop=True)
    )

    _1a_rodada = df[(df["N_cpf"] == 1) & (df["N_TE"] == 1)]
    _1a_rodada = _1a_rodada[["id_candidato_bd_str", "cpf", "titulo_eleitoral", "nome"]]

    if para_2a_rodada.shape[0] > 0:
        del df
        df = para_2a_rodada.copy()

        df["nome"] = df["nome"].apply(
            lambda x: unidecode(x) if isinstance(x, str) else x
        )
        df["aux_primeiro"] = df["nome"].apply(
            lambda x: x.split(" ")[0] if isinstance(x, str) else x
        )
        df["aux_ultimo"] = df["nome"].apply(
            lambda x: x.split(" ")[-1] if isinstance(x, str) else x
        )

        aux_nomes = pd.pivot_table(
            data=df,
            index=["id_candidato_bd_str"],
            values=["aux_primeiro", "aux_ultimo"],
            aggfunc=pd.Series.nunique,
        )

        df["N_nomes"] = df["id_candidato_bd_str"].map(
            dict(zip(aux_nomes.index, aux_nomes.aux_primeiro))
        )

        # not actually used. See https://github.com/basedosdados/mais/blob/master/bases/br_tse_eleicoes/code/sub/cria_id_candidato.do
        # pylint: disable=unused-variable
        para_3a_rodada = (
            df[(df["N_nomes"] > 1)].sort_values("N_nomes").reset_index(drop=True)
        )

        df = df[(df["N_nomes"] == 1)]

        df["aux_id"] = (
            df.set_index(
                ["id_candidato_bd_str", "aux_primeiro", "aux_ultimo"]
            ).index.factorize()[-0]
            + 1
        )

        df["id_candidato_bd_str"] = "2 " + df["aux_id"].astype(str)

        df.drop(["aux_id"], axis=1, inplace=True)

        _2a_rodada = df[["id_candidato_bd_str", "cpf", "titulo_eleitoral", "nome"]]

        del df

    if para_2a_rodada.shape[0] > 0:
        df = _1a_rodada.append(_2a_rodada).sort_values(by="cpf").reset_index(drop=True)
    else:
        df = _1a_rodada.copy()

    df["id_candidato_bd"] = (
        df.set_index(["id_candidato_bd_str"]).index.factorize()[-0] + 1
    )
    df.sort_values(by="id_candidato_bd", inplace=True)
    df = df[["id_candidato_bd", "cpf", "titulo_eleitoral", "nome"]]

    data.drop(["id_candidato_bd"], axis=1, inplace=True)

    data = data.merge(df, on=["cpf", "titulo_eleitoral", "nome"], how="left")

    data["id_candidato_bd"] = (
        data["id_candidato_bd"].fillna(value=0).astype(int).replace({0: None})
    )

    data = data.reindex(
        [
            "ano",
            "tipo_eleicao",
            "sigla_uf",
            "id_municipio",
            "id_municipio_tse",
            "id_candidato_bd",
            "cpf",
            "titulo_eleitoral",
            "sequencial",
            "numero",
            "nome",
            "nome_urna",
            "numero_partido",
            "sigla_partido",
            "cargo",
            "situacao",
            "ocupacao",
            "data_nascimento",
            "idade",
            "genero",
            "instrucao",
            "estado_civil",
            "nacionalidade",
            "sigla_uf_nascimento",
            "municipio_nascimento",
            "email",
            "raca",
            "situacao_totalizacao",
            "numero_federacao",
            "nome_federacao",
            "sigla_federacao",
            "composicao_federacao",
            "prestou_contas",
        ],
        axis=1,
    )

    return data


def get_blobs_from_raw(dataset_id: str, table_id: str) -> list:
    """
    Get all blobs from a table in a dataset.
    """

    storage = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    return list(
        storage.client["storage_staging"]
        .bucket("basedosdados-dev")
        .list_blobs(prefix=f"raw/{storage.dataset_id}/{storage.table_id}/")
    )


def get_data_from_prod(dataset_id: str, table_id: str, columns: list) -> list:
    """
    Get select columns from a table in prod.
    """

    storage = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    blobs = list(
        storage.client["storage_staging"]
        .bucket("basedosdados-dev")
        .list_blobs(prefix=f"staging/{storage.dataset_id}/{storage.table_id}/")
    )

    dfs = []

    for blob in blobs:
        partitions = re.findall(r"\w+(?==)", blob.name)
        if len(set(partitions) & set(columns)) == 0:
            df = pd.read_csv(blob.public_url, usecols=columns)
            dfs.append(df)
        else:
            columns2add = list(set(partitions) & set(columns))
            for column in columns2add:
                columns.remove(column)
            df = pd.read_csv(blob.public_url, usecols=columns)
            for column in columns2add:
                df[column] = blob.name.split(column + "=")[1].split("/")[0]
            dfs.append(df)

    df = pd.concat(dfs)

    return df


def normalize_dahis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies normalizations defined bt Ricardo Dahis (see: https://github.com/basedosdados/mais/blob/master/bases/br_tse_eleicoes/code/sub/normalizacao_particao.do)
    """
    df = df[~df.id_candidato_bd.isna()]

    df.drop_duplicates(
        subset=[
            "ano",
            "tipo_eleicao",
            "sigla_uf",
            "id_municipio_tse",
            "numero",
            "cargo",
            "id_candidato_bd",
        ],
        inplace=True,
    )

    df["dup"] = df.duplicated(
        subset=[
            "ano",
            "tipo_eleicao",
            "sigla_uf",
            "id_municipio_tse",
            "numero",
            "cargo",
        ],
        keep=False,
    )

    df = df[(df.dup is not True) | (df.situacao == "deferido")]

    df.drop(columns=["dup"], inplace=True)

    df.drop_duplicates(
        subset=[
            "ano",
            "tipo_eleicao",
            "sigla_uf",
            "id_municipio_tse",
            "numero",
            "cargo",
        ],
        inplace=True,
    )

    df.drop_duplicates(
        subset=["ano", "tipo_eleicao", "id_candidato_bd"],
        inplace=True,
    )

    return df


def clean_digit_id(number: int, n_digits: int) -> str:
    """
    Clean digit id.
    """
    number = str(number)
    number = number.zfill(n_digits)
    number = "".join([i for i in number if i.isdigit()])

    return number
