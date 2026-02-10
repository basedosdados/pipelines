import gc
from pathlib import Path

import basedosdados as bd  # type: ignore
import numpy as np
import pandas as pd
import tqdm

df_municipio = bd.read_sql(
    "SELECT id_municipio, id_municipio_6, sigla_uf FROM `basedosdados.br_bd_diretorios_brasil.municipio`",
    billing_project_id="basedosdados",
    reauth=False,
)


def to_partitions(
    data: pd.DataFrame,
    partition_columns: list[str],
    savepath: str,
    file_type: str = "csv",
):
    if isinstance(data, (pd.core.frame.DataFrame)):
        savepath = Path(savepath)
        # create unique combinations between partition columns
        unique_combinations = (
            data[partition_columns]
            # .astype(str)
            .drop_duplicates(subset=partition_columns)
            .to_dict(orient="records")
        )

        for filter_combination in unique_combinations:
            patitions_values = [
                f"{partition}={value}"
                for partition, value in filter_combination.items()
            ]

            # get filtered data
            df_filter = data.loc[
                data[filter_combination.keys()]
                .isin(filter_combination.values())
                .all(axis=1),
                :,
            ]
            df_filter = df_filter.drop(columns=partition_columns)

            # create folder tree
            filter_save_path = Path(savepath / "/".join(patitions_values))
            filter_save_path.mkdir(parents=True, exist_ok=True)

            if file_type == "csv":
                # append data to csv
                file_filter_save_path = Path(filter_save_path) / "data.csv"
                df_filter.to_csv(
                    file_filter_save_path,
                    sep=",",
                    encoding="utf-8",
                    na_rep="",
                    index=False,
                    mode="a",
                    header=not file_filter_save_path.exists(),
                )
            elif file_type == "parquet":
                # append data to parquet
                file_filter_save_path = Path(filter_save_path) / "data.parquet"
                df_filter.to_parquet(
                    file_filter_save_path, index=False, compression="gzip"
                )
    else:
        raise BaseException("Data need to be a pandas DataFrame")


def load_and_process_rais_vinculos(
    input,
    ano,
    partition_columns,
    savepath="tmp/Vinculos/output",
    file_type="csv",
):
    valor = 0

    for df in tqdm.tqdm(
        pd.read_csv(
            input,
            sep=",",
            encoding="latin1",
            low_memory=False,
            chunksize=1000000,
            dtype=str,
        )
    ):
        valor = valor + 1
        print(f"Quantidade: {valor}")

        df = df.rename(
            columns={
                "Tipo Vínculo": "tipo_vinculo",
                "Tipo Vínculo - Código": "tipo_vinculo",
                "Vínculo Ativo 31/12": "vinculo_ativo_3112",
                "Ind Vínculo Ativo 31/12 - Código": "vinculo_ativo_3112",
                "Tipo Admissão": "tipo_admissao",
                "Tipo Admissão Trabalhador - Código": "tipo_admissao",
                "Mês Admissão": "mes_admissao",
                "Mês Admissão - Código": "mes_admissao",
                "Mês Desligamento": "mes_desligamento",
                "Mês Desligamento - Código": "mes_desligamento",
                "Motivo Desligamento": "motivo_desligamento",
                "Motivo Desligamento - Código": "motivo_desligamento",
                "Causa Afastamento 1": "causa_desligamento_1",
                "Causa Afastamento 1 - Código": "causa_desligamento_1",
                "Causa Afastamento 2": "causa_desligamento_2",
                "Causa Afastamento 2 - Código": "causa_desligamento_2",
                "Causa Afastamento 3": "causa_desligamento_3",
                "Causa Afastamento 3 - Código": "causa_desligamento_3",
                "Faixa Tempo Emprego": "faixa_tempo_emprego",
                "Faixa Tempo Emprego - Código": "faixa_tempo_emprego",
                "Tempo Emprego": "tempo_emprego",
                "Faixa Hora Contrat": "faixa_horas_contratadas",
                "Faixa Hora Contrat - Código": "faixa_horas_contratadas",
                "Qtd Hora Contr": "quantidade_horas_contratadas",
                "Mun Trab": "id_municipio_trabalho",
                "Município Trab - Código": "id_municipio_trabalho",
                "Qtd Dias Afastamento": "quantidade_dias_afastamento",
                "Ind CEI Vinculado": "indicador_cei_vinculado",
                "Ind CEI Vinculado - Código": "indicador_cei_vinculado",
                "Ind Trab Parcial": "indicador_trabalho_parcial",
                "Ind Trabalho Parcial - Código": "indicador_trabalho_parcial",
                "Ind Trab Intermitente": "indicador_trabalho_intermitente",
                "Ind Trabalho Intermitente - Código": "indicador_trabalho_intermitente",
                "Faixa Remun Média (SM)": "faixa_remuneracao_media_sm",
                "Faixa Rem Média (SM) - Código": "faixa_remuneracao_media_sm",
                "Vl Remun Média (SM)": "valor_remuneracao_media_sm",
                "Vl Rem Média (SM)": "valor_remuneracao_media_sm",
                "Vl Remun Média Nom": "valor_remuneracao_media",
                "Vl Rem Média Nom": "valor_remuneracao_media",
                "Faixa Remun Dezem (SM)": "faixa_remuneracao_dezembro_sm",
                "Faixa Rem Dez (SM) - Código": "faixa_remuneracao_dezembro_sm",
                "Vl Remun Dezembro (SM)": "valor_remuneracao_dezembro_sm",
                "Vl Rem Dezembro (SM)": "valor_remuneracao_dezembro_sm",
                "Vl Rem Janeiro SC": "valor_remuneracao_janeiro",
                "Vl Rem Fevereiro SC": "valor_remuneracao_fevereiro",
                "Vl Rem Março SC": "valor_remuneracao_marco",
                "Vl Rem Abril SC": "valor_remuneracao_abril",
                "Vl Rem Maio SC": "valor_remuneracao_maio",
                "Vl Rem Junho SC": "valor_remuneracao_junho",
                "Vl Rem Julho SC": "valor_remuneracao_julho",
                "Vl Rem Agosto SC": "valor_remuneracao_agosto",
                "Vl Rem Setembro SC": "valor_remuneracao_setembro",
                "Vl Rem Outubro SC": "valor_remuneracao_outubro",
                "Vl Rem Novembro SC": "valor_remuneracao_novembro",
                "Vl Remun Dezembro Nom": "valor_remuneracao_dezembro",
                "Vl Rem Dezembro Nom": "valor_remuneracao_dezembro",
                "CBO Ocupação 2002": "cbo_2002",
                "CBO 2002 Ocupação - Código": "cbo_2002",
                "Faixa Etária": "faixa_etaria",
                "Faixa Etária - Código": "faixa_etaria",
                "Idade": "idade",
                "Escolaridade após 2005": "grau_instrucao_apos_2005",
                "Escolaridade Após 2005 - Código": "grau_instrucao_apos_2005",
                "Nacionalidade": "nacionalidade",
                "Nacionalidade - Código": "nacionalidade",
                "Sexo Trabalhador": "sexo",
                "Sexo - Código": "sexo",
                "Raça Cor": "raca_cor",
                "Raça Cor - Código": "raca_cor",
                "Ind Portador Defic": "indicador_portador_deficiencia",
                "Ind Portador Defic - Código": "indicador_portador_deficiencia",
                "Tipo Defic": "tipo_deficiencia",
                "Tipo Deficiência - Código": "tipo_deficiencia",
                "Ano Chegada Brasil": "ano_chegada_brasil",
                "IBGE Subsetor": "subsetor_ibge",
                "IBGE Subsetor - Código": "subsetor_ibge",
                "CNAE 95 Classe": "cnae_1",
                "CNAE 95 Classe - Código": "cnae_1",
                "CNAE 2.0 Classe": "cnae_2",
                "CNAE 2.0 Classe - Código": "cnae_2",
                "CNAE 2.0 Subclasse": "cnae_2_subclasse",
                "CNAE 2.0 Subclasse - Código": "cnae_2_subclasse",
                "Tamanho Estabelecimento": "tamanho_estabelecimento",
                "Tamanho Estabelecimento - Código": "tamanho_estabelecimento",
                "Tipo Estab": "tipo_estabelecimento",
                "Tipo Estabelecimento - Código": "tipo_estabelecimento",
                "Natureza Jurídica": "natureza_juridica",
                "Natureza Jurídica - Código": "natureza_juridica",
                "Ind Simples": "indicador_simples",
                "Ind Estabelecimento Participante SIMPLES - Código": "indicador_simples",
                "Bairros SP": "bairros_sp",
                "Bairros SP - Código": "bairros_sp",
                "Distritos SP": "distritos_sp",
                "Distritos SP - Código": "distritos_sp",
                "Bairros Fortaleza": "bairros_fortaleza",
                "Bairros Fortaleza - Código": "bairros_fortaleza",
                "Bairros RJ": "bairros_rj",
                "Bairros RJ - Código": "bairros_rj",
                "Regiões Adm DF": "regioes_administrativas_df",
                "Região Adm DF - Código": "regioes_administrativas_df",
                "Município": "municipio",
                "Município - Código": "municipio",
            },
        )

        df["ano"] = ano

        df[["municipio", "id_municipio_trabalho"]] = df[
            ["municipio", "id_municipio_trabalho"]
        ].astype(str)

        # Mescla com o arquivo de municípios

        df = df.merge(
            df_municipio,
            left_on=["municipio"],
            right_on=["id_municipio_6"],
            how="left",
        )

        df = df.merge(
            df_municipio,
            left_on=["id_municipio_trabalho"],
            right_on=["id_municipio_6"],
            how="left",
        )

        df = df.drop(
            [
                "id_municipio_trabalho",
                "municipio",
                "id_municipio_6_x",
                "id_municipio_6_y",
                "sigla_uf_y",
            ],
            axis=1,
        )

        df = df.rename(
            columns={
                "id_municipio_x": "id_municipio",
                "sigla_uf_x": "sigla_uf",
                "id_municipio_y": "id_municipio_trabalho",
            },
        )

        df["sigla_uf"] = df["sigla_uf"].replace([np.nan, "NI"], "IGNORADO")

        vars_list = [
            "ano",
            "sigla_uf",
            "id_municipio",
            "tipo_vinculo",
            "vinculo_ativo_3112",
            "tipo_admissao",
            "mes_admissao",
            "mes_desligamento",
            "motivo_desligamento",
            "causa_desligamento_1",
            "causa_desligamento_2",
            "causa_desligamento_3",
            "faixa_tempo_emprego",
            "tempo_emprego",
            "faixa_horas_contratadas",
            "quantidade_horas_contratadas",
            "id_municipio_trabalho",
            "quantidade_dias_afastamento",
            "indicador_cei_vinculado",
            "indicador_trabalho_parcial",
            "indicador_trabalho_intermitente",
            "faixa_remuneracao_media_sm",
            "valor_remuneracao_media_sm",
            "valor_remuneracao_media",
            "faixa_remuneracao_dezembro_sm",
            "valor_remuneracao_dezembro_sm",
            "valor_remuneracao_janeiro",
            "valor_remuneracao_fevereiro",
            "valor_remuneracao_marco",
            "valor_remuneracao_abril",
            "valor_remuneracao_maio",
            "valor_remuneracao_junho",
            "valor_remuneracao_julho",
            "valor_remuneracao_agosto",
            "valor_remuneracao_setembro",
            "valor_remuneracao_outubro",
            "valor_remuneracao_novembro",
            "valor_remuneracao_dezembro",
            "tipo_salario",
            "valor_salario_contratual",
            "subatividade_ibge",
            "subsetor_ibge",
            "cbo_1994",
            "cbo_2002",
            "cnae_1",
            "cnae_2",
            "cnae_2_subclasse",
            "faixa_etaria",
            "idade",
            "grau_instrucao_1985_2005",
            "grau_instrucao_apos_2005",
            "nacionalidade",
            "sexo",
            "raca_cor",
            "indicador_portador_deficiencia",
            "tipo_deficiencia",
            "ano_chegada_brasil",
            "tamanho_estabelecimento",
            "tipo_estabelecimento",
            "natureza_juridica",
            "indicador_simples",
            "bairros_sp",
            "distritos_sp",
            "bairros_fortaleza",
            "bairros_rj",
            "regioes_administrativas_df",
        ]

        for var in vars_list:
            if var not in df.columns:
                df[var] = ""

        df = df[vars_list]

        # Limpeza de variáveis
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        # Limpeza de códigos inválidos

        invalid_codes_bairros = [
            "0000",
            "00000",
            "000000",
            "0000000",
            "0000-1",
            "000-1",
            "9999",
            "9997",
        ]
        for col in [
            "bairros_rj",
            "bairros_sp",
            "bairros_fortaleza",
            "distritos_sp",
            "regioes_administrativas_df",
        ]:
            df[col] = df[col].replace(invalid_codes_bairros, "")

        # Mais substituições de códigos

        invalid_codes_general = [
            "0000",
            "00000",
            "000000",
            "0000000",
            "0000-1",
            "000-1",
        ]

        for col in [
            "cbo_1994",
            "cbo_2002",
            "cnae_1",
            "cnae_2",
            "cnae_2_subclasse",
            "ano_chegada_brasil",
        ]:
            df[col] = df[col].replace(invalid_codes_general, "")

        df["mes_admissao"] = df["mes_admissao"].replace("00", "")

        df["mes_desligamento"] = df["mes_desligamento"].replace("00", "")

        df["motivo_desligamento"] = df["motivo_desligamento"].replace("0", "")

        df["causa_desligamento_1"] = df["causa_desligamento_1"].replace(
            "99", ""
        )

        df["raca_cor"] = df["raca_cor"].replace("99", "9")

        # Ajustes adicionais

        df["natureza_juridica"] = df["natureza_juridica"].replace(
            ["9990", "9999"], ""
        )

        df["tipo_estabelecimento"] = df["tipo_estabelecimento"].replace(
            ["CNPJ", "Cnpj", "01", "1"], "1"
        )

        df["tipo_estabelecimento"] = df["tipo_estabelecimento"].replace(
            "CAEPF", "2"
        )

        df["tipo_estabelecimento"] = df["tipo_estabelecimento"].replace(
            ["CEI", "Cei", "CEI/CNO", "Cei/Cno", "CNO", "Cno", "03", "3"], "3"
        )

        # Conversão de valores monetários

        monetary_vars = [
            "tempo_emprego",
            "valor_remuneracao_janeiro",
            "valor_remuneracao_fevereiro",
            "valor_remuneracao_marco",
            "valor_remuneracao_abril",
            "valor_remuneracao_maio",
            "valor_remuneracao_junho",
            "valor_remuneracao_julho",
            "valor_remuneracao_agosto",
            "valor_remuneracao_setembro",
            "valor_remuneracao_outubro",
            "valor_remuneracao_novembro",
            "valor_remuneracao_dezembro",
            # "valor_salario_contratual",
            "valor_remuneracao_dezembro_sm",
            "valor_remuneracao_media",
            "valor_remuneracao_media_sm",
        ]

        for var in monetary_vars:
            df[var] = (
                df[var]
                .astype(str)  # Converte a coluna para string
                .str.replace(",", ".", regex=False)
                .replace("n/d", np.nan)  # Substitui vírgulas por pontos
                .astype(float)  # Converte para float
            )

        to_partitions(
            data=df,
            partition_columns=partition_columns,
            savepath=savepath,
            file_type=file_type,
        )

        del df

        gc.collect()


load_and_process_rais_vinculos(
    input="tmp/input/RAIS_VINC_PUB_SP/RAIS_VINC_PUB_SP.COMT",
    partition_columns=["ano", "sigla_uf"],
    ano="2023",
)
