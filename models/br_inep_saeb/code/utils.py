import pandas as pd

RENAMES_BR = {
    "ANO_SAEB": "ano",
    "DEPENDENCIA_ADM": "rede",
    "LOCALIZACAO": "localizacao",
    "MEDIA_2_LP": "media_lp_2",
    "MEDIA_2_MT": "media_mt_2",
    "MEDIA_5_LP": "media_lp_5",
    "MEDIA_5_MT": "media_mt_5",
    "MEDIA_9_LP": "media_lp_9",
    "MEDIA_9_MT": "media_mt_9",
    "MEDIA_9_CH": "media_ch_9",
    "MEDIA_9_CN": "media_cn_9",
    "MEDIA_12_LP": "media_lp_em",
    "MEDIA_12_MT": "media_mt_em",
    "MEDIA_13_LP": "media_lp_em_integral",
    "MEDIA_13_MT": "media_mt_em_integral",
    "MEDIA_14_LP": "media_lp_em_regular",
    "MEDIA_14_MT": "media_mt_em_regular",
    "nivel_0_LP2": "nivel_0_lp_2",
    "nivel_1_LP2": "nivel_1_lp_2",
    "nivel_2_LP2": "nivel_2_lp_2",
    "nivel_3_LP2": "nivel_3_lp_2",
    "nivel_4_LP2": "nivel_4_lp_2",
    "nivel_5_LP2": "nivel_5_lp_2",
    "nivel_6_LP2": "nivel_6_lp_2",
    "nivel_7_LP2": "nivel_7_lp_2",
    "nivel_8_LP2": "nivel_8_lp_2",
    "nivel_0_MT2": "nivel_0_mt_2",
    "nivel_1_MT2": "nivel_1_mt_2",
    "nivel_2_MT2": "nivel_2_mt_2",
    "nivel_3_MT2": "nivel_3_mt_2",
    "nivel_4_MT2": "nivel_4_mt_2",
    "nivel_5_MT2": "nivel_5_mt_2",
    "nivel_6_MT2": "nivel_6_mt_2",
    "nivel_7_MT2": "nivel_7_mt_2",
    "nivel_8_MT2": "nivel_8_mt_2",
    "nivel_0_LP5": "nivel_0_lp_5",
    "nivel_1_LP5": "nivel_1_lp_5",
    "nivel_2_LP5": "nivel_2_lp_5",
    "nivel_3_LP5": "nivel_3_lp_5",
    "nivel_4_LP5": "nivel_4_lp_5",
    "nivel_5_LP5": "nivel_5_lp_5",
    "nivel_6_LP5": "nivel_6_lp_5",
    "nivel_7_LP5": "nivel_7_lp_5",
    "nivel_8_LP5": "nivel_8_lp_5",
    "nivel_9_LP5": "nivel_9_lp_5",
    "nivel_0_MT5": "nivel_0_mt_5",
    "nivel_1_MT5": "nivel_1_mt_5",
    "nivel_2_MT5": "nivel_2_mt_5",
    "nivel_3_MT5": "nivel_3_mt_5",
    "nivel_4_MT5": "nivel_4_mt_5",
    "nivel_5_MT5": "nivel_5_mt_5",
    "nivel_6_MT5": "nivel_6_mt_5",
    "nivel_7_MT5": "nivel_7_mt_5",
    "nivel_8_MT5": "nivel_8_mt_5",
    "nivel_9_MT5": "nivel_9_mt_5",
    "nivel_10_MT5": "nivel_10_mt_5",
    "nivel_0_LP9": "nivel_0_lp_9",
    "nivel_1_LP9": "nivel_1_lp_9",
    "nivel_2_LP9": "nivel_2_lp_9",
    "nivel_3_LP9": "nivel_3_lp_9",
    "nivel_4_LP9": "nivel_4_lp_9",
    "nivel_5_LP9": "nivel_5_lp_9",
    "nivel_6_LP9": "nivel_6_lp_9",
    "nivel_7_LP9": "nivel_7_lp_9",
    "nivel_8_LP9": "nivel_8_lp_9",
    "nivel_0_MT9": "nivel_0_mt_9",
    "nivel_1_MT9": "nivel_1_mt_9",
    "nivel_2_MT9": "nivel_2_mt_9",
    "nivel_3_MT9": "nivel_3_mt_9",
    "nivel_4_MT9": "nivel_4_mt_9",
    "nivel_5_MT9": "nivel_5_mt_9",
    "nivel_6_MT9": "nivel_6_mt_9",
    "nivel_7_MT9": "nivel_7_mt_9",
    "nivel_8_MT9": "nivel_8_mt_9",
    "nivel_9_MT9": "nivel_9_mt_9",
    "nivel_0_CH9": "nivel_0_ch_9",
    "nivel_1_CH9": "nivel_1_ch_9",
    "nivel_2_CH9": "nivel_2_ch_9",
    "nivel_3_CH9": "nivel_3_ch_9",
    "nivel_4_CH9": "nivel_4_ch_9",
    "nivel_5_CH9": "nivel_5_ch_9",
    "nivel_6_CH9": "nivel_6_ch_9",
    "nivel_7_CH9": "nivel_7_ch_9",
    "nivel_8_CH9": "nivel_8_ch_9",
    "nivel_9_CH9": "nivel_9_ch_9",
    "nivel_0_CN9": "nivel_0_cn_9",
    "nivel_1_CN9": "nivel_1_cn_9",
    "nivel_2_CN9": "nivel_2_cn_9",
    "nivel_3_CN9": "nivel_3_cn_9",
    "nivel_4_CN9": "nivel_4_cn_9",
    "nivel_5_CN9": "nivel_5_cn_9",
    "nivel_6_CN9": "nivel_6_cn_9",
    "nivel_7_CN9": "nivel_7_cn_9",
    "nivel_8_CN9": "nivel_8_cn_9",
    "nivel_0_LP12": "nivel_0_lp_em",
    "nivel_1_LP12": "nivel_1_lp_em",
    "nivel_2_LP12": "nivel_2_lp_em",
    "nivel_3_LP12": "nivel_3_lp_em",
    "nivel_4_LP12": "nivel_4_lp_em",
    "nivel_5_LP12": "nivel_5_lp_em",
    "nivel_6_LP12": "nivel_6_lp_em",
    "nivel_7_LP12": "nivel_7_lp_em",
    "nivel_8_LP12": "nivel_8_lp_em",
    "nivel_0_MT12": "nivel_0_mt_em",
    "nivel_1_MT12": "nivel_1_mt_em",
    "nivel_2_MT12": "nivel_2_mt_em",
    "nivel_3_MT12": "nivel_3_mt_em",
    "nivel_4_MT12": "nivel_4_mt_em",
    "nivel_5_MT12": "nivel_5_mt_em",
    "nivel_6_MT12": "nivel_6_mt_em",
    "nivel_7_MT12": "nivel_7_mt_em",
    "nivel_8_MT12": "nivel_8_mt_em",
    "nivel_9_MT12": "nivel_9_mt_em",
    "nivel_10_MT12": "nivel_10_mt_em",
    "nivel_0_LP13": "nivel_0_lp_em_integral",
    "nivel_1_LP13": "nivel_1_lp_em_integral",
    "nivel_2_LP13": "nivel_2_lp_em_integral",
    "nivel_3_LP13": "nivel_3_lp_em_integral",
    "nivel_4_LP13": "nivel_4_lp_em_integral",
    "nivel_5_LP13": "nivel_5_lp_em_integral",
    "nivel_6_LP13": "nivel_6_lp_em_integral",
    "nivel_7_LP13": "nivel_7_lp_em_integral",
    "nivel_8_LP13": "nivel_8_lp_em_integral",
    "nivel_0_MT13": "nivel_0_mt_em_integral",
    "nivel_1_MT13": "nivel_1_mt_em_integral",
    "nivel_2_MT13": "nivel_2_mt_em_integral",
    "nivel_3_MT13": "nivel_3_mt_em_integral",
    "nivel_4_MT13": "nivel_4_mt_em_integral",
    "nivel_5_MT13": "nivel_5_mt_em_integral",
    "nivel_6_MT13": "nivel_6_mt_em_integral",
    "nivel_7_MT13": "nivel_7_mt_em_integral",
    "nivel_8_MT13": "nivel_8_mt_em_integral",
    "nivel_9_MT13": "nivel_9_mt_em_integral",
    "nivel_10_MT13": "nivel_10_mt_em_integral",
    "nivel_0_LP14": "nivel_0_lp_em_regular",
    "nivel_1_LP14": "nivel_1_lp_em_regular",
    "nivel_2_LP14": "nivel_2_lp_em_regular",
    "nivel_3_LP14": "nivel_3_lp_em_regular",
    "nivel_4_LP14": "nivel_4_lp_em_regular",
    "nivel_5_LP14": "nivel_5_lp_em_regular",
    "nivel_6_LP14": "nivel_6_lp_em_regular",
    "nivel_7_LP14": "nivel_7_lp_em_regular",
    "nivel_8_LP14": "nivel_8_lp_em_regular",
    "nivel_0_MT14": "nivel_0_mt_em_regular",
    "nivel_1_MT14": "nivel_1_mt_em_regular",
    "nivel_2_MT14": "nivel_2_mt_em_regular",
    "nivel_3_MT14": "nivel_3_mt_em_regular",
    "nivel_4_MT14": "nivel_4_mt_em_regular",
    "nivel_5_MT14": "nivel_5_mt_em_regular",
    "nivel_6_MT14": "nivel_6_mt_em_regular",
    "nivel_7_MT14": "nivel_7_mt_em_regular",
    "nivel_8_MT14": "nivel_8_mt_em_regular",
    "nivel_9_MT14": "nivel_9_mt_em_regular",
    "nivel_10_MT14": "nivel_10_mt_em_regular",
}

RENAMES_UFS = {"NO_UF": "nome_uf", **RENAMES_BR}

RENAMES_MUN = {"CO_MUNICIPIO": "id_municipio", **RENAMES_UFS}


def get_disciplina_serie(value: str) -> tuple[str, str]:
    _, disciplina, *rest = value.split("_")

    return (disciplina, "_".join(rest))


def get_nivel_serie_disciplina(value: str) -> tuple[int, str, str]:
    _, nivel_number, disciplina, *rest = value.split("_")
    return (int(nivel_number), disciplina, "_".join(rest))


def convert_to_pd_dtype(type: str) -> str:
    if type == "INTEGER":
        return "int"
    elif type == "STRING":
        return "str"
    elif type == "FLOAT":
        return "float64"
    else:
        raise AssertionError


def drop_empty_lines(df: pd.DataFrame) -> pd.DataFrame:
    return df.copy().loc[
        (df["media"].notna())
        | (df["nivel_0"].notna())
        | (df["nivel_1"].notna())
        | (df["nivel_2"].notna())
        | (df["nivel_3"].notna())
        | (df["nivel_4"].notna())
        | (df["nivel_5"].notna())
        | (df["nivel_6"].notna())
        | (df["nivel_7"].notna())
        | (df["nivel_8"].notna())
        | (df["nivel_9"].notna())
        | (df["nivel_10"].notna())
    ]
