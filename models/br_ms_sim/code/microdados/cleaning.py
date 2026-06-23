import os
import tempfile
from pathlib import Path

import basedosdados as bd
import pandas as pd
from datasus_dbc import decompress as dbc2dbf
from dbfread import DBF

MUNICIPIOS_PATH = "extra/municipios.csv"

DATE_COLUMNS = [
    "data_obito",
    "data_nascimento",
    "data_atestado",
    "data_investigacao",
    "data_cadastro",
    "data_recebimento",
    "data_recebimento_original",
    "data_recebimento_original_a",
    "data_cadastro_informacao",
    "data_cadastro_investigacao",
    "data_conclusao_investigacao",
    "data_conclusao_caso",
]

RENAME_MAP = {
    "CONTADOR": "sequencial_obito",
    "TIPOBITO": "tipo_obito",
    "DTOBITO": "data_obito",
    "HORAOBITO": "hora_obito",
    "NATURAL": "naturalidade",
    "CODMUNNATU": "id_municipio_6_naturalidade",
    "DTNASC": "data_nascimento",
    "IDADE": "idade_raw",
    "SEXO": "sexo",
    "RACACOR": "raca_cor",
    "ESTCIV": "estado_civil",
    "ESC": "escolaridade",
    "ESC2010": "escolaridade_2010",
    "SERIESCFAL": "serie_escolar_falecido",
    "OCUP": "ocupacao",
    "CODMUNRES": "id_municipio_6_resid",
    "CODBAIRES": "codigo_bairro_residencia",
    "LOCOCOR": "local_ocorrencia",
    "CODESTAB": "codigo_estabelecimento",
    "ESTABDESCR": "descricao_estabelecimento",
    "CODMUNOCOR": "id_municipio_6_ocor",
    "CODBAIOCOR": "codigo_bairro_ocorrencia",
    "IDADEMAE": "idade_mae",
    "ESCMAE": "escolaridade_mae",
    "ESCMAE2010": "escolaridade_mae_2010",
    "SERIESCMAE": "serie_escolar_mae",
    "OCUPMAE": "ocupacao_mae",
    "QTDFILVIVO": "quantidade_filhos_vivos",
    "QTDFILMORT": "quantidade_filhos_mortos",
    "GRAVIDEZ": "gravidez",
    "SEMAGESTAC": "semanas_gestacao",
    "GESTACAO": "gestacao",
    "PARTO": "parto",
    "OBITOPARTO": "obito_parto",
    "MORTEPARTO": "morte_parto",
    "PESO": "peso",
    "TPMORTEOCO": "tipo_morte_ocorrencia",
    "OBITOGRAV": "obito_gravidez",
    "OBITOPUERP": "obito_puerperio",
    "ASSISTMED": "assistencia_medica",
    "EXAME": "exame",
    "CIRURGIA": "cirurgia",
    "NECROPSIA": "necropsia",
    "LINHAA": "linha_a",
    "LINHAB": "linha_b",
    "LINHAC": "linha_c",
    "LINHAD": "linha_d",
    "LINHAII": "linha_ii",
    "CAUSABAS": "causa_basica",
    "CB_PRE": "causa_basica_pre",
    "COMUNSVOIM": "id_municipio_6_svo_iml",
    "DTATESTADO": "data_atestado",
    "CIRCOBITO": "circunstancia_obito",
    "ACIDTRAB": "acidente_trabalho",
    "FONTE": "fonte",
    "NUMEROLOTE": "numero_lote",
    "TPPOS": "tipo_pos",
    "DTINVESTIG": "data_investigacao",
    "CAUSABAS_O": "causa_basica_original",
    "DTCADASTRO": "data_cadastro",
    "ATESTANTE": "atestante",
    "STCODIFICA": "status_codificadora",
    "CODIFICADO": "codificado",
    "VERSAOSIST": "versao_sistema",
    "VERSAOSCB": "versao_scb",
    "FONTEINV": "fonte_investigacao",
    "DTRECEBIM": "data_recebimento",
    "ATESTADO": "atestado",
    "DTRECORIG": "data_recebimento_original",
    "DTRECORIGA": "data_recebimento_original_a",
    "CAUSAMAT": "causa_materna",
    "ESCMAEAGR1": "escolaridade_mae_2010_agr",
    "ESCFALAGR1": "escolaridade_falecido_2010_agr",
    "STDOEPIDEM": "status_do_epidem",
    "STDONOVA": "status_do_nova",
    "DIFDATA": "diferenca_data",
    "NUDIASOBCO": "numero_dias_obito_investigacao",
    "NUDIASOBIN": "numero_dias_obito_ficha",
    "DTCADINV": "data_cadastro_investigacao",
    "TPOBITOCOR": "tipo_obito_ocorrencia",
    "DTCONINV": "data_conclusao_investigacao",
    "FONTES": "fontes",
    "TPRESGINFO": "tipo_resgate_informacao",
    "TPNIVELINV": "tipo_nivel_investigador",
    "NUDIASINF": "numero_dias_informacao",
    "DTCADINF": "data_cadastro_informacao",
    "DTCONCASO": "data_conclusao_caso",
    "FONTESINF": "fontes_informacao",
    "ALTCAUSA": "alt_causa",
    "CRM": "crm",
}

# Mesma ordem de br_ms_sim__microdados.sql
COLUMN_ORDER = [
    "ano",
    "sigla_uf",
    "sequencial_obito",
    "tipo_obito",
    "causa_basica",
    "data_obito",
    "hora_obito",
    "naturalidade",
    "data_nascimento",
    "idade",
    "sexo",
    "raca_cor",
    "estado_civil",
    "escolaridade",
    "ocupacao",
    "codigo_bairro_residencia",
    "id_municipio_residencia",
    "local_ocorrencia",
    "codigo_bairro_ocorrencia",
    "id_municipio_ocorrencia",
    "idade_mae",
    "escolaridade_mae",
    "ocupacao_mae",
    "quantidade_filhos_vivos",
    "quantidade_filhos_mortos",
    "gravidez",
    "gestacao",
    "parto",
    "obito_parto",
    "morte_parto",
    "peso",
    "obito_gravidez",
    "obito_puerperio",
    "assistencia_medica",
    "exame",
    "cirurgia",
    "necropsia",
    "linha_a",
    "linha_b",
    "linha_c",
    "linha_d",
    "linha_ii",
    "circunstancia_obito",
    "acidente_trabalho",
    "fonte",
    "codigo_estabelecimento",
    "atestante",
    "data_atestado",
    "tipo_pos",
    "data_investigacao",
    "causa_basica_original",
    "data_cadastro",
    "fonte_investigacao",
    "data_recebimento",
    "causa_basica_pre",
    "tipo_obito_ocorrencia",
    "tipo_morte_ocorrencia",
    "data_cadastro_informacao",
    "data_cadastro_investigacao",
    "id_municipio_svo_iml",
    "data_recebimento_original",
    "data_recebimento_original_a",
    "causa_materna",
    "status_do_epidem",
    "status_do_nova",
    "serie_escolar_falecido",
    "serie_escolar_mae",
    "escolaridade_2010",
    "escolaridade_mae_2010",
    "escolaridade_falecido_2010_agr",
    "escolaridade_mae_2010_agr",
    "semanas_gestacao",
    "diferenca_data",
    "data_conclusao_investigacao",
    "data_conclusao_caso",
    "numero_dias_obito_investigacao",
    "id_municipio_naturalidade",
    "descricao_estabelecimento",
    "crm",
    "numero_lote",
    "status_codificadora",
    "codificado",
    "versao_sistema",
    "versao_scb",
    "atestado",
    "numero_dias_obito_ficha",
    "fontes",
    "tipo_resgate_informacao",
    "tipo_nivel_investigador",
    "numero_dias_informacao",
    "fontes_informacao",
    "alt_causa",
]


def read_dbc(filepath: str, encoding: str = "iso-8859-1") -> pd.DataFrame:
    filepath = Path(filepath)
    tmp_fd, tmp_path = tempfile.mkstemp(
        suffix=".dbf", dir=tempfile.gettempdir()
    )
    os.close(tmp_fd)
    try:
        dbc2dbf(str(filepath), tmp_path)
        table = DBF(tmp_path, encoding=encoding, load=True)
        return pd.DataFrame(iter(table))
    finally:
        Path(tmp_path).unlink(missing_ok=True)


def load_municipios(municipios_path: str) -> pd.DataFrame:
    return bd.read_sql(
        "SELECT id_municipio, id_municipio_6 FROM `basedosdados-dev.br_bd_diretorios_brasil.municipio`",
        billing_project_id="basedosdados-dev",
        from_file=True,
    ).astype(str)


def convert_municipio_6_to_7(
    df: pd.DataFrame, col_6: str, col_7: str, municipios: pd.DataFrame
) -> pd.DataFrame:
    if col_6 not in df.columns:
        return df
    df = df.merge(
        municipios[["id_municipio_6", "id_municipio"]],
        how="left",
        left_on=col_6,
        right_on="id_municipio_6",
    )
    df = df.drop(columns=[col_6, "id_municipio_6"])
    return df.rename(columns={"id_municipio": col_7})


def convert_municipio_resid_ocor(
    df: pd.DataFrame, ano: int, municipios: pd.DataFrame
) -> pd.DataFrame:
    if ano <= 2005:
        renames = {}
        if "id_municipio_6_resid" in df.columns:
            renames["id_municipio_6_resid"] = "id_municipio_residencia"
        if "id_municipio_6_ocor" in df.columns:
            renames["id_municipio_6_ocor"] = "id_municipio_ocorrencia"
        return df.rename(columns=renames)

    df = convert_municipio_6_to_7(
        df, "id_municipio_6_resid", "id_municipio_residencia", municipios
    )
    return convert_municipio_6_to_7(
        df, "id_municipio_6_ocor", "id_municipio_ocorrencia", municipios
    )


def parse_date(val: str) -> str | None:
    if not val or len(str(val).strip()) < 8 or str(val).strip() == "00000000":
        return None
    val = str(val).strip()
    return f"{val[4:8]}-{val[2:4]}-{val[0:2]}"


def parse_hora(val: str) -> str | None:
    if not val or len(str(val).strip()) < 4:
        return None
    val = str(val).strip().zfill(4)
    return f"{val[0:2]}:{val[2:4]}:00"


def parse_idade(val: str) -> float | None:
    if not val or len(str(val).strip()) < 2:
        return None
    val = str(val).strip()
    prefixo = val[0]
    try:
        numero = int(val[1:])
    except ValueError:
        return None
    if prefixo == "1":
        idade = 0.0
    elif prefixo == "2":
        idade = numero / 365
    elif prefixo == "3":
        idade = numero / 12
    elif prefixo == "4":
        idade = float(numero)
    elif prefixo == "5":
        idade = float(100 + numero)
    else:
        return None
    return round(idade, 2)


def _nullify(df: pd.DataFrame, col: str, invalid: list[str]) -> None:
    if col in df.columns:
        df[col] = df[col].replace(invalid, None)


def _recode(df: pd.DataFrame, col: str, mapping: dict[str, str]) -> None:
    if col in df.columns:
        df[col] = df[col].replace(mapping)


def recode_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    _nullify(df, "local_ocorrencia", ["0", "6", "7", "9"])
    _nullify(df, "sexo", ["0", "6", "7", "9"])
    _nullify(df, "raca_cor", ["0", "6", "7", "9"])
    _nullify(df, "estado_civil", ["0", "9"])
    for col in ["escolaridade", "escolaridade_mae"]:
        _nullify(df, col, ["0", "6", "7", "9", "A"])
    _nullify(df, "escolaridade_2010", ["9"])
    _nullify(df, "escolaridade_mae_2010", ["9"])
    _nullify(df, "gravidez", ["0", "9"])
    _nullify(df, "gestacao", ["0", "9"])
    _nullify(df, "parto", ["0", "3", "4", "5", "6", "7", "9"])
    _nullify(df, "obito_parto", ["0", "4", "5", "6", "7", "9"])
    _nullify(df, "morte_parto", ["0", "4", "5", "6", "7", "9"])
    _nullify(df, "obito_gravidez", ["0", "3", "4", "5", "6", "7", "9"])
    _nullify(df, "obito_puerperio", ["0", "4", "5", "6", "7", "9"])
    for col in [
        "assistencia_medica",
        "exame",
        "cirurgia",
        "necropsia",
        "acidente_trabalho",
    ]:
        _nullify(df, col, ["0", "4", "5", "6", "7", "9"])
    _nullify(df, "circunstancia_obito", ["0", "5", "6", "7", "9"])
    _nullify(df, "fonte", ["0", "5", "6", "7", "9"])
    _nullify(df, "fonte_investigacao", ["0", "9"])
    _nullify(df, "tipo_morte_ocorrencia", ["9"])

    _recode(df, "tipo_obito", {"1": "fetal", "2": "nao-fetal"})
    _recode(df, "sexo", {"1": "masculino", "2": "feminino"})
    _recode(
        df,
        "raca_cor",
        {
            "1": "branca",
            "2": "preta",
            "3": "amarela",
            "4": "parda",
            "5": "indigena",
        },
    )
    _recode(
        df,
        "estado_civil",
        {
            "1": "solteiro",
            "2": "casado",
            "3": "viuvo",
            "4": "separado judicialmente/divorciado",
            "5": "uniao consensual",
        },
    )
    for col in ["escolaridade", "escolaridade_mae"]:
        _recode(
            df,
            col,
            {
                "1": "nenhuma",
                "2": "1 a 3 anos",
                "3": "4 a 7 anos",
                "4": "8 a 11 anos",
                "5": "12 e mais",
                "8": "9 a 11 anos",
            },
        )
    for col in ["escolaridade_2010", "escolaridade_mae_2010"]:
        _recode(
            df,
            col,
            {
                "0": "sem escolaridade",
                "1": "fundamental I",
                "2": "fundamental II",
                "3": "medio",
                "4": "superior incompleto",
                "5": "superior completo",
            },
        )
    agr_map = {
        "00": "sem escolaridade",
        "01": "fundamental I incompleto",
        "02": "fundamental I completo",
        "03": "fundamental II incompleto",
        "04": "fundamental II completo",
        "05": "ensino medio incompleto",
        "06": "ensino medio completo",
        "07": "ensino superior incompleto",
        "08": "ensino superior completo",
        "09": "ignorado",
        "10": "fundamental I incompleto ou inespecifico",
        "11": "fundamental II incompleto ou inespecifico",
        "12": "ensino medio incompleto ou inespecifico",
    }
    for col in ["escolaridade_mae_2010_agr", "escolaridade_falecido_2010_agr"]:
        _recode(df, col, agr_map)

    _recode(
        df,
        "local_ocorrencia",
        {
            "1": "hospital",
            "2": "outro estabelecimento de saude",
            "3": "domicilio",
            "4": "via publica",
            "5": "outros",
            "6": "aldeia indigena",
        },
    )
    _recode(
        df,
        "gravidez",
        {"1": "unica", "2": "dupla", "3": "tripla e mais"},
    )
    _recode(
        df,
        "gestacao",
        {
            "A": "21 a 27 semanas",
            "1": "menos de 22 semanas",
            "2": "22 a 27 semanas",
            "3": "28 a 31 semanas",
            "4": "32 a 36 semanas",
            "5": "37 a 41 semanas",
            "6": "42 semanas ou mais",
            "7": "28 semanas ou mais",
            "8": "28 a 36 semanas",
        },
    )
    _recode(df, "parto", {"1": "vaginal", "2": "cesareo"})
    _recode(
        df,
        "obito_parto",
        {"1": "antes", "2": "durante", "3": "depois"},
    )
    _recode(
        df,
        "morte_parto",
        {"1": "antes", "2": "durante", "3": "depois"},
    )
    _recode(df, "obito_gravidez", {"1": "sim", "2": "nao"})
    _recode(
        df,
        "obito_puerperio",
        {
            "1": "0 a 42 dias",
            "2": "43 dias a 1 ano",
            "3": "nao",
        },
    )
    for col in ["assistencia_medica", "exame", "cirurgia", "necropsia"]:
        _recode(df, col, {"1": "sim", "2": "nao"})
    _recode(
        df,
        "circunstancia_obito",
        {
            "1": "acidente",
            "2": "suicidio",
            "3": "homicidio",
            "4": "outro",
        },
    )
    _recode(df, "acidente_trabalho", {"1": "sim", "2": "nao"})
    _recode(
        df,
        "fonte",
        {
            "1": "boletim de ocorrencia",
            "2": "hospital",
            "3": "familia",
            "4": "outro",
        },
    )
    _recode(
        df,
        "fonte_investigacao",
        {
            "1": "comite de mortalidade materna e/ou infantil",
            "2": "visita familiar / entrevista familia",
            "3": "estabelecimento de saude / prontuario",
            "4": "relacionamento com outros bancos de dados",
            "5": "SVO",
            "6": "IML",
            "7": "outra fonte",
            "8": "multiplas fontes",
        },
    )
    _recode(df, "status_do_epidem", {"1": "sim", "0": "nao"})
    _recode(df, "status_do_nova", {"1": "sim", "0": "nao"})
    _recode(
        df,
        "atestante",
        {
            "1": "sim",
            "2": "substituto",
            "3": "IML",
            "4": "SVO",
            "5": "outros",
        },
    )
    _recode(df, "tipo_pos", {"S": "sim", "N": "nao"})
    _recode(df, "status_codificadora", {"S": "sim", "N": "nao"})
    _recode(df, "codificado", {"S": "sim", "N": "nao"})
    _recode(
        df,
        "tipo_obito_ocorrencia",
        {
            "1": "durante a gestacao",
            "2": "duranto abortamento",
            "3": "apos abortamento",
            "4": "no parto ou ate 1 hora apos o parto",
            "5": "no puerperio (ate 42 dias do termino da gestacao)",
            "6": "entre o 43º dia e ate um ano apos o termino da gestacao",
            "7": "investigacao nao identificou o momento do obito",
            "8": "mais de 1 ano apos o parto",
            "9": "outras",
        },
    )
    _recode(
        df,
        "tipo_morte_ocorrencia",
        {
            "1": "na gravidez",
            "2": "no parto",
            "3": "no aborto",
            "4": "ate 42 dias apos o parto",
            "5": "de 43 dias ate 1 ano apos o parto",
            "8": "nao ocorreu nestes periodos",
        },
    )
    _recode(
        df,
        "tipo_resgate_informacao",
        {
            "1": "nao acrescentou nem corrigiu informacao",
            "2": "sim, permitiu o resgate de novas informacoes",
            "3": "sim, permitiu a correcao de alguma das causas informadas originalmente",
        },
    )
    _recode(
        df,
        "tipo_nivel_investigador",
        {"E": "estadual", "R": "regional", "M": "municipal"},
    )

    if "peso" in df.columns:
        df["peso"] = df["peso"].replace(["0"], None)

    return df


def ensure_schema_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in COLUMN_ORDER:
        if col not in df.columns:
            df[col] = None
    return df[COLUMN_ORDER]


def process_file(
    filepath: str, ano: int, uf: str, municipios: pd.DataFrame
) -> pd.DataFrame:
    df = read_dbc(filepath)
    df.columns = df.columns.str.upper()
    df = df.astype(str).replace({"None": None, "nan": None, "": None})
    df = df.replace("NA", None)

    df = df.rename(columns=RENAME_MAP)
    df = df.drop(columns=["ORIGEM", "UFINFORM"], errors="ignore")

    df["ano"] = ano
    df["sigla_uf"] = uf

    df = convert_municipio_resid_ocor(df, ano, municipios)
    df = convert_municipio_6_to_7(
        df, "id_municipio_6_svo_iml", "id_municipio_svo_iml", municipios
    )
    df = convert_municipio_6_to_7(
        df,
        "id_municipio_6_naturalidade",
        "id_municipio_naturalidade",
        municipios,
    )

    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(parse_date)

    if "hora_obito" in df.columns:
        df["hora_obito"] = df["hora_obito"].apply(parse_hora)

    if "idade_raw" in df.columns:
        df["idade"] = df["idade_raw"].apply(parse_idade)
        df = df.drop(columns=["idade_raw"])

    df = recode_columns(df)
    return ensure_schema_columns(df)


def process(
    year_range: list[int],
    input_dir: str,
    output_dir: str,
    municipios_path: str = MUNICIPIOS_PATH,
) -> None:
    municipios = load_municipios(municipios_path)

    for ano in year_range:
        for filepath in sorted(Path(input_dir).glob(f"DO*{ano}.dbc")):
            uf = filepath.stem[2:4]
            print(f"  Processando: {filepath.name}")

            try:
                df = process_file(str(filepath), ano, uf, municipios)
                partition_dir = (
                    Path(output_dir) / f"ano={ano}" / f"sigla_uf={uf}"
                )
                partition_dir.mkdir(parents=True, exist_ok=True)
                out_path = partition_dir / "microdados.csv"
                df.drop(columns=["ano", "sigla_uf"]).to_csv(
                    out_path, index=False
                )
                print(f"  Salvo: {out_path} ({len(df)} registros)")
            except Exception as e:
                print(f"  Erro ao processar {filepath.name}: {e}")
