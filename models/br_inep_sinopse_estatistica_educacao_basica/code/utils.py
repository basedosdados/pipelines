"""Funções puras do tratamento da Sinopse Estatística da Educação Básica.

Nenhuma função aqui depende de estado global nem do Prefect: recebem o caminho
da planilha e o mapa de UF, devolvem DataFrame. Um orquestrador — o script de
uma edição ou uma task de flow — compõe as chamadas.

Seis seções, na ordem em que o dado passa por elas:

- **cabeçalho** — descobre qual coluna da planilha é qual, lendo o cabeçalho
  hierárquico em vez de contar posições;
- **fonte** — baixa o zip, acha a planilha, lê aba e cabeçalho;
- **apoio** — o que todas as tabelas compartilham: renomear, filtrar município,
  derreter para formato longo, resolver a sigla da UF;
- **matrícula** e **docente** — uma `clean_*` por tabela, reunidas em `CLEANERS`;
- **saída** — escreve o CSV particionado e sobe para o staging.

O porquê das decisões está no README do conjunto.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Iterable
from pathlib import Path

import basedosdados as bd
import pandas as pd
import requests
import urllib3.exceptions
from constants import (  # type: ignore
    BLOCOS_DOCENTE_LOCALIZACAO,
    BLOCOS_ETAPA_ENSINO_SERIE,
    BLOCOS_LOCALIZACAO,
    BLOCOS_TEMPO_ENSINO,
    ETAPAS_FAIXA_ETARIA,
    ETAPAS_LOCALIZACAO,
    ETAPAS_POR_PREFIXO,
    ETAPAS_SEXO_RACA_COR,
    ETAPAS_TEMPO_ENSINO,
    FAIXAS_ETARIAS,
    FOOTNOTE,
    N_COLS_HEADER,
    NIVEIS_AGREGADOS,
    NIVEIS_PRIVADA,
    NIVEIS_PUBLICA,
    NIVEL_LOCALIZACAO,
    NIVEL_TEMPO_ENSINO,
    RACA_COR,
    REDES,
    REDES_COM_PUBLICA,
    REDES_PUBLICAS,
    RENAMES_DOCENTE_DEFICIENCIA,
    RENAMES_DOCENTE_ESCOLARIDADE,
    RENAMES_DOCENTE_ETAPA_ENSINO,
    RENAMES_DOCENTE_FAIXA_ETARIA_SEXO,
    RENAMES_DOCENTE_REGIME_CONTRATO,
    RENAMES_FAIXA_ETARIA,
    TOTAL,
    UF_NOME_SIGLA,
    renames_sexo_raca_cor,
    sheets_docente_deficiencia,
    sheets_docente_escolaridade,
    sheets_docente_etapa_ensino,
    sheets_docente_faixa_etaria_sexo,
    sheets_docente_localizacao,
    sheets_docente_regime_contrato,
    sheets_etapa_ensino_serie,
    sheets_faixa_etaria,
    sheets_localizacao,
    sheets_sexo_raca_cor,
    sheets_tempo_ensino,
)

# ------------------------------------------------------------- cabeçalho


def _clean(value: object) -> str | None:
    """Normaliza o valor de uma célula de cabeçalho, ou devolve None se vazia.

    Fora da primeira coluna de um intervalo mesclado, o pandas devolve NaN — e
    não None —, então converter direto para texto produziria a string "nan" como
    se fosse o nome de um nível.
    """
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    text = " ".join(str(value).split())
    return text or None


def clean_block(name: str) -> str:
    """Normaliza o nome do bloco para servir de chave estável.

    Duas coisas atrapalham usar o nome cru:

    - a planilha cola a nota de rodapé no fim (`'Ensino Fundamental5'`). Como o
      padrão exige um caractere não numérico antes do dígito, `'1º ano'` e
      `'4º ano'` passam intactos;
    - a própria planilha mistura hífen e travessão entre os nomes dos cursos
      profissionais, então o separador é unificado em hífen — que é o que a Base
      dos Dados usa (`br_inep_educacao_especial__etapa_ensino.sql` já faz esse
      mesmo `replace`).
    """
    name = name.replace("–", "-").replace("—", "-")  # noqa: RUF001
    return FOOTNOTE.sub(r"\1", name).strip()


def _row_values(row: tuple, n_cols: int) -> list[str | None]:
    """Valores limpos de uma linha, um por coluna, completando o que falta."""
    return [_clean(row[i]) if i < len(row) else None for i in range(n_cols)]


def _labels_columns(values: list[str | None]) -> bool:
    """A linha rotula colunas, em vez de ser título da aba.

    O título da planilha ocupa uma célula só; uma linha de cabeçalho nomeia
    vários grupos ou colunas.
    """
    return sum(1 for value in values if value) >= 2


def _spread_merged(
    values: list[str | None], paths: list[list[str]]
) -> list[str | None]:
    """Estende cada valor para a direita, até onde o grupo acima ainda é o mesmo.

    Célula mesclada só tem valor na primeira coluna do intervalo, então o valor
    precisa alcançar as demais. O freio é `paths`, o caminho já acumulado nas
    linhas de cima: quando ele muda, começou outro grupo e a propagação para.
    Sem isso, a `Filantrópica` do 1º ano alcançaria a primeira coluna do 2º.
    """
    spread: list[str | None] = []
    current: str | None = None

    for i, value in enumerate(values):
        same_group = i > 0 and paths[i] == paths[i - 1]
        if value is not None:
            current = value
        elif not same_group:
            current = None
        spread.append(current)

    return spread


def column_paths(
    header_rows: list[tuple], n_cols: int
) -> dict[int, tuple[str, ...]]:
    """Reconstrói o caminho de cabeçalho de cada coluna.

    Args:
        header_rows: linhas do cabeçalho, de cima para baixo, como tuplas de
            valores crus.
        n_cols: quantas colunas considerar.

    Returns:
        Mapa de índice da coluna para o caminho, do nível mais externo ao mais
        interno.
    """
    paths: list[list[str]] = [[] for _ in range(n_cols)]

    for row in header_rows:
        values = _row_values(row, n_cols)
        if not _labels_columns(values):
            continue
        for i, value in enumerate(_spread_merged(values, paths)):
            if value is not None:
                paths[i] = [*paths[i], value]

    return {i: tuple(p) for i, p in enumerate(paths)}


def rede_columns(
    paths: dict[int, tuple[str, ...]],
    under: str | None = None,
    agregado: str | None = None,
    com_publica: bool = False,
) -> dict[tuple[str, str], int]:
    """Acha a coluna de cada par (bloco, rede).

    O bloco é o nível imediatamente acima de `Rede Pública`/`Rede Privada` — a
    série ("5º ano"), a etapa ("Creche"), o curso, a localização ("Urbana") ou o
    tempo de ensino ("Tempo Integral"). A rede privada é a coluna
    `Rede Privada / Total`, e **não** `Particular`: esta é apenas uma das quatro
    categorias que compõem a privada, e confundi-las foi o que reduziu a rede
    privada a alguns milhares de matrículas em 2025.

    Args:
        paths: caminho de cabeçalho de cada coluna.
        under: quando dado, só considera as colunas abaixo desse nível. As abas
            de `localizacao` e `tempo_ensino` repetem, antes dele, o total por
            rede da etapa inteira — colunas que têm a mesma forma e outro
            significado.
        agregado: nome a dar ao bloco das colunas de rede que ficam direto sob
            o título da aba, o total da etapa. Sem ele essas colunas são
            descartadas junto com o resto que está fora de `under`.
        com_publica: também devolve a rede `Pública`, que é
            `Rede Pública / Total`. As tabelas de matrícula publicam só as três
            redes públicas; as de docente publicam a soma delas.

    Returns:
        Mapa de (bloco, rede) para índice da coluna, com `rede` em
        Federal/Estadual/Municipal/Privada, mais Pública se pedida.

    Raises:
        ValueError: se `under` não existe no cabeçalho da aba, ou se duas
            colunas disputam o mesmo par.
    """
    if under is not None and not any(under in path for path in paths.values()):
        raise ValueError(f"a aba não tem o nível {under!r} no cabeçalho")

    found: dict[tuple[str, str], int] = {}

    for idx, path in sorted(paths.items()):
        if len(path) < 3:
            continue
        bloco, nivel, folha = path[-3], path[-2], path[-1]

        if nivel in NIVEIS_PUBLICA and folha in REDES_PUBLICAS:
            rede = folha
        elif nivel in NIVEIS_PUBLICA and folha == TOTAL and com_publica:
            rede = "Pública"
        elif nivel in NIVEIS_PRIVADA and folha == TOTAL:
            rede = "Privada"
        else:
            continue

        if len(path) == 3 and agregado is not None:
            bloco = agregado  # a rede está logo abaixo do título da aba
        elif under is not None and under not in path:
            continue

        if bloco in NIVEIS_AGREGADOS:
            continue

        key = (clean_block(bloco), rede)
        if key in found:
            raise ValueError(
                f"coluna ambígua para {key}: {found[key]} e {idx}"
            )
        found[key] = idx

    return found


def build_renames(
    found: dict[tuple[str, str], int],
    prefixes: dict[str, str],
    redes: tuple[str, ...] = REDES,
) -> dict[str, str]:
    """Monta o dicionário de renomeação do pandas a partir do cabeçalho.

    `prefixes` liga o nome do bloco **como a planilha o escreve** ao prefixo que
    o resto do script usa (`"5º ano"` -> `"5"`, gerando `5_federal`). Por ser
    indexado por nome e não por posição, uma renomeação de bloco pelo INEP falha
    aqui em vez de virar número errado publicado.

    Args:
        found: saída de `rede_columns`.
        prefixes: nome do bloco na planilha -> prefixo interno.
        redes: as redes que todo bloco tem de ter.

    Raises:
        ValueError: se um bloco declarado não existe na aba, se a aba traz um
            bloco não declarado, ou se algum bloco não tem todas as redes.
    """
    blocos = {bloco for bloco, _ in found}
    declarados = set(prefixes)

    if faltando := declarados - blocos:
        raise ValueError(
            f"blocos declarados que a aba não tem: {sorted(faltando)} "
            f"(a aba tem {sorted(blocos)})"
        )
    if sobrando := blocos - declarados:
        raise ValueError(f"blocos da aba não declarados: {sorted(sobrando)}")

    renames: dict[str, str] = {}
    for bloco, prefix in prefixes.items():
        encontradas = {rede for b, rede in found if b == bloco}
        if encontradas != set(redes):
            raise ValueError(
                f"bloco {bloco!r} sem as redes {sorted(redes)}: "
                f"{sorted(encontradas)}"
            )
        for rede in redes:
            renames[f"Unnamed: {found[(bloco, rede)]}"] = (
                f"{prefix}_{rede.lower()}"
            )

    return renames


# ---------------------------------------------------------------- fonte


def download_zip(url: str, zip_path: Path, extract_to: Path) -> None:
    """Baixa e extrai o zip da Sinopse, se ainda não estiver em disco.

    O certificado de download.inep.gov.br não valida, daí `verify=False`.
    """
    if zip_path.exists():
        return

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    response = requests.get(
        url,
        headers={"User-Agent": "Mozilla/5.0"},
        verify=False,
        stream=True,
        timeout=180,
    )
    response.raise_for_status()

    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with open(zip_path, "wb") as fd:
        for chunk in response.iter_content(chunk_size=1 << 20):
            fd.write(chunk)

    import zipfile

    with zipfile.ZipFile(zip_path) as archive:
        archive.extractall(extract_to)


def find_workbook(input_dir: Path) -> Path:
    """Acha a planilha extraída sem depender do nome do arquivo.

    O nome carrega o "ção" mal codificado pelo zip e ganha sufixos de revisão
    (`_V2`), então escrevê-lo à mão quebra a cada republicação do INEP. A maior
    planilha é a Sinopse; o zip pode trazer anexos menores.
    """
    candidates = sorted(input_dir.glob("**/*.xlsx"))
    if not candidates:
        raise FileNotFoundError(f"nenhuma planilha .xlsx em {input_dir}")
    return max(candidates, key=lambda path: path.stat().st_size)


def read_sheet(
    workbook: Path, sheet_name: str, skiprows: int = 8
) -> pd.DataFrame:
    return pd.read_excel(workbook, sheet_name=sheet_name, skiprows=skiprows)


def read_header_rows(
    workbook: Path, sheet_name: str, skiprows: int
) -> list[tuple]:
    """Lê as linhas de cabeçalho cruas, para reconstruir a hierarquia.

    São exatamente as linhas que `read_sheet` pula. Célula mesclada vem como
    NaN fora da primeira coluna do intervalo, o que `column_paths` resolve.
    """
    raw = pd.read_excel(
        workbook, sheet_name=sheet_name, header=None, nrows=skiprows
    )
    return list(raw.itertuples(index=False, name=None))


def load_uf_map(billing_project_id: str | None = None) -> dict[str, str]:
    """Nome da unidade da federação -> sigla.

    Consulta o diretório da Base dos Dados, que é a fonte de verdade. Sem
    `billing_project_id`, devolve o mapa fixo de `constants.UF_NOME_SIGLA`, que
    permite rodar o tratamento sem acesso ao BigQuery — o service account de dev
    não tem permissão de criar job, e as 27 unidades da federação não mudam.
    """
    if billing_project_id is None:
        return dict(UF_NOME_SIGLA)

    directory = bd.read_sql(
        "SELECT nome, sigla FROM `basedosdados.br_bd_diretorios_brasil.uf`",
        billing_project_id=billing_project_id,
    )
    return {row["nome"]: row["sigla"] for row in directory.to_dict("records")}


def load_municipio_ids(
    billing_project_id: str | None = None,
) -> set[str] | None:
    """Ids do diretório de municípios, ou None sem acesso ao BigQuery.

    O número de municípios muda entre edições, então não cabe no código: o
    script de 2024 comparava com 5.570 escrito à mão, que 2025 contraria. Sem o
    diretório, `clean_all` compara as tabelas entre si, o que não prova que a
    planilha traz todos os municípios, mas pega uma tabela que perdeu alguns.
    """
    if billing_project_id is None:
        return None

    directory = bd.read_sql(
        "SELECT id_municipio FROM `basedosdados.br_bd_diretorios_brasil.municipio`",
        billing_project_id=billing_project_id,
    )
    return set(directory["id_municipio"].astype("string"))


# ---------------------------------------------------------------- apoio


def drop_unused_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Descarta o que não foi renomeado: colunas sem nome e os totais."""
    to_drop = [
        col
        for col in df.columns
        if col.startswith("Unnamed") or col.startswith("Total")
    ]
    return df.drop(columns=to_drop)


def rename_by_sheet(
    df: pd.DataFrame, sheet_name: str, renames: dict[str, str]
) -> pd.DataFrame:
    """Renomeia com `errors="raise"`, dizendo qual aba falhou."""
    try:
        return df.rename(columns=renames, errors="raise")
    except Exception as e:
        raise Exception(f"Falha ao renomear a aba {sheet_name}") from e


def read_and_rename(
    workbook: Path,
    sheets: dict[str, str | tuple[str, int]],
    renames: dict[str, str] | dict[str, dict[str, str]],
    *,
    skiprows: int = 8,
    per_sheet_renames: bool = False,
    keep_renamed_order: bool = False,
) -> dict[str, pd.DataFrame]:
    """Lê cada aba, renomeia as colunas e descarta as não usadas.

    Args:
        workbook: caminho da planilha.
        sheets: nome interno -> nome da aba, ou (nome da aba, skiprows).
        renames: mapa de renomeação, ou um mapa por aba se `per_sheet_renames`.
        skiprows: usado quando `sheets` não traz o valor por aba.
        per_sheet_renames: `renames` é indexado pelo nome interno da aba.
        keep_renamed_order: reindexa as colunas na ordem do mapa, descartando
            o que ficou de fora.
    """
    out: dict[str, pd.DataFrame] = {}

    for name, spec in sheets.items():
        sheet_name, rows = (
            spec if isinstance(spec, tuple) else (spec, skiprows)
        )
        sheet_renames = renames[name] if per_sheet_renames else renames
        df = drop_unused_columns(
            rename_by_sheet(
                read_sheet(workbook, sheet_name, skiprows=rows),
                sheet_name=sheet_name,
                renames=sheet_renames,  # type: ignore[arg-type]
            )
        )
        if keep_renamed_order:
            df = df[list(sheet_renames.values())]  # type: ignore[union-attr]
        out[name] = df

    return out


def municipio_as_string(values: pd.Series) -> pd.Series:
    """Id do município como texto, mesmo onde o pandas leu a coluna como número.

    Em algumas abas a coluna de código vem sem texto nenhum e o pandas a lê como
    float, porque as linhas de região e de total ficam vazias. Converter direto
    para texto produz `'1100015.0'`, e o município aparece duas vezes na tabela,
    com e sem o sufixo — foi o que 2025 publicou em `docente_etapa_ensino`.
    """
    if pd.api.types.is_numeric_dtype(values):
        return values.astype("Int64").astype("string")
    return values.astype("string").str.strip()


def only_municipality_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza o id do município e descarta as linhas de total e região."""
    municipio = municipio_as_string(df["id_municipio"])
    return df.assign(id_municipio=municipio).loc[
        municipio.notna() & (municipio != "")
    ]


def melt_sheets(
    dfs: dict[str, pd.DataFrame],
    *,
    label_column: str,
    value_vars: Callable[[pd.DataFrame], list[str]],
    var_name: str,
    value_name: str,
) -> pd.DataFrame:
    """Empilha as abas no formato longo, marcando de qual aba cada linha veio.

    `label_column` é o nome da coluna que recebe a chave da aba, e
    `value_vars` escolhe, por aba, quais colunas virar linha.
    """
    frames = [
        df.assign(**{label_column: label})
        .pipe(only_municipality_rows)
        .pipe(
            lambda d: pd.melt(
                d,
                id_vars=["id_municipio", "uf", label_column],
                value_vars=value_vars(d),
                var_name=var_name,
                value_name=value_name,
            )
        )
        for label, df in dfs.items()
    ]
    return pd.concat(frames)


def add_sigla_uf(df: pd.DataFrame, uf_map: dict[str, str]) -> pd.DataFrame:
    """Troca o nome da unidade da federação pela sigla, em `sigla_uf`."""
    return df.assign(
        sigla_uf=lambda d: d["uf"].str.strip().replace(uf_map)
    ).drop(columns=["uf"])


def suffix(value: str) -> str:
    """Último pedaço do nome de coluna, que carrega a rede.

    Não capitaliza: as tabelas de matrícula aplicam `.str.title()` por cima, as
    de docente preservam a caixa da planilha. Capitalizar aqui estragaria siglas
    como "Contrato CLT".
    """
    return value.split("_")[-1].strip()


def prefix(value: str) -> str:
    """Primeiro pedaço do nome de coluna, que carrega a categoria."""
    return value.split("_")[0].strip()


def header_renames(
    workbook: Path,
    sheets: dict[str, str] | dict[str, tuple[str, int]],
    blocos: dict[str, str] | dict[str, dict[str, str]],
    *,
    skiprows: int = 8,
    per_sheet_blocos: bool = False,
    under: str | None = None,
    agregado: str | None = None,
    redes: tuple[str, ...] = REDES,
) -> dict[str, dict[str, str]]:
    """Renomeação de cada aba, derivada do cabeçalho em vez da posição.

    Não usa índice de coluna: `build_renames` falha se o INEP mudar os blocos,
    em vez de publicar o número de outra rede.

    Args:
        workbook: caminho da planilha.
        sheets: nome interno -> nome da aba, ou (nome da aba, skiprows).
        blocos: nome do bloco na planilha -> prefixo interno; um mapa por aba se
            `per_sheet_blocos`.
        skiprows: usado quando `sheets` não traz o valor por aba.
        per_sheet_blocos: `blocos` é indexado pelo nome interno da aba.
        under: repassado a `rede_columns`, para recortar o trecho do cabeçalho.
        agregado: repassado a `rede_columns`, para nomear o bloco do total.
        redes: as redes esperadas em cada bloco; `REDES_COM_PUBLICA` nas tabelas
            de docente, que publicam também a soma das redes públicas.

    Returns:
        Nome interno da aba -> mapa de renomeação do pandas.
    """
    renames: dict[str, dict[str, str]] = {}

    for name, spec in sheets.items():
        sheet_name, rows = (
            spec if isinstance(spec, tuple) else (spec, skiprows)
        )
        declarados = blocos[name] if per_sheet_blocos else blocos
        renames[name] = {
            "Unnamed: 1": "uf",
            "Unnamed: 3": "id_municipio",
            **build_renames(
                rede_columns(
                    column_paths(
                        read_header_rows(workbook, sheet_name, rows),
                        N_COLS_HEADER,
                    ),
                    under=under,
                    agregado=agregado,
                    com_publica="Pública" in redes,
                ),
                declarados,  # type: ignore[arg-type]
                redes=redes,
            ),
        }

    return renames


def etapa_ensino_from_column(value: str) -> str:
    """Etapa de ensino a partir do prefixo do nome de coluna."""
    if value.startswith("creche"):
        return "Educação Infantil - Creche"
    if value.startswith("pre_escola"):
        return "Educação Infantil - Pré-Escola"
    if value.startswith("em"):
        return "Ensino Médio Regular"
    if value.startswith("ep"):
        curso = value.rsplit("_", 1)[0]
        try:
            return ETAPAS_POR_PREFIXO[curso]
        except KeyError as e:
            raise ValueError(
                f"prefixo de curso profissional desconhecido: {value!r}"
            ) from e
    if value.startswith("eja"):
        _, etapa, _ = value.split("_")
        if etapa == "ef":
            return "EJA - Ensino Fundamental"
        if etapa == "em":
            return "EJA - Ensino Médio"
        raise ValueError(f"etapa de EJA desconhecida: {value!r}")

    ano = int(value.split("_")[0])
    if ano <= 5:
        return "Ensino Fundamental - Anos Iniciais"
    if ano <= 9:
        return "Ensino Fundamental - Anos Finais"
    raise ValueError(f"ano desconhecido: {value!r}")


def serie_from_column(value: str) -> str | None:
    """Série a partir do nome de coluna, ou None quando a etapa não tem série."""
    parts = value.split("_")

    if parts[0].isdigit() and int(parts[0]) in range(1, 10):
        etapa = "Iniciais" if int(parts[0]) <= 5 else "Finais"
        return f"{parts[0]}º ano do Ensino Fundamental - Anos {etapa}"

    if value.startswith("em"):
        numero = parts[1].strip()
        if numero == "ns":
            return "Ensino Médio Não Seriado"
        if numero.isdigit():
            return f"{numero}º ano do Ensino Médio Regular"
        raise ValueError(f"ano do ensino médio desconhecido: {value!r}")

    return None


# ---------------------------------------------------------------- matrícula


def clean_etapa_ensino_serie(
    workbook: Path, uf_map: dict[str, str]
) -> pd.DataFrame:
    dfs = read_and_rename(
        workbook,
        sheets_etapa_ensino_serie,
        header_renames(
            workbook,
            sheets_etapa_ensino_serie,
            BLOCOS_ETAPA_ENSINO_SERIE,
            per_sheet_blocos=True,
        ),
        per_sheet_renames=True,
    )
    return (
        melt_sheets(
            dfs,
            label_column="_aba",
            value_vars=lambda d: [
                c
                for c in d.columns
                if c.endswith(("federal", "estadual", "municipal", "privada"))
            ],
            var_name="coluna",
            value_name="quantidade_matricula",
        )
        .assign(
            etapa_ensino=lambda d: (
                d["coluna"].apply(etapa_ensino_from_column).astype("string")
            ),
            serie=lambda d: d["coluna"].apply(serie_from_column),
            rede=lambda d: d["coluna"].apply(suffix).str.title(),
            quantidade_matricula=lambda d: d["quantidade_matricula"].astype(
                "Int64"
            ),
        )
        .pipe(add_sigla_uf, uf_map)[
            [
                "sigla_uf",
                "id_municipio",
                "rede",
                "etapa_ensino",
                "serie",
                "quantidade_matricula",
            ]
        ]
    )


def clean_faixa_etaria(workbook: Path, uf_map: dict[str, str]) -> pd.DataFrame:
    dfs = read_and_rename(
        workbook,
        sheets_faixa_etaria,
        RENAMES_FAIXA_ETARIA,
        per_sheet_renames=True,
    )
    return (
        melt_sheets(
            dfs,
            label_column="etapa",
            value_vars=lambda d: [
                c for c in d.columns if c.endswith(("anos", "mais"))
            ],
            var_name="faixa_etaria",
            value_name="quantidade_matricula",
        )
        .assign(
            etapa_ensino=lambda d: d["etapa"].replace(ETAPAS_FAIXA_ETARIA),
            faixa_etaria=lambda d: d["faixa_etaria"].replace(FAIXAS_ETARIAS),
            quantidade_matricula=lambda d: d["quantidade_matricula"].astype(
                "Int64"
            ),
        )
        .pipe(add_sigla_uf, uf_map)[
            [
                "sigla_uf",
                "id_municipio",
                "etapa_ensino",
                "faixa_etaria",
                "quantidade_matricula",
            ]
        ]
    )


def clean_localizacao(workbook: Path, uf_map: dict[str, str]) -> pd.DataFrame:
    dfs = read_and_rename(
        workbook,
        sheets_localizacao,
        header_renames(
            workbook,
            sheets_localizacao,
            BLOCOS_LOCALIZACAO,
            skiprows=10,
            under=NIVEL_LOCALIZACAO,
        ),
        skiprows=10,
        per_sheet_renames=True,
    )
    return (
        melt_sheets(
            dfs,
            label_column="etapa",
            value_vars=lambda d: [
                c for c in d.columns if c.startswith(("rural", "urbana"))
            ],
            var_name="coluna",
            value_name="quantidade_matricula",
        )
        .assign(
            rede=lambda d: d["coluna"].apply(suffix).str.title(),
            localizacao=lambda d: d["coluna"].apply(prefix).str.title(),
            etapa_ensino=lambda d: d["etapa"].replace(ETAPAS_LOCALIZACAO),
            quantidade_matricula=lambda d: d["quantidade_matricula"].astype(
                "Int64"
            ),
        )
        .pipe(add_sigla_uf, uf_map)[
            [
                "sigla_uf",
                "id_municipio",
                "rede",
                "etapa_ensino",
                "localizacao",
                "quantidade_matricula",
            ]
        ]
    )


def clean_tempo_ensino(workbook: Path, uf_map: dict[str, str]) -> pd.DataFrame:
    dfs = read_and_rename(
        workbook,
        sheets_tempo_ensino,
        header_renames(
            workbook,
            sheets_tempo_ensino,
            BLOCOS_TEMPO_ENSINO,
            skiprows=11,
            under=NIVEL_TEMPO_ENSINO,
        ),
        skiprows=11,
        per_sheet_renames=True,
    )
    return (
        melt_sheets(
            dfs,
            label_column="etapa",
            value_vars=lambda d: [
                c for c in d.columns if c.startswith(("parcial", "integral"))
            ],
            var_name="coluna",
            value_name="quantidade_matricula",
        )
        .assign(
            rede=lambda d: d["coluna"].apply(suffix).str.title(),
            tempo_ensino=lambda d: d["coluna"].apply(prefix).str.title(),
            etapa_ensino=lambda d: d["etapa"].replace(ETAPAS_TEMPO_ENSINO),
            quantidade_matricula=lambda d: d["quantidade_matricula"].astype(
                "Int64"
            ),
        )
        .pipe(add_sigla_uf, uf_map)[
            [
                "sigla_uf",
                "id_municipio",
                "rede",
                "etapa_ensino",
                "tempo_ensino",
                "quantidade_matricula",
            ]
        ]
    )


def clean_sexo_raca_cor(
    workbook: Path, uf_map: dict[str, str]
) -> pd.DataFrame:
    dfs = read_and_rename(
        workbook, sheets_sexo_raca_cor, renames_sexo_raca_cor, skiprows=9
    )
    return (
        melt_sheets(
            dfs,
            label_column="etapa",
            value_vars=lambda d: [
                c for c in d.columns if c.startswith(("feminino", "masculino"))
            ],
            var_name="coluna",
            value_name="quantidade_matricula",
        )
        .assign(
            etapa_ensino=lambda d: d["etapa"].replace(ETAPAS_SEXO_RACA_COR),
            sexo=lambda d: d["coluna"].apply(prefix).str.title(),
            raca_cor=lambda d: (
                d["coluna"].apply(lambda v: v.split("_")[-1]).replace(RACA_COR)
            ),
            quantidade_matricula=lambda d: d["quantidade_matricula"].astype(
                "Int64"
            ),
        )
        .pipe(add_sigla_uf, uf_map)[
            [
                "sigla_uf",
                "id_municipio",
                "etapa_ensino",
                "sexo",
                "raca_cor",
                "quantidade_matricula",
            ]
        ]
    )


# ---------------------------------------------------------------- docente


def _melt_docente(
    dfs: dict[str, pd.DataFrame],
    uf_map: dict[str, str],
    *,
    label_column: str,
) -> pd.DataFrame:
    """Parte comum das tabelas de docente: empilha, resolve UF e tipa.

    Todas derretem tudo que não é identificador, chamam a medida
    `quantidade_docente` e deixam o nome da coluna de origem em `coluna`.
    """
    return (
        melt_sheets(
            dfs,
            label_column=label_column,
            value_vars=lambda d: [
                c
                for c in d.columns
                if c not in ("id_municipio", "uf", label_column, "rede")
            ],
            var_name="coluna",
            value_name="quantidade_docente",
        )
        .assign(
            quantidade_docente=lambda d: d["quantidade_docente"].astype(
                "Int64"
            ),
        )
        .pipe(add_sigla_uf, uf_map)
    )


def clean_docente_etapa_ensino(
    workbook: Path, uf_map: dict[str, str]
) -> pd.DataFrame:
    dfs = read_and_rename(
        workbook,
        sheets_docente_etapa_ensino,
        RENAMES_DOCENTE_ETAPA_ENSINO,
        per_sheet_renames=True,
        keep_renamed_order=True,
    )
    return _melt_docente(dfs, uf_map, label_column="tipo_classe").rename(
        columns={
            "coluna": "etapa_ensino",
            "quantidade_docente": "quantidade_docentes",
        }
    )[
        [
            "id_municipio",
            "etapa_ensino",
            "quantidade_docentes",
            "tipo_classe",
            "sigla_uf",
        ]
    ]


def clean_docente_localizacao(
    workbook: Path, uf_map: dict[str, str]
) -> pd.DataFrame:
    dfs = read_and_rename(
        workbook,
        sheets_docente_localizacao,
        header_renames(
            workbook,
            sheets_docente_localizacao,
            BLOCOS_DOCENTE_LOCALIZACAO,
            skiprows=10,
            under=NIVEL_LOCALIZACAO,
            agregado=TOTAL,
            redes=REDES_COM_PUBLICA,
        ),
        skiprows=10,
        per_sheet_renames=True,
    )
    return _melt_docente(dfs, uf_map, label_column="etapa_ensino").assign(
        rede=lambda d: d["coluna"].apply(suffix).str.title(),
        localizacao=lambda d: d["coluna"].apply(prefix).str.title(),
    )[
        [
            "id_municipio",
            "etapa_ensino",
            "rede",
            "localizacao",
            "quantidade_docente",
            "sigla_uf",
        ]
    ]


def clean_docente_escolaridade(
    workbook: Path, uf_map: dict[str, str]
) -> pd.DataFrame:
    dfs = read_and_rename(
        workbook,
        sheets_docente_escolaridade,
        RENAMES_DOCENTE_ESCOLARIDADE,
        skiprows=10,
    )
    return _melt_docente(dfs, uf_map, label_column="tipo_classe").rename(
        columns={"coluna": "escolaridade"}
    )[
        [
            "id_municipio",
            "escolaridade",
            "quantidade_docente",
            "tipo_classe",
            "sigla_uf",
        ]
    ]


def clean_docente_deficiencia(
    workbook: Path, uf_map: dict[str, str]
) -> pd.DataFrame:
    dfs = read_and_rename(
        workbook,
        sheets_docente_deficiencia,
        RENAMES_DOCENTE_DEFICIENCIA,
        skiprows=7,
        per_sheet_renames=True,
    )
    return _melt_docente(dfs, uf_map, label_column="tipo_classe").rename(
        columns={"coluna": "deficiencia"}
    )[
        [
            "id_municipio",
            "deficiencia",
            "quantidade_docente",
            "tipo_classe",
            "sigla_uf",
        ]
    ]


def clean_docente_faixa_etaria_sexo(
    workbook: Path, uf_map: dict[str, str]
) -> pd.DataFrame:
    dfs = read_and_rename(
        workbook,
        sheets_docente_faixa_etaria_sexo,
        RENAMES_DOCENTE_FAIXA_ETARIA_SEXO,
        per_sheet_renames=True,
    )
    return _melt_docente(dfs, uf_map, label_column="etapa_ensino").assign(
        faixa_etaria=lambda d: d["coluna"].apply(
            lambda v: (
                v.replace("Feminino_", "")
                if v.startswith("Feminino")
                else v.replace("Masculino_", "")
            )
        ),
        sexo=lambda d: d["coluna"].apply(lambda v: v.split("_")[0].strip()),
    )[
        [
            "id_municipio",
            "faixa_etaria",
            "quantidade_docente",
            "etapa_ensino",
            "sexo",
            "sigla_uf",
        ]
    ]


def clean_docente_regime_contrato(
    workbook: Path, uf_map: dict[str, str]
) -> pd.DataFrame:
    dfs = read_and_rename(
        workbook,
        sheets_docente_regime_contrato,
        RENAMES_DOCENTE_REGIME_CONTRATO,
        per_sheet_renames=True,
    )
    return _melt_docente(dfs, uf_map, label_column="etapa_ensino").assign(
        rede=lambda d: d["coluna"].apply(suffix),
        regime_contrato=lambda d: d["coluna"].apply(prefix),
    )[
        [
            "id_municipio",
            "regime_contrato",
            "quantidade_docente",
            "etapa_ensino",
            "rede",
            "sigla_uf",
        ]
    ]


#: Tabela -> função que produz o DataFrame limpo dela.
CLEANERS: dict[str, Callable[[Path, dict[str, str]], pd.DataFrame]] = {
    "etapa_ensino_serie": clean_etapa_ensino_serie,
    "faixa_etaria": clean_faixa_etaria,
    "localizacao": clean_localizacao,
    "tempo_ensino": clean_tempo_ensino,
    "sexo_raca_cor": clean_sexo_raca_cor,
    "docente_etapa_ensino": clean_docente_etapa_ensino,
    "docente_localizacao": clean_docente_localizacao,
    "docente_escolaridade": clean_docente_escolaridade,
    "docente_deficiencia": clean_docente_deficiencia,
    "docente_faixa_etaria_sexo": clean_docente_faixa_etaria_sexo,
    "docente_regime_contrato": clean_docente_regime_contrato,
}


# ---------------------------------------------------------------- saída


def write_partitioned(
    df: pd.DataFrame, table_id: str, year: int, output_dir: Path
) -> int:
    """Escreve um CSV por UF em `<output>/<tabela>/ano=<ano>/sigla_uf=<uf>/`.

    Returns:
        Quantas linhas foram escritas.
    """
    written = 0
    for sigla_uf, partition in df.groupby("sigla_uf"):
        path = output_dir / table_id / f"ano={year}" / f"sigla_uf={sigla_uf}"
        path.mkdir(parents=True, exist_ok=True)
        partition.drop(columns=["sigla_uf"]).to_csv(
            path / "data.csv", index=False
        )
        written += len(partition)
    return written


def check_municipios(
    df: pd.DataFrame, table_id: str, esperados: set[str] | None
) -> set[str]:
    """Confere os municípios da tabela contra `esperados` e os devolve.

    Args:
        df: tabela já tratada.
        table_id: nome da tabela, para a mensagem de erro.
        esperados: conjunto do diretório da Base dos Dados, ou None na primeira
            tabela quando não há acesso ao BigQuery.

    Raises:
        ValueError: se a tabela não tem exatamente os municípios esperados.
    """
    municipios = set(df["id_municipio"].astype("string").str.strip().dropna())
    if esperados is None:
        return municipios

    if faltando := esperados - municipios:
        raise ValueError(
            f"{table_id}: faltam {len(faltando)} municípios, entre eles "
            f"{sorted(faltando)[:5]}"
        )
    if sobrando := municipios - esperados:
        raise ValueError(
            f"{table_id}: {len(sobrando)} municípios a mais, entre eles "
            f"{sorted(sobrando)[:5]}"
        )
    return municipios


def clean_all(
    workbook: Path,
    uf_map: dict[str, str],
    year: int,
    output_dir: Path,
    tables: Iterable[str] | None = None,
    municipios: set[str] | None = None,
) -> dict[str, int]:
    """Limpa e escreve as tabelas pedidas (todas, por padrão).

    Args:
        workbook: caminho da planilha.
        uf_map: nome da unidade da federação -> sigla.
        year: ano da edição, que vira partição.
        output_dir: onde escrever.
        tables: quais tabelas tratar.
        municipios: ids esperados, de `load_municipio_ids`. Sem eles, a primeira
            tabela tratada define o conjunto e as demais são comparadas com ela.

    Returns:
        Tabela -> número de linhas escritas.
    """
    selected = list(tables) if tables is not None else list(CLEANERS)
    if unknown := set(selected) - set(CLEANERS):
        raise ValueError(f"tabelas desconhecidas: {sorted(unknown)}")

    written: dict[str, int] = {}
    for table_id in selected:
        df = CLEANERS[table_id](workbook, uf_map)
        municipios = check_municipios(df, table_id, municipios)
        written[table_id] = write_partitioned(df, table_id, year, output_dir)

    return written


def upload_tables(
    output_dir: Path, dataset_id: str, tables: Iterable[str] | None = None
) -> None:
    """Sobe cada diretório de tabela para o staging de `basedosdados-dev`.

    O destino vem de `~/.basedosdados/config.toml`: os arquivos vão para
    `<bucket>/staging/<dataset_id>/<tabela>/` e a tabela externa nasce em
    `<projeto>.<dataset_id>_staging.<tabela>`. Produção não é tocada aqui — quem
    materializa `basedosdados.<dataset_id>.*` é o `table-approve` no merge.

    `source_format="csv"` é explícito por decisão: a convenção do repositório
    pede parquet todo STRING, mas o staging deste conjunto já é CSV desde as
    edições anteriores, e mudar o formato agora divergiria dos anos que já estão
    lá.

    `if_storage_data_exists="replace"` sobrescreve **arquivo por arquivo**, e não
    o prefixo inteiro: os anos que não estão em `output_dir` continuam onde
    estão. Por isso o nome do arquivo tem de bater com o que já está no bucket —
    a tabela externa lê tudo que houver no prefixo, e um nome diferente
    duplicaria a partição em silêncio. Aqui é `data.csv`, nos 19 anos.
    """
    wanted = set(tables) if tables is not None else None
    for directory in sorted(output_dir.iterdir()):
        if not directory.is_dir():
            continue
        if wanted is not None and directory.name not in wanted:
            continue
        table = bd.Table(dataset_id=dataset_id, table_id=directory.name)
        table.create(
            path=directory,
            source_format="csv",
            if_storage_data_exists="replace",
            if_table_exists="replace",
        )
