"""Funções puras do tratamento do Censo Escolar.

Nenhuma função aqui depende de estado global: recebem caminho e arquitetura,
devolvem DataFrame. Um orquestrador — o script de uma edição ou uma task de
flow — compõe as chamadas.

Cinco seções, na ordem em que o dado passa por elas:

- **fonte** — baixa o zip e acha o arquivo de cada tabela;
- **arquitetura** — decide quais colunas da fonte entram e com que nome;
- **leitura** — lê cada arquivo já filtrado pelas colunas que interessam;
- **montagem** — junta os quatro arquivos e alinha com a tabela publicada;
- **saída** — escreve o CSV particionado e sobe para o staging.

O porquê das decisões está no README do conjunto.
"""

from __future__ import annotations

import zipfile
from io import StringIO
from pathlib import Path

import basedosdados as bd
import numpy as np
import pandas as pd
import requests
import urllib3.exceptions
from constants import (  # type: ignore
    ANO,
    CHAVE,
    DELIMITER,
    ENCODING,
    ID_UF_SIGLA,
    TABELAS,
)

# ---------------------------------------------------------------- fonte


def prepare_input(
    url: str, zip_path: Path, input_dir: Path, tabelas: tuple[str, ...]
) -> None:
    """Garante os CSVs do microdado em disco, baixando e extraindo se faltar.

    Cada etapa é pulada se já estiver feita: o zip tem 512 MB e a extração é
    demorada, então não vale repetir nenhuma das duas à toa.
    """
    try:
        for tabela in tabelas:
            find_csv(input_dir, tabela)
    except FileNotFoundError:
        download_zip(url, zip_path, input_dir)


def download_zip(url: str, zip_path: Path, extract_to: Path) -> None:
    """Baixa e extrai o zip do microdado, se ainda não estiver em disco.

    O certificado de download.inep.gov.br não valida, daí `verify=False`. São
    512 MB, e o servidor derruba a conexão com frequência.
    """
    if not zip_path.exists():
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        response = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
            verify=False,
            stream=True,
            timeout=600,
        )
        response.raise_for_status()

        zip_path.parent.mkdir(parents=True, exist_ok=True)
        with open(zip_path, "wb") as fd:
            for chunk in response.iter_content(chunk_size=1 << 20):
                fd.write(chunk)

    with zipfile.ZipFile(zip_path) as archive:
        archive.extractall(extract_to)


def find_csv(input_dir: Path, tabela: str, ano: int = ANO) -> Path:
    """Acha o CSV de uma tabela sem depender do sufixo de revisão.

    A republicação de julho de 2026 acrescentou `_V2` ao nome da pasta e ao de
    cada arquivo, e no Gestor Escolar o sufixo veio minúsculo. Procurar pelo
    começo do nome evita reescrever o caminho a cada revisão do INEP.

    Raises:
        FileNotFoundError: se não achar exatamente um arquivo.
    """
    prefixo = f"tabela_{tabela}_{ano}".lower()
    achados = [
        path
        for path in input_dir.glob("**/*.csv")
        if path.name.lower().startswith(prefixo)
    ]
    if len(achados) != 1:
        raise FileNotFoundError(
            f"esperava um arquivo começando com {prefixo!r} em {input_dir}, "
            f"achei {len(achados)}: {[p.name for p in achados]}"
        )
    return achados[0]


# ---------------------------------------------------------- arquitetura


def read_architecture_table(url_architecture: str) -> pd.DataFrame:
    """Lê a tabela de arquitetura publicada no Google Sheets."""
    url = url_architecture.replace("edit#gid=", "export?format=csv&gid=")
    architecture = pd.read_csv(
        StringIO(requests.get(url, timeout=60).content.decode("utf-8"))
    ).query("name != '(excluido)'")
    return architecture.replace(np.nan, "", regex=True).drop(
        columns=["Unnamed: 0", "Unnamed: 1"], errors="ignore"
    )


def colunas_da_edicao(architecture: pd.DataFrame, ano: int) -> pd.DataFrame:
    """Linhas da arquitetura cuja cobertura temporal ainda alcança `ano`.

    A arquitetura descreve a tabela em todos os anos, então guarda também as
    colunas aposentadas, e algumas delas vêm da mesma coluna de origem que uma
    corrente: `IN_PODER_PUBLICO_PARCERIA` alimenta `poder_publico_parceria`,
    que é a atual, e `conveniada_poder_publico`, publicada até 2023. Sem este
    filtro a coluna cai no nome aposentado e a atual sai inteiramente nula —
    foi o que 2025 publicou.
    """
    fim = architecture["temporal_coverage"].str.extract(r"\)\s*(\d{4})")[0]
    return architecture[fim.isna() | (fim.astype(float) >= ano)]


def renames_da_fonte(
    architecture: pd.DataFrame, columns: pd.Index | list[str]
) -> dict[str, str]:
    """original_name -> nome da Base dos Dados, só para o que o arquivo tem.

    Raises:
        ValueError: se um nome de origem ainda mapear para dois nomes da Base
            dos Dados, sinal de que a arquitetura tem duas linhas vivas para a
            mesma coluna e a escolha entre elas seria silenciosa.
    """
    renames: dict[str, str] = {}
    for row in architecture[["name", "original_name"]].to_dict("records"):
        origem, destino = row["original_name"], row["name"]
        if not origem or origem not in columns:
            continue
        if renames.get(origem, destino) != destino:
            raise ValueError(
                f"{origem} mapeia para {renames[origem]} e {destino}"
            )
        renames[origem] = destino
    return renames


# ---------------------------------------------------------------- leitura


def read_source_columns(path: Path) -> list[str]:
    """Nomes das colunas do arquivo, sem ler o resto dele."""
    return list(
        pd.read_csv(
            path, delimiter=DELIMITER, encoding=ENCODING, nrows=0
        ).columns
    )


def read_table(path: Path, architecture: pd.DataFrame) -> pd.DataFrame:
    """Lê um arquivo do microdado já com as colunas e os nomes da BD.

    Só carrega as colunas que a arquitetura pede: dos 290 campos da tabela de
    escola de 2025, por exemplo, 237 entram. Tudo é lido como texto, porque o
    staging é CSV e a tipagem acontece no modelo dbt.
    """
    renames = renames_da_fonte(architecture, read_source_columns(path))
    df = pd.read_csv(
        path,
        delimiter=DELIMITER,
        encoding=ENCODING,
        dtype="string",
        usecols=list(renames),
    )
    return df[list(renames)].rename(columns=renames, errors="raise")


# -------------------------------------------------------------- montagem


def build_escola(dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Junta os arquivos do microdado em uma linha por escola.

    A edição 2025 distribui escola, matrícula, turma e docente em arquivos
    separados, todos agregados por escola. A junção é pela esquerda, a partir
    da tabela de escola, que é quem define o universo.

    Os três arquivos secundários repetem onze colunas de identificação — ano,
    unidade da federação, município, rede, localização. Só entram as colunas
    que a tabela de escola ainda não tem, senão o pandas as duplicaria com
    sufixo `_x`/`_y` e elas não casariam mais com a tabela publicada.
    """
    escola = dfs["escola"]
    for nome in TABELAS[1:]:
        outro = dfs[nome]
        novas = [
            col
            for col in outro.columns
            if col == CHAVE or col not in escola.columns
        ]
        escola = escola.merge(outro[novas], on=CHAVE, how="left")
    return escola


def fill_sigla_uf(df: pd.DataFrame) -> pd.DataFrame:
    """Completa a sigla da unidade da federação pelo código do município.

    Em 2025 uma escola de Santa Maria (RS) veio com `SG_UF` em branco e o
    código do município preenchido. Como `sigla_uf` é coluna de partição, o
    `groupby` da escrita descartaria essa linha sem dizer nada.
    """
    return df.assign(
        sigla_uf=lambda d: d["sigla_uf"].fillna(
            d["id_municipio"].str[:2].map(ID_UF_SIGLA)
        )
    )


def load_table_columns(
    project_id: str, dataset_id: str, table_id: str, billing_project_id: str
) -> list[str]:
    """Colunas da tabela publicada, na ordem em que ela as tem.

    É o alvo do tratamento: o staging precisa ter as mesmas colunas, na mesma
    ordem, porque a tabela externa é CSV e casa coluna por posição. Lê o schema
    pela API de metadados, que não cria job no BigQuery e por isso funciona com
    a credencial de desenvolvimento.

    A arquitetura declara uma coluna a mais, `id_distrito`, que a tabela nunca
    publicou. Acrescentá-la aqui quebraria a leitura das partições de 2007 a
    2024, que já estão no bucket com o layout antigo.
    """
    from google.cloud import bigquery

    client = bigquery.Client(project=billing_project_id)
    table = client.get_table(f"{project_id}.{dataset_id}.{table_id}")
    return [field.name for field in table.schema]


def align_columns(
    df: pd.DataFrame,
    columns: list[str],
    nao_publicadas: tuple[str, ...] = (),
) -> tuple[pd.DataFrame, list[str]]:
    """Alinha a tabela com as colunas publicadas, preenchendo o que falta.

    Comparar conjuntos, e não quantidades: com `len(a) == len(b)` uma coluna
    que entra compensa uma que sai e a verificação passa com a tabela errada.

    Args:
        df: tabela montada.
        columns: colunas da tabela publicada, na ordem dela.
        nao_publicadas: colunas que a arquitetura declara e a tabela não tem,
            descartadas de propósito. Qualquer outra coluna a mais é erro.

    Returns:
        A tabela alinhada e a lista de colunas sem origem nesta edição.

    Raises:
        ValueError: se o tratamento produziu coluna que a tabela não tem e que
            não está declarada em `nao_publicadas`.
    """
    df = df.drop(columns=list(nao_publicadas), errors="ignore")

    sobrando = set(df.columns) - set(columns)
    if sobrando:
        raise ValueError(
            f"colunas que a tabela publicada não tem: {sorted(sobrando)}"
        )

    faltando = [col for col in columns if col not in df.columns]
    if faltando:
        df[faltando] = None
    return df[columns], faltando


# ---------------------------------------------------------------- saída


def write_partitioned(
    df: pd.DataFrame, table_id: str, ano: int, output_dir: Path
) -> int:
    """Escreve um CSV por UF em `<output>/<tabela>/ano=<ano>/sigla_uf=<uf>/`.

    Returns:
        Quantas linhas foram escritas.

    Raises:
        ValueError: se alguma linha não tem sigla da unidade da federação. O
            `groupby` a descartaria sem dizer nada, e uma escola sumiria da
            tabela.
    """
    if (sem_uf := df["sigla_uf"].isna().sum()) > 0:
        raise ValueError(f"{sem_uf} linhas sem sigla_uf, que é partição")

    written = 0
    for sigla_uf, partition in df.groupby("sigla_uf"):
        path = output_dir / table_id / f"ano={ano}" / f"sigla_uf={sigla_uf}"
        path.mkdir(parents=True, exist_ok=True)
        partition.drop(columns=["ano", "sigla_uf"]).to_csv(
            path / f"{table_id}.csv", index=False
        )
        written += len(partition)
    return written


def upload_table(output_dir: Path, dataset_id: str, table_id: str) -> None:
    """Sobe o diretório da tabela para o staging de `basedosdados-dev`.

    `if_storage_data_exists="replace"` sobrescreve **arquivo por arquivo**, e não
    o prefixo inteiro: os anos que não estão em `output_dir` continuam onde
    estão. Por isso o nome do arquivo tem de bater com o que já está no bucket —
    a tabela externa lê tudo que houver no prefixo, e um nome diferente
    duplicaria a partição em silêncio. Aqui é `escola.csv`, nos 19 anos.

    Produção não é tocada aqui — quem materializa `basedosdados.<dataset>.*` é o
    `table-approve` no merge.
    """
    table = bd.Table(dataset_id=dataset_id, table_id=table_id)
    table.create(
        path=output_dir / table_id,
        source_format="csv",
        if_storage_data_exists="replace",
        if_table_exists="replace",
    )
