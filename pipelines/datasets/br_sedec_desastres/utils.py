"""Download e limpeza do br_sedec_desastres.

Funções puras: nada aqui importa Prefect no nível do módulo. Quem embala em
`@task` é o `tasks.py`. O `log()` vem de `pipelines.utils.utils` e resolve o
logger do Prefect só em tempo de chamada — dentro de uma task ele escreve no log
do run, fora dela cai no `logging` padrão —, então o módulo segue importável sem
o Prefect instalado.

O schema (ordem das colunas, tipos, nome de origem) vem de `constants.COLUNAS`,
não de arquivo — a planilha de arquitetura fica fora do repo, em `task_davi/`.
"""

import io
import shutil
import time
from datetime import date
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from pipelines.datasets.br_sedec_desastres.constants import constants
from pipelines.utils.utils import log

PA = {
    "INT64": pa.int64(),
    "FLOAT64": pa.float64(),
    "STRING": pa.string(),
    "DATE": pa.date32(),
    "BOOL": pa.bool_(),
}


# Abaixo disto a resposta não é o relatório: a página real passa de 250 KB, e o
# que o pod recebeu quando o WAF barrou tinha 165 bytes.
_HTML_MINIMO = 2000


# ── download ────────────────────────────────────────────────────────────────
def _checar_fonte() -> None:
    """Registra o que a fonte responde a requisições simples, sem browser.

    Existe para separar três causas que chegam ao selenium como o mesmo timeout:

    - **conteúdo do pedido** (User-Agent, headers, browser headless detectado):
      se esta checagem passar e o Chrome não, o barrado é o browser, e o caminho
      é montar o postback com ``requests`` em vez de dirigir o navegador;
    - **origem de rede**: se a raiz e o caminho do relatório levarem o mesmo
      erro, o negado é o IP de saída, e não há correção possível no código;
    - **regra por caminho**: se a raiz passar e só o relatório cair, a política
      é do path, e vale investigar sessão, ``Referer`` e ordem de navegação.

    O IP de saída vai junto no log para permitir comparar com uma origem que
    funciona — o mesmo pedido, do mesmo jeito, passa de outras redes.

    Nunca levanta: é diagnóstico, e falhar aqui não deve impedir a tentativa com
    o browser.
    """
    try:
        eco = requests.get("https://api.ipify.org", timeout=15)
        log(f"IP de saída do worker: {eco.text.strip()}")
    except Exception as erro:
        log(
            f"não consegui ler o IP de saída: {type(erro).__name__}: {erro}",
            "warning",
        )

    raiz = f"{urlparse(constants.BASE_URL.value).scheme}://{urlparse(constants.BASE_URL.value).netloc}/"
    for url in (raiz, constants.BASE_URL.value):
        try:
            resposta = requests.get(
                url,
                headers={"User-Agent": constants.USER_AGENT.value},
                timeout=30,
            )
            log(
                f"pré-checagem {url} → status={resposta.status_code} | "
                f"{len(resposta.content)} bytes | "
                f"server={resposta.headers.get('server')!r} | "
                f"content-type={resposta.headers.get('content-type')!r}"
            )
            if len(resposta.content) < _HTML_MINIMO:
                log(f"corpo de {url}: {resposta.text!r}", "warning")
        except Exception as erro:
            log(
                f"pré-checagem {url} falhou: {type(erro).__name__}: {erro}",
                "warning",
            )


def _chrome_options(download_dir: Path) -> webdriver.ChromeOptions:
    """Monta as opções do Chrome headless, baixando direto em ``download_dir``.

    Args:
        download_dir: Diretório onde o Chrome grava o export.

    Returns:
        Opções prontas para passar ao ``webdriver.Chrome``.
    """
    options = webdriver.ChromeOptions()
    # https://github.com/SeleniumHQ/selenium/issues/11637
    options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": str(download_dir),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        },
    )
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-first-run")
    options.add_argument("--no-sandbox")
    # Essencial no k8s: o /dev/shm do pod é pequeno e o Chrome morre sem isso.
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(f"user-agent={constants.USER_AGENT.value}")
    return options


def _click(driver, xpath: str, timeout: int, descricao: str) -> None:
    """Espera o elemento ficar clicável e clica nele.

    Ao estourar, registra quantos elementos casam o XPath antes de propagar o
    erro. É a informação que separa os dois modos de falha: zero significa que o
    elemento não está na página (bloqueio da fonte, página de erro ou HTML que
    mudou), enquanto um ou mais significa que ele existe e nunca ficou clicável
    (painel fechado, overlay por cima). Sem isso, os dois casos chegam como o
    mesmo ``TimeoutException`` sem contexto nenhum.

    Args:
        driver: WebDriver ativo.
        xpath: XPath do elemento.
        timeout: Segundos de espera até o elemento ficar clicável.
        descricao: Nome do elemento em português, usado nas mensagens de log.

    Raises:
        TimeoutException: Se o elemento não ficar clicável dentro de ``timeout``.
    """
    log(f"clicando em {descricao}")
    try:
        WebDriverWait(driver, timeout).until(
            ec.element_to_be_clickable((By.XPATH, xpath))
        ).click()
    except TimeoutException:
        casam = len(driver.find_elements(By.XPATH, xpath))
        log(
            f"timeout em {descricao} após {timeout}s: {casam} elemento(s) casam "
            f"o xpath | url={driver.current_url!r} | título={driver.title!r} | "
            f"html={len(driver.page_source)} bytes",
            "error",
        )
        raise


def _is_in_flight(path: Path) -> bool:
    """Diz se o Chrome ainda está escrevendo o arquivo.

    O Chrome usa dois esquemas de nome para transferência em andamento, e os dois
    precisam ser reconhecidos: ``<nome>.crdownload`` e um temporário oculto
    chamado ``.com.google.Chrome.XXXXXX``, que ele escreve e depois renomeia no
    lugar. Tratar o temporário como download pronto devolve um caminho que
    desaparece antes de dar tempo de renomear — e deixa o export de verdade
    sobrando, sem dono, para ser confundido com o arquivo da UF seguinte.

    Args:
        path: Uma entrada do diretório.

    Returns:
        True se for transferência em andamento, e não arquivo pronto.
    """
    return path.suffix == ".crdownload" or path.name.startswith(
        (".com.google.Chrome.", ".org.chromium.")
    )


def _read_ufs(driver) -> list[tuple[str, str]]:
    """Lê as opções do dropdown de estado como pares ``(sigla, nome)``.

    Os ``<li>`` clicáveis são rotulados pelo nome ("Acre"), enquanto o
    ``<select>`` nativo escondido tem a sigla ("AC") — por isso os dois são
    necessários: o nome para clicar, a sigla para nomear o arquivo baixado. Ler
    atributo de elemento escondido é permitido; só interagir com ele não é.

    Note que ``.text`` devolve string vazia em elemento escondido, daí o
    ``textContent``.

    Args:
        driver: WebDriver ativo, já na página do relatório.

    Returns:
        Um par ``(sigla, nome)`` por estado, na ordem da própria fonte.

    Raises:
        ValueError: Se o dropdown não tiver os 27 estados esperados.
    """
    xpath = constants.XPATHS.value["estado_select_oculto"]
    options = driver.find_elements(By.XPATH, f"{xpath}//option")
    ufs = [
        (
            option.get_attribute("value").strip(),
            (option.get_attribute("textContent") or "").strip(),
        )
        for option in options
    ]
    ufs = [(sigla, nome) for sigla, nome in ufs if sigla]
    if len(ufs) != 27:
        raise ValueError(f"esperava 27 estados, achei {len(ufs)}: {ufs}")
    return ufs


def _wait_for_download(
    download_dir: Path, timeout: int, seen: set[str]
) -> Path:
    """Bloqueia até o Chrome terminar de escrever um CSV **novo**.

    Um arquivo só conta como o download novo quando é ``.csv``, não é
    transferência em andamento (ver :func:`_is_in_flight`), não está em ``seen``,
    e nada mais está em andamento. Sondar isso é melhor que dormir um número fixo
    de segundos, que é o que deixa os outros crawlers de selenium do repo
    frágeis.

    O ``seen`` é o que permite usar isso dentro de um laço: o relatório é
    exportado uma vez por estado, então já na segunda volta o diretório tem um
    arquivo pronto, e "qualquer arquivo completo" devolveria o export do estado
    anterior.

    Args:
        download_dir: Diretório configurado como destino do Chrome.
        timeout: Segundos até desistir.
        seen: Nomes já baixados, que não contam como arquivo novo.

    Returns:
        Caminho do arquivo recém-baixado.

    Raises:
        TimeoutError: Se nenhum arquivo novo aparecer dentro de ``timeout``.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        entries = [path for path in download_dir.iterdir() if path.is_file()]
        in_flight = [path for path in entries if _is_in_flight(path)]
        new_files = [
            path
            for path in entries
            if not _is_in_flight(path)
            and path.suffix == ".csv"
            and path.name not in seen
        ]
        if new_files and not in_flight:
            log(f"download concluído: {new_files[0].name}")
            return new_files[0]
        time.sleep(1)
    # O conteúdo do diretório é o que separa "o clique não disparou download
    # nenhum" de "baixou mas travou pela metade" — os dois chegam aqui iguais.
    presentes = sorted(p.name for p in download_dir.iterdir() if p.is_file())
    log(
        f"timeout de download após {timeout}s | {len(seen)} já baixados | "
        f"no diretório: {presentes}",
        "error",
    )
    raise TimeoutError(
        f"nenhum arquivo novo em {download_dir} após {timeout}s "
        f"({len(seen)} já baixados)"
    )


def download_reconhecimentos_vigentes(input_dir: Path) -> Path:
    """Baixa o relatório "Reconhecimentos vigentes" do S2ID.

    Dirige um Chrome headless porque a fonte é uma aplicação JSF/PrimeFaces: o
    export sai de um postback de formulário atrelado a sessão e ``ViewState``,
    não de uma URL de download estável.

    Exporta uma vez **por estado**: o dropdown de estado do relatório não tem
    opção "todos" (27 opções, nenhuma vazia), então o retrato nacional é a
    concatenação de 27 downloads. Cada arquivo é renomeado para ``<sigla>.csv``,
    para a UF sobreviver mesmo que não venha como coluna do export.

    Args:
        input_dir: Diretório de destino; criado se não existir.

    Returns:
        O diretório com os 27 exports baixados.
    """
    xpaths = constants.XPATHS.value
    element_timeout = constants.ELEMENT_TIMEOUT.value
    download_timeout = constants.DOWNLOAD_TIMEOUT.value

    # Uma retentativa reaproveita o mesmo work_dir e o `seen` nasce vazio: um CSV
    # sobrando da tentativa anterior é devolvido por _wait_for_download antes de o
    # download novo começar (nesse instante nada está em andamento), e renomeado
    # para a UF errada. A checagem de `UF != sigla_uf` no build acusa isso, então
    # sem limpar o diretório o retry não tem como dar certo.
    shutil.rmtree(input_dir, ignore_errors=True)
    input_dir.mkdir(parents=True)

    _checar_fonte()

    log("resolvendo o chromedriver")
    service = ChromeService(ChromeDriverManager().install())
    log(f"chromedriver em {service.path}; abrindo o Chrome")

    driver = webdriver.Chrome(
        service=service,
        options=_chrome_options(input_dir),
    )
    try:
        log(f"carregando {constants.BASE_URL.value}")
        inicio = time.monotonic()
        driver.get(constants.BASE_URL.value)
        # Título e tamanho do HTML logo após o get: uma página de bloqueio ou de
        # erro se denuncia aqui, e não 120s depois num timeout sem contexto.
        html = driver.page_source
        if len(html) < _HTML_MINIMO:
            # Curto demais para ser o relatório, que passa de 250 KB. O HTML
            # inteiro cabe no log e é o que distingue rejeição de WAF, página de
            # erro do servidor e aplicação que não renderizou.
            log(f"html recebido na íntegra: {html!r}", "warning")
        log(
            f"página carregada em {time.monotonic() - inicio:.1f}s | "
            f"título={driver.title!r} | html={len(driver.page_source)} bytes"
        )

        _click(driver, xpaths["painel"], element_timeout, "painel de vigentes")

        ufs = _read_ufs(driver)
        log(f"{len(ufs)} estados a exportar")

        _click(
            driver,
            xpaths["todas_tipologias"],
            element_timeout,
            "checkbox de todas as tipologias",
        )

        seen: set[str] = set()
        for i, (sigla, nome) in enumerate(ufs, start=1):
            log(f"[{i}/{len(ufs)}] {sigla} ({nome})")
            inicio_uf = time.monotonic()

            _click(
                driver,
                xpaths["estado_widget"],
                element_timeout,
                f"widget de estado ({sigla})",
            )
            _click(
                driver,
                xpaths["estado_item"].format(uf_nome=nome),
                element_timeout,
                f"item {nome} do dropdown",
            )
            _click(
                driver,
                xpaths["exportar_csv"],
                element_timeout,
                f"botão Exportar CSV ({sigla})",
            )

            baixado = _wait_for_download(input_dir, download_timeout, seen)
            destino = input_dir / f"{sigla}.csv"
            baixado.rename(destino)
            seen.add(destino.name)
            log(
                f"[{i}/{len(ufs)}] {sigla}: {destino.stat().st_size} bytes em "
                f"{time.monotonic() - inicio_uf:.1f}s"
            )

        log(f"{len(seen)} arquivos em {input_dir}")
        return input_dir
    finally:
        driver.quit()


# ── transform ───────────────────────────────────────────────────────────────
def _parse_export(path: Path) -> tuple[pl.DataFrame, int]:
    """Lê um export de UF e devolve as linhas mais o total que a fonte declara.

    O export é um *relatório*, não um CSV simples. Verificado nos 27 arquivos em
    2026-08-05:

    - quatro linhas de preâmbulo (ministério, secretaria, título do relatório,
      ``UF: XX``) antes do cabeçalho de colunas, que sempre começa com ``UF;``;
    - toda linha de dados tem um ``;`` **sobrando** no fim, ou seja 7 campos
      contra 6 do cabeçalho — é isso que faz uma leitura ingênua falhar com
      "found more fields than defined in 'Schema'";
    - depois uma linha vazia e um rodapé
      ``Total de reconhecimentos vigentes: ;N;``.

    Vale guardar esse rodapé: o ``N`` é a contagem da própria fonte, então ele
    valida o parsing em vez de a gente só confiar nele.

    Args:
        path: Um export ``<sigla>.csv``.

    Returns:
        As linhas lidas e o total declarado no rodapé.
    """
    csv_rows = path.read_text(encoding="latin-1").splitlines()

    header_index = next(
        index for index, row in enumerate(csv_rows) if row.startswith("UF;")
    )

    total_rows = 0

    df_rows = []

    for row in csv_rows[header_index + 1 :]:
        if row == "":
            continue
        elif row.startswith("Total de reconhecimentos"):
            total_rows = int(row.split(";")[1])
        else:
            if row.endswith(";"):
                row = row[:-1]
            df_rows.append(row)

    data_in_buffer = io.StringIO("\n".join([csv_rows[header_index], *df_rows]))

    df = pl.read_csv(data_in_buffer, separator=";", infer_schema_length=0)

    return df, total_rows


def build_reconhecimentos_vigentes(input_dir: Path) -> pd.DataFrame:
    """Monta a tabela limpa a partir dos exports baixados.

    Concatena os 27 exports por UF, renomeia as colunas da fonte para os nomes do
    staging (mapeando pelo ``original_name`` de ``constants.COLUNAS``) e devolve
    as linhas prontas para o :func:`write_partitioned`.

    Args:
        input_dir: Diretório com os exports ``<sigla>.csv``, vindos de
            :func:`download_reconhecimentos_vigentes`.

    Returns:
        Uma linha por reconhecimento vigente no retrato.
    """
    sorted_files = sorted(input_dir.glob("*.csv"))

    if len(sorted_files) != 27:
        raise ValueError(
            f"esperava 27 CSVs em {input_dir}, achei {len(sorted_files)}"
        )

    dfs_polars = []
    total = 0

    for path in sorted_files:
        df, total_df = _parse_export(path)

        if len(df) != total_df:
            raise ValueError(
                f"{path.stem}: {len(df)} linhas != rodapé {total_df}"
            )

        df = df.with_columns(pl.lit(path.stem).alias("sigla_uf"))

        dfs_polars.append(df)

        total += total_df

    concated_dfs: pl.DataFrame = pl.concat(dfs_polars, how="vertical")

    if len(concated_dfs) != total:
        raise ValueError("Erro no download dos arquivos!")

    mismatched_values = concated_dfs.filter(pl.col("UF") != pl.col("sigla_uf"))

    if len(mismatched_values) > 0:
        raise ValueError(
            f"UF do CSV difere do nome do arquivo em {len(mismatched_values)} linhas: "
            f"{mismatched_values.select('UF', 'sigla_uf').unique().to_dicts()}"
        )

    concated_dfs = concated_dfs.drop("UF")

    renamed_cols = {
        col["original_name"]: constants.STAGING_SUBSTITUICOES.value.get(
            col["name"], col["name"]
        )
        for col in constants.COLUNAS.value
        if col["original_name"]
        and col["original_name"] not in ("UF", "COBRADE")
    }

    concated_dfs = concated_dfs.rename(renamed_cols)

    concated_dfs = concated_dfs.with_columns(
        pl.col("COBRADE").str.splitn(" - ", 2).alias("partes")
    ).unnest("partes")

    concated_dfs = concated_dfs.rename(
        {"field_0": "id_cobrade", "field_1": "nome_cobrade"}
    )

    concated_dfs = concated_dfs.drop("COBRADE")

    concated_dfs = concated_dfs.with_columns(
        pl.col("data_ocorrencia").str.to_date("%d/%m/%Y"),
        pl.col("data_vigencia").str.to_date("%d/%m/%Y"),
    )

    concated_dfs = concated_dfs.with_columns(
        pl.lit(date.today()).alias("data_extracao")
    )

    return concated_dfs.to_pandas()


def clean_all(input_dir: Path, output_dir: Path) -> dict:
    """Monta todas as tabelas a partir dos exports baixados.

    Args:
        input_dir: Diretório com os exports baixados.
        output_dir: Diretório raiz de saída.

    Returns:
        Um mapa de slug da tabela para o diretório particionado, mais
        ``"max_date"`` — a data do retrato, que alimenta o poll da fonte em
        ``flows.py``.
    """
    table = constants.TABLE_ID.value
    df = build_reconhecimentos_vigentes(input_dir)
    table_in_dir = write_partitioned(df, table, output_dir)
    return {
        table: table_in_dir,
        "max_date": df["data_extracao"].max().strftime("%Y-%m-%d"),
    }


def write_partitioned(df: pd.DataFrame, table: str, output_dir: Path) -> Path:
    """Grava a tabela como parquet Snappy todo STRING, particionado em hive.

    O staging é todo STRING por convenção da BD: o modelo dbt faz ``safe_cast``
    de cada coluna para o tipo real, e o ``pipelines.utils.gcs.dump_header``
    transforma em string o arquivo de cabeçalho de onde o BigQuery infere o
    schema do staging. Emitir parquet tipado contra esse schema STRING faz o
    BigQuery rejeitar os arquivos ("has type DOUBLE which does not match the
    target cpp_type STRING_PIECE").

    Os valores passam primeiro pelos tipos reais, para um inteiro serializar como
    ``"2024"`` e não ``"2024.0"``, e só então são convertidos para string via
    arrow — nunca com ``astype(str)``, que escreve NULL como o literal ``"nan"``
    e derrota o ``safe_cast`` do dbt.

    A ordem das colunas vem de ``constants.COLUNAS`` com
    ``STAGING_SUBSTITUICOES`` aplicado: ``COLUNAS`` descreve a tabela **final**, e
    uma de suas colunas (``id_municipio``) é produzida pelo modelo dbt, não
    carregada pelo parquet. Substituir em vez de pular mantém o descasamento
    explícito — um nome de coluna errado de verdade continua levantando
    ``KeyError``.

    Args:
        df: Linhas limpas, vindas de :func:`build_reconhecimentos_vigentes`.
        table: Slug da tabela, usado no caminho de saída.
        output_dir: Diretório raiz de saída.

    Returns:
        O diretório da tabela, ``<output_dir>/<table>/<partição>/data.parquet``.
    """
    arch = constants.COLUNAS.value
    subs = constants.STAGING_SUBSTITUICOES.value
    order = [subs.get(a["name"], a["name"]) for a in arch]
    typed_schema = pa.schema(
        [
            pa.field(subs.get(a["name"], a["name"]), PA[a["bigquery_type"]])
            for a in arch
        ]
    )
    string_schema = pa.schema([pa.field(nome, pa.string()) for nome in order])
    out = df[order]
    tdir = output_dir / table

    # Decidido: acumular histórico, um retrato por execução (ver README).
    partition_col = "data_extracao"
    for key, g in out.groupby(partition_col, sort=True):
        # Formatar a chave explicitamente: pandas não tem tipo `date`, então a
        # coluna Date do polars chega aqui como datetime64 e a chave do groupby é
        # um Timestamp. Interpolada direto, ela renderiza
        # "data_extracao=2026-08-05 00:00:00" — com espaço no nome do diretório,
        # o que quebra a partição hive no BigQuery.
        pdir = tdir / f"{partition_col}={key:%Y-%m-%d}"
        pdir.mkdir(parents=True, exist_ok=True)
        at = pa.Table.from_pandas(g, schema=typed_schema, preserve_index=False)
        at = at.cast(string_schema)
        pq.write_table(at, pdir / "data.parquet", compression="snappy")

    log(f"{table}: {len(out):,} linhas -> {tdir}")
    return tdir
