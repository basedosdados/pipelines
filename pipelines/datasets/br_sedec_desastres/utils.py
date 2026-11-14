"""Download e limpeza das tabelas do br_sedec_desastres."""

import base64
import contextlib
import io
import shutil
import socket
import threading
import time
import unicodedata
from collections.abc import Callable, Generator
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import unquote, urlparse

import polars as pl
import requests
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from pipelines.datasets.br_sedec_desastres.constants import (
    constants,
    reconhecimentos_realizados,
    reconhecimentos_vigentes,
)
from pipelines.utils.utils import brasil_proxy_dict, brasil_proxy_url, log

_HTML_MINIMO = 2000


def _checar_fonte() -> None:
    """Registra no log o que a fonte responde a requisições simples, sem navegador.

    Nunca levanta exceção.
    """
    proxies = brasil_proxy_dict()
    log(f"proxy brasileiro {'em uso' if proxies else 'não configurado'}")

    raiz = f"{urlparse(constants.BASE_URL.value).scheme}://{urlparse(constants.BASE_URL.value).netloc}/"
    for url in (raiz, constants.BASE_URL.value):
        try:
            resposta = requests.get(
                url,
                headers={"User-Agent": constants.USER_AGENT.value},
                timeout=30,
                proxies=proxies,
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


def _repassa(origem: socket.socket, destino: socket.socket) -> None:
    """Copia bytes de um socket para o outro até a origem fechar.

    Args:
        origem: Socket de onde ler.
        destino: Socket para onde escrever.
    """
    try:
        while True:
            dados = origem.recv(65536)
            if not dados:
                break
            destino.sendall(dados)
    except OSError:
        pass
    finally:
        for lado in (origem, destino):
            with contextlib.suppress(OSError):
                lado.shutdown(socket.SHUT_RDWR)


def _injeta_credencial(cabecalho: bytes, credencial: str) -> bytes:
    """Acrescenta o ``Proxy-Authorization`` ao cabeçalho do pedido.

    Args:
        cabecalho: Cabeçalho do pedido, da linha de requisição até a linha em
            branco.
        credencial: Valor pronto do ``Proxy-Authorization``.

    Returns:
        O mesmo cabeçalho com a credencial, e sem uma anterior que houvesse.
    """
    linhas = [
        linha
        for linha in cabecalho.split(b"\r\n")
        if not linha.lower().startswith(b"proxy-authorization:")
    ]
    linhas.insert(1, f"Proxy-Authorization: {credencial}".encode())
    return b"\r\n".join(linhas)


def _handle_client(
    client: socket.socket, upstream: tuple[str | None, int], credential: str
) -> None:
    """Repassa ao Squid uma conexão do Chrome, com a credencial no cabeçalho.

    Args:
        client: Conexão aberta pelo Chrome com o repasse.
        upstream: Endereço ``(host, porta)`` do Squid.
        credential: Valor pronto do ``Proxy-Authorization``.
    """
    try:
        header = b""
        while b"\r\n\r\n" not in header:
            chunk = client.recv(65536)
            if not chunk:
                return
            header += chunk

        header, rest = header.split(b"\r\n\r\n", 1)
        squid = socket.create_connection(upstream, timeout=60)
        squid.settimeout(None)
        squid.sendall(
            _injeta_credencial(header, credential) + b"\r\n\r\n" + rest
        )

        threading.Thread(
            target=_repassa, args=(squid, client), daemon=True
        ).start()
        _repassa(client, squid)
    except OSError as error:
        log(
            f"repasse do proxy falhou: {type(error).__name__}: {error}",
            "warning",
        )
    finally:
        client.close()


def _accept_clients(
    server: socket.socket, upstream: tuple[str | None, int], credential: str
) -> None:
    """Aceita conexões no repasse até o socket fechar, uma thread por conexão.

    Args:
        server: Socket do repasse, já escutando.
        upstream: Endereço ``(host, porta)`` do Squid.
        credential: Valor pronto do ``Proxy-Authorization``.
    """
    while True:
        try:
            client, _ = server.accept()
        except OSError:
            return
        threading.Thread(
            target=_handle_client,
            args=(client, upstream, credential),
            daemon=True,
        ).start()


def _proxy_local(url_proxy: str) -> tuple[str, Callable[[], None]]:
    """Sobe em ``127.0.0.1`` um proxy que repassa ao Squid com a credencial.

    Args:
        url_proxy: URL do proxy, com usuário e senha.

    Returns:
        O endereço ``host:porta`` do repasse e a função que o encerra.
    """
    parts = urlparse(url_proxy)
    user = unquote(parts.username or "")
    password = unquote(parts.password or "")
    credential = (
        "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()
    )
    upstream = (parts.hostname, parts.port or 3128)

    server = socket.socket()
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", 0))
    server.listen(32)
    port = server.getsockname()[1]

    threading.Thread(
        target=_accept_clients,
        args=(server, upstream, credential),
        daemon=True,
    ).start()
    log(f"repasse do proxy ouvindo em 127.0.0.1:{port}")
    return f"127.0.0.1:{port}", server.close


def _chrome_options(
    download_dir: Path, endereco_proxy: str | None = None
) -> webdriver.ChromeOptions:
    """Monta as opções do Chrome headless, baixando direto em ``download_dir``.

    Args:
        download_dir: Diretório onde o Chrome grava o export.
        endereco_proxy: ``host:porta`` do repasse local, quando há proxy.

    Returns:
        Opções prontas para passar ao ``webdriver.Chrome``.
    """
    options = webdriver.ChromeOptions()
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
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(f"user-agent={constants.USER_AGENT.value}")

    if endereco_proxy:
        options.add_argument(f"--proxy-server=http://{endereco_proxy}")

    return options


@contextlib.contextmanager
def _open_report(
    input_dir: Path,
) -> Generator[webdriver.Chrome, None, None]:
    """Abre a página dos relatórios num Chrome headless que baixa em ``input_dir``.

    Recria ``input_dir`` vazio e faz a pré-checagem da fonte antes de abrir o
    Chrome. Na saída, fecha o Chrome e o repasse do proxy.

    Args:
        input_dir: Diretório de destino dos downloads.

    Yields:
        O WebDriver já na página dos relatórios.
    """
    shutil.rmtree(input_dir, ignore_errors=True)
    input_dir.mkdir(parents=True)

    _checar_fonte()

    proxy_url = brasil_proxy_url()
    proxy_address, close_proxy = (
        _proxy_local(proxy_url) if proxy_url else (None, lambda: None)
    )

    driver = None
    try:
        log("resolvendo o chromedriver")
        service = ChromeService(ChromeDriverManager().install())
        log(f"chromedriver em {service.path}; abrindo o Chrome")
        driver = webdriver.Chrome(
            service=service,
            options=_chrome_options(input_dir, proxy_address),
        )

        log(f"carregando {constants.BASE_URL.value}")
        started = time.monotonic()
        driver.get(constants.BASE_URL.value)
        html = driver.page_source
        if len(html) < _HTML_MINIMO:
            log(f"html recebido na íntegra: {html!r}", "warning")
        log(
            f"página carregada em {time.monotonic() - started:.1f}s | "
            f"título={driver.title!r} | html={len(html)} bytes"
        )

        yield driver
    finally:
        if driver is not None:
            driver.quit()
        close_proxy()


def _click(driver, xpath: str, timeout: int, descricao: str) -> None:
    """Espera o elemento ficar clicável e clica nele.

    No timeout, registra no log quantos elementos casam o XPath antes de
    propagar o erro.

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


def _fill_date(driver, xpath: str, value: date, timeout: int) -> None:
    """Escreve uma data num campo de calendário somente leitura.

    Args:
        driver: WebDriver ativo.
        xpath: XPath do ``<input>`` do calendário.
        value: Data a escrever.
        timeout: Segundos de espera até o campo existir.
    """
    field = WebDriverWait(driver, timeout).until(
        ec.presence_of_element_located((By.XPATH, xpath))
    )
    driver.execute_script(
        "arguments[0].value = arguments[1];", field, f"{value:%d/%m/%Y}"
    )


def _is_in_flight(path: Path) -> bool:
    """Diz se o arquivo é um download do Chrome ainda em andamento.

    Reconhece ``<nome>.crdownload`` e os temporários ocultos
    ``.com.google.Chrome.*`` e ``.org.chromium.*``.

    Args:
        path: Uma entrada do diretório.

    Returns:
        True se for transferência em andamento, e não arquivo pronto.
    """
    return path.suffix == ".crdownload" or path.name.startswith(
        (".com.google.Chrome.", ".org.chromium.")
    )


def _wait_for_download(
    download_dir: Path, timeout: int, seen: set[str]
) -> Path:
    """Espera o Chrome terminar de gravar um CSV que não esteja em ``seen``.

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


def _base_do_estado(driver) -> str:
    """Lê na página o id do widget de estado da vigentes.

    O id do widget é o do ``<select>`` oculto sem o sufixo ``_input``.

    Args:
        driver: WebDriver ativo, já na página do relatório.

    Returns:
        O id do widget, sem o sufixo ``_input``.

    Raises:
        ValueError: Se o select do estado não estiver na página.
    """
    achados = driver.find_elements(
        By.XPATH, reconhecimentos_vigentes.XPATHS.value["estado_select_oculto"]
    )
    if not achados:
        raise ValueError(
            "não achei o select de estado no painel de vigentes; a página mudou"
        )
    identificador = achados[0].get_attribute("id") or ""
    return identificador.removesuffix("_input")


def _read_ufs(driver) -> list[tuple[str, str]]:
    """Lê as opções do select oculto de estado como pares ``(sigla, nome)``.

    Args:
        driver: WebDriver ativo, já na página do relatório.

    Returns:
        Um par ``(sigla, nome)`` por estado, na ordem da própria fonte.

    Raises:
        ValueError: Se o dropdown não tiver os 27 estados esperados.
    """
    xpath = reconhecimentos_vigentes.XPATHS.value["estado_select_oculto"]
    options = driver.find_elements(By.XPATH, f"{xpath}/option")
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


def download_reconhecimentos_vigentes(input_dir: Path) -> Path:
    """Baixa o relatório "Reconhecimentos vigentes" do S2ID, um export por UF.

    Cada export é renomeado para ``<sigla>.csv``.

    Args:
        input_dir: Diretório de destino; recriado vazio.

    Returns:
        O diretório com os 27 exports baixados.
    """
    xpaths = reconhecimentos_vigentes.XPATHS.value
    element_timeout = constants.ELEMENT_TIMEOUT.value
    download_timeout = constants.DOWNLOAD_TIMEOUT.value

    with _open_report(input_dir) as driver:
        _click(driver, xpaths["painel"], element_timeout, "painel de vigentes")

        ufs = _read_ufs(driver)
        base_estado = _base_do_estado(driver)
        log(f"{len(ufs)} estados a exportar | widget de estado: {base_estado}")

        _click(
            driver,
            xpaths["todas_tipologias"],
            element_timeout,
            "checkbox de todas as tipologias",
        )

        seen: set[str] = set()
        for uf_number, (sigla, nome) in enumerate(ufs, start=1):
            log(f"[{uf_number}/{len(ufs)}] {sigla} ({nome})")
            started = time.monotonic()

            _click(
                driver,
                xpaths["estado_widget"].format(base=base_estado),
                element_timeout,
                f"widget de estado ({sigla})",
            )
            _click(
                driver,
                xpaths["estado_item"].format(base=base_estado, uf_nome=nome),
                element_timeout,
                f"item {nome} do dropdown",
            )
            _click(
                driver,
                xpaths["exportar_csv"],
                element_timeout,
                f"botão Exportar CSV ({sigla})",
            )

            downloaded = _wait_for_download(input_dir, download_timeout, seen)
            target = input_dir / f"{sigla}.csv"
            downloaded.rename(target)
            seen.add(target.name)
            log(
                f"[{uf_number}/{len(ufs)}] {sigla}: {target.stat().st_size} bytes em "
                f"{time.monotonic() - started:.1f}s"
            )

        log(f"{len(seen)} arquivos em {input_dir}")

    return input_dir


def recent_years(today: date) -> list[int]:
    """Devolve os anos que o flow recarrega: o corrente e, em janeiro, o anterior.

    Args:
        today: Data da execução.

    Returns:
        Os anos em ordem crescente.
    """
    return sorted({(today - timedelta(days=31)).year, today.year})


def build_windows(years: list[int], today: date) -> list[tuple[date, date]]:
    """Divide cada ano em semestres, sem passar do dia de hoje.

    Args:
        years: Anos a consultar.
        today: Data da execução, limite superior da última janela.

    Returns:
        Pares ``(primeiro dia, último dia)`` em ordem cronológica.

    Raises:
        ValueError: Se algum ano for anterior ao primeiro ano da fonte.
    """
    first_year = reconhecimentos_realizados.PRIMEIRO_ANO.value
    if min(years) < first_year:
        raise ValueError(
            f"a fonte começa em {first_year}; pedidos: {sorted(years)}"
        )

    windows = []
    for year in sorted(years):
        for first_day, last_day in (
            (date(year, 1, 1), date(year, 6, 30)),
            (date(year, 7, 1), date(year, 12, 31)),
        ):
            if first_day <= today:
                windows.append((first_day, min(last_day, today)))
    return windows


def download_reconhecimentos_realizados(
    input_dir: Path, windows: list[tuple[date, date]]
) -> Path:
    """Baixa o relatório "Reconhecimentos realizados" do S2ID, uma janela por arquivo.

    Cada export cobre o país inteiro ("Todas as tipologias" e "Todos os
    estados") no período da janela, e é renomeado para
    ``<AAAAMMDD primeiro dia>_<AAAAMMDD último dia>.csv``.

    Args:
        input_dir: Diretório de destino; recriado vazio.
        windows: Pares ``(primeiro dia, último dia)``, vindos de
            :func:`build_windows`.

    Returns:
        O diretório com um export por janela.
    """
    xpaths = reconhecimentos_realizados.XPATHS.value
    element_timeout = constants.ELEMENT_TIMEOUT.value
    download_timeout = constants.DOWNLOAD_TIMEOUT.value

    with _open_report(input_dir) as driver:
        _click(
            driver, xpaths["painel"], element_timeout, "painel de realizados"
        )
        _click(
            driver,
            xpaths["todas_tipologias"],
            element_timeout,
            "checkbox de todas as tipologias",
        )
        _click(
            driver,
            xpaths["todos_estados"],
            element_timeout,
            "checkbox de todos os estados",
        )

        seen: set[str] = set()
        for window_number, (first_day, last_day) in enumerate(
            windows, start=1
        ):
            log(
                f"[{window_number}/{len(windows)}] "
                f"{first_day:%d/%m/%Y} a {last_day:%d/%m/%Y}"
            )
            _fill_date(
                driver, xpaths["data_inicial"], first_day, element_timeout
            )
            _fill_date(driver, xpaths["data_final"], last_day, element_timeout)
            _click(
                driver,
                xpaths["exportar_csv"],
                element_timeout,
                f"botão Exportar CSV ({first_day:%Y-%m-%d})",
            )

            downloaded = _wait_for_download(input_dir, download_timeout, seen)
            target = input_dir / f"{first_day:%Y%m%d}_{last_day:%Y%m%d}.csv"
            downloaded.rename(target)
            seen.add(target.name)
            log(
                f"[{window_number}/{len(windows)}] {target.stat().st_size} bytes"
            )

    return input_dir


def _write_parquet(df: pl.DataFrame, columns: list[str], path: Path) -> None:
    """Grava ``df`` como parquet Snappy todo STRING, na ordem de ``columns``.

    Args:
        df: Linhas a gravar.
        columns: Nomes das colunas, na ordem do arquivo.
        path: Caminho do arquivo; o diretório é criado se não existir.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    df.select(
        [pl.col(column).cast(pl.String) for column in columns]
    ).write_parquet(path, compression="snappy")


def _parse_export(path: Path) -> tuple[pl.DataFrame, int]:
    """Lê um export de UF da vigentes e devolve as linhas e o total do rodapé.

    Args:
        path: Um export ``<sigla>.csv``.

    Returns:
        As linhas lidas e o total declarado em
        ``Total de reconhecimentos vigentes``.
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


def _read_vigentes_exports(input_dir: Path) -> pl.DataFrame:
    """Lê os 27 exports por UF da vigentes, conferindo cada um com o rodapé.

    Args:
        input_dir: Diretório com os exports ``<sigla>.csv``.

    Returns:
        As linhas de todos os exports, com ``sigla_uf`` tirada do nome do
        arquivo no lugar da coluna ``UF``.

    Raises:
        ValueError: Se não houver 27 exports, se um export divergir do próprio
            rodapé ou se a UF de uma linha divergir do nome do arquivo.
    """
    paths = sorted(input_dir.glob("*.csv"))
    if len(paths) != 27:
        raise ValueError(
            f"esperava 27 CSVs em {input_dir}, achei {len(paths)}"
        )

    dfs = []
    for path in paths:
        df, total = _parse_export(path)
        if len(df) != total:
            raise ValueError(
                f"{path.stem}: {len(df)} linhas != rodapé {total}"
            )
        dfs.append(df.with_columns(pl.lit(path.stem).alias("sigla_uf")))

    df = pl.concat(dfs, how="vertical")

    mismatched = df.filter(pl.col("UF") != pl.col("sigla_uf"))
    if len(mismatched) > 0:
        raise ValueError(
            f"UF do CSV difere do nome do arquivo em {len(mismatched)} linhas: "
            f"{mismatched.select('UF', 'sigla_uf').unique().to_dicts()}"
        )

    return df.drop("UF")


def build_reconhecimentos_vigentes(input_dir: Path) -> pl.DataFrame:
    """Monta a tabela vigentes a partir dos 27 exports por UF.

    Args:
        input_dir: Diretório com os exports ``<sigla>.csv``, vindos de
            :func:`download_reconhecimentos_vigentes`.

    Returns:
        Uma linha por reconhecimento vigente no retrato.
    """
    renames = reconhecimentos_vigentes.STAGING_SUBSTITUICOES.value

    df = _read_vigentes_exports(input_dir)

    df = df.rename(
        {
            column["original_name"]: renames.get(
                column["name"], column["name"]
            )
            for column in reconhecimentos_vigentes.COLUNAS.value
            if column["original_name"]
            and column["original_name"] not in ("UF", "COBRADE")
        }
    )

    df = (
        df.with_columns(pl.col("COBRADE").str.splitn(" - ", 2).alias("parts"))
        .unnest("parts")
        .rename({"field_0": "id_cobrade", "field_1": "nome_cobrade"})
        .drop("COBRADE")
    )

    return df.with_columns(
        pl.col("data_ocorrencia").str.to_date("%d/%m/%Y"),
        pl.col("data_vigencia").str.to_date("%d/%m/%Y"),
        pl.lit(date.today()).alias("data_extracao"),
    )


def write_partitioned(df: pl.DataFrame, table: str, output_dir: Path) -> Path:
    """Grava a tabela vigentes como parquet todo STRING, particionado por ``data_extracao``.

    A ordem das colunas é a de ``reconhecimentos_vigentes.COLUNAS``, com
    ``STAGING_SUBSTITUICOES`` aplicado.

    Args:
        df: Linhas limpas, vindas de :func:`build_reconhecimentos_vigentes`.
        table: Slug da tabela, usado no caminho de saída.
        output_dir: Diretório raiz de saída.

    Returns:
        O diretório da tabela, ``<output_dir>/<table>/<partição>/data.parquet``.
    """
    renames = reconhecimentos_vigentes.STAGING_SUBSTITUICOES.value
    columns = [
        renames.get(column["name"], column["name"])
        for column in reconhecimentos_vigentes.COLUNAS.value
    ]
    table_dir = output_dir / table

    for extraction_date in df["data_extracao"].unique().sort():
        partition_dir = table_dir / f"data_extracao={extraction_date:%Y-%m-%d}"
        rows = df.filter(pl.col("data_extracao") == extraction_date)
        _write_parquet(rows, columns, partition_dir / "data.parquet")

    log(f"{table}: {len(df):,} linhas -> {table_dir}")
    return table_dir


def clean_all(input_dir: Path, output_dir: Path) -> dict:
    """Monta e grava a tabela vigentes a partir dos exports baixados.

    Args:
        input_dir: Diretório com os exports baixados.
        output_dir: Diretório raiz de saída.

    Returns:
        Um mapa do slug da tabela para o diretório particionado, mais
        ``"max_date"``, a data do retrato.
    """
    table = reconhecimentos_vigentes.TABLE_ID.value
    df = build_reconhecimentos_vigentes(input_dir)
    table_dir = write_partitioned(df, table, output_dir)
    max_date = df.select(
        pl.col("data_extracao").max().dt.strftime("%Y-%m-%d")
    ).item()
    return {table: table_dir, "max_date": max_date}


def _parse_realizados(path: Path) -> tuple[pl.DataFrame, int]:
    """Lê um export de realizados e devolve as linhas e o total do rodapé.

    Args:
        path: Um export ``<primeiro dia>_<último dia>.csv``.

    Returns:
        As linhas lidas e o total declarado em
        ``Total de reconhecimentos realizados``.

    Raises:
        ValueError: Se o arquivo não tiver o rodapé de total.
    """
    csv_rows = path.read_text(encoding="latin-1").splitlines()

    header_index = next(
        index for index, row in enumerate(csv_rows) if row.startswith("Nº;UF;")
    )
    header = csv_rows[header_index]

    total_rows = None
    df_rows = []
    for row in csv_rows[header_index + 1 :]:
        if row == "" or row.startswith("Total de municípios"):
            continue
        if row.startswith("Total de reconhecimentos realizados"):
            total_rows = int(row.split(";")[1])
            continue
        if row.count(";") > header.count(";") and row.endswith(";"):
            row = row[:-1]
        df_rows.append(row)

    if total_rows is None:
        raise ValueError(f"{path.name}: sem o rodapé de total")

    data_in_buffer = io.StringIO("\n".join([header, *df_rows]))
    df = pl.read_csv(data_in_buffer, separator=";", infer_schema_length=0)
    return df, total_rows


def _read_realizados_exports(input_dir: Path) -> pl.DataFrame:
    """Lê os exports da realizados, conferindo cada um com o rodapé.

    Args:
        input_dir: Diretório com os exports ``<primeiro dia>_<último dia>.csv``.

    Returns:
        As linhas de todos os exports, com ``ano`` tirado do nome do arquivo.

    Raises:
        ValueError: Se um export divergir do próprio rodapé ou se nenhum
            reconhecimento vier.
    """
    dfs = []
    for path in sorted(input_dir.glob("*.csv")):
        df, total = _parse_realizados(path)
        if len(df) != total:
            raise ValueError(
                f"{path.stem}: {len(df)} linhas != rodapé {total}"
            )
        dfs.append(df.with_columns(pl.lit(int(path.stem[:4])).alias("ano")))

    if not dfs or sum(len(df) for df in dfs) == 0:
        raise ValueError(f"nenhum reconhecimento nos exports de {input_dir}")

    return pl.concat(dfs, how="vertical")


def _cobrade_key(name: str) -> str:
    """Reduz o nome de uma tipologia a maiúsculas sem acento, só letras e dígitos.

    Args:
        name: Nome da tipologia, como vem do export ou do painel.

    Returns:
        A chave de comparação.
    """
    without_accents = unicodedata.normalize("NFD", name.upper())
    return "".join(char for char in without_accents if char.isalnum())


def _add_cobrade(df: pl.DataFrame) -> pl.DataFrame:
    """Troca a coluna ``Desastre`` por ``id_cobrade`` e ``nome_cobrade``.

    O nome do export é comparado com os de ``reconhecimentos_realizados.COBRADE``
    pela chave de :func:`_cobrade_key`.

    Args:
        df: Linhas com a coluna ``Desastre``.

    Returns:
        As mesmas linhas, com ``id_cobrade`` e ``nome_cobrade`` no lugar de
        ``Desastre``.

    Raises:
        ValueError: Se algum nome não tiver código.
    """
    cobrade = reconhecimentos_realizados.COBRADE.value
    code_by_key = {_cobrade_key(name): code for code, name in cobrade.items()}

    df = df.with_columns(
        pl.col("Desastre")
        .map_elements(
            lambda name: code_by_key.get(_cobrade_key(name)),
            return_dtype=pl.String,
        )
        .alias("id_cobrade")
    )

    unmatched = df.filter(
        pl.col("Desastre").is_not_null() & pl.col("id_cobrade").is_null()
    )
    if len(unmatched) > 0:
        raise ValueError(
            "desastre sem código COBRADE: "
            f"{unmatched['Desastre'].unique().to_list()}"
        )

    return df.with_columns(
        pl.col("id_cobrade")
        .map_elements(cobrade.get, return_dtype=pl.String)
        .alias("nome_cobrade")
    ).drop("Desastre")


def build_reconhecimentos_realizados(input_dir: Path) -> pl.DataFrame:
    """Monta a tabela realizados a partir dos exports baixados.

    Args:
        input_dir: Diretório com os exports, vindos de
            :func:`download_reconhecimentos_realizados`.

    Returns:
        Uma linha por reconhecimento, nas colunas de
        ``reconhecimentos_realizados.COLUNAS``.
    """
    columns = reconhecimentos_realizados.COLUNAS.value

    df = _read_realizados_exports(input_dir)

    df = df.rename(
        {
            column["original_name"]: column["name"]
            for column in columns
            if column["original_name"]
            and column["original_name"] != "Desastre"
        }
    )

    df = _add_cobrade(df)

    df = df.with_columns(
        [
            pl.col(column["name"]).str.to_date("%d/%m/%Y")
            for column in columns
            if column["bigquery_type"] == "DATE"
        ]
    )

    return df.select([column["name"] for column in columns])


def write_realizados(df: pl.DataFrame, output_dir: Path) -> Path:
    """Grava a tabela realizados como parquet todo STRING, particionado por ``ano``.

    Args:
        df: Linhas limpas, vindas de :func:`build_reconhecimentos_realizados`.
        output_dir: Diretório raiz de saída.

    Returns:
        O diretório da tabela, ``<output_dir>/<tabela>/ano=<ano>/data.parquet``.
    """
    columns = [
        column["name"] for column in reconhecimentos_realizados.COLUNAS.value
    ]
    table_dir = output_dir / reconhecimentos_realizados.TABLE_ID.value

    for year in df["ano"].unique().sort():
        rows = df.filter(pl.col("ano") == year)
        _write_parquet(
            rows, columns, table_dir / f"ano={year}" / "data.parquet"
        )
        log(f"ano={year}: {len(rows):,} linhas")

    return table_dir


def clean_all_realizados(input_dir: Path, output_dir: Path) -> dict:
    """Monta e grava a tabela realizados a partir dos exports baixados.

    Args:
        input_dir: Diretório com os exports baixados.
        output_dir: Diretório raiz de saída.

    Returns:
        ``"data_path"``, o diretório particionado, e ``"max_date"``, a maior
        data de ``reconhecimentos_realizados.COLUNA_DATA``.
    """
    df = build_reconhecimentos_realizados(input_dir)
    data_path = write_realizados(df, output_dir)
    max_date = df.select(
        pl.col(reconhecimentos_realizados.COLUNA_DATA.value)
        .max()
        .dt.strftime("%Y-%m-%d")
    ).item()
    return {"data_path": str(data_path), "max_date": max_date}
