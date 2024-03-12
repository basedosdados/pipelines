# -*- coding: utf-8 -*-
"""
Tasks for br_mp_pep_cargos_funcoes
"""
import datetime
import io
import os
import re
import time
import zipfile
from datetime import timedelta

import pandas as pd
import requests

from requests.exceptions import ConnectionError
from prefect import task
from selenium import webdriver
from selenium.common.exceptions import (
    ElementNotInteractableException,
    NoSuchElementException,
    StaleElementReferenceException,
    WebDriverException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement

from pipelines.constants import constants as c
from pipelines.datasets.br_mp_pep_cargos_funcoes.constants import constants
from pipelines.datasets.br_mp_pep_cargos_funcoes.utils import (
    get_normalized_values_by_col,
    try_find_element,
    try_find_elements,
)
from pipelines.utils.metadata.utils import get_api_most_recent_date
from pipelines.utils.utils import log, to_partitions


@task
def setup_web_driver() -> None:
    r = requests.get(constants.CHROME_DRIVER.value, stream=True)
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        z.extractall(constants.PATH.value)

    os.environ["PATH"] += os.pathsep + constants.PATH.value


@task(
    max_retries=c.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=c.TASK_RETRY_DELAY.value),
)
def scraper(
    headless: bool = True,
    year_start: int = 1999,
    year_end: int = datetime.datetime.now().year,
) -> list[tuple[int, str]]:
    log(f"Scraper dates: from {year_start} to {year_end}")

    if not os.path.exists(constants.PATH.value):
        os.mkdir(constants.PATH.value)

    if not os.path.exists(constants.INPUT_DIR.value):
        os.mkdir(constants.INPUT_DIR.value)

    options = webdriver.ChromeOptions()

    # https://github.com/SeleniumHQ/selenium/issues/11637
    prefs = {
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    options.add_experimental_option(
        "prefs",
        prefs,
    )

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--crash-dumps-dir=/tmp")
    options.add_argument("--remote-debugging-port=9222")
    # NOTE: A resolucao afeta a renderizacao dos elementos
    options.add_argument("--window-size=1920,1080")

    if headless:
        options.add_argument("--headless=new")

    driver = webdriver.Chrome(options=options)

    try:
        driver.get(constants.TARGET.value)
    except WebDriverException as e:
        log(e)
        time.sleep(10.0)
        driver.get(constants.TARGET.value)

    home_element = try_find_element(
        driver, By.XPATH, constants.XPATHS.value["card_home_funcoes"], timeout=60 * 4
    )

    time.sleep(5.0)
    home_element.click()
    log("Card 'cargos e funções' clicked")

    time.sleep(5.0)

    tab_tabelas_element = try_find_element(
        driver, By.XPATH, constants.XPATHS.value["tabelas"], timeout=60 * 4
    )

    assert tab_tabelas_element.is_displayed()

    tab_tabelas_element.click()

    log("Tabelas button clicked")

    time.sleep(4)

    selectables = driver.find_elements(By.CLASS_NAME, "QvOptional_LED_CHECK_363636")

    valid_selections = [
        selection
        for selection in selectables
        if selection.get_attribute("title")
        in [*constants.SELECTIONS_DIMENSIONS.value, *constants.SELECTIONS_METRICS.value]
    ]

    # NOTE: Nem sempre 'Mês e Cargos' esta selecionado
    first_selection_title = valid_selections[0].get_attribute("title")
    valid_selections[0].click()
    log(f"Selected {first_selection_title}")

    # Wait for DOM changes
    time.sleep(5)

    # NOTE: Em cada seleção o DOM sofre alterações. Para cada click é
    # preciso buscar os mesmo elementos e filtrar.
    # Embora a classe seja a mesma as referências são diferentes
    _, *rest_dimensions_selections = constants.SELECTIONS_DIMENSIONS.value
    dimensions_selected: list[str] = []

    for _ in range(0, len(rest_dimensions_selections)):
        # Wait for DOM changes
        time.sleep(9.0)
        elements = driver.find_elements(By.CLASS_NAME, "QvExcluded_LED_CHECK_363636")
        # TODO: improve this
        head, *_ = [
            selection
            for selection in elements
            if selection.get_attribute("title") in rest_dimensions_selections
            and selection.get_attribute("title") not in dimensions_selected
        ]

        year_title = head.get_attribute("title")
        assert isinstance(year_title, str)

        head.click()
        dimensions_selected.append(year_title)

    log(f"Selections for dimensions, {dimensions_selected=}")

    # Wait for DOM Changes
    time.sleep(5.0)

    metrics_selection = [
        selection
        for selection in driver.find_elements(By.CLASS_NAME, "QvOptional_LED_CHECK_363636")
        if selection.get_attribute("title") == "CCE & FCE"
    ]
    metrics_selection[0].click()
    log("CCE &  FCE clicked")

    time.sleep(3.0)

    _, *rest_metrics_selections = constants.SELECTIONS_METRICS.value
    metrics_selected: list[str] = []

    for _ in range(0, len(rest_metrics_selections)):
        # Wait for DOM changes
        time.sleep(9.0)
        elements = driver.find_elements(By.CLASS_NAME, "QvExcluded_LED_CHECK_363636")
        head, *_ = [
            selection
            for selection in elements
            if selection.get_attribute("title") in rest_metrics_selections
            and selection.get_attribute("title") not in metrics_selected
        ]

        metric_title = head.get_attribute("title")
        assert isinstance(metric_title, str)

        head.click()
        metrics_selected.append(metric_title)

    log(f"Selections for metrics, {metrics_selected=}")

    # Wait for DOM changes
    # NOTE: Depois que os campos são selecionados o menu sera renderizado
    time.sleep(3.0)

    def open_menu_years() -> None:
        years_elements = [
            selection
            for selection in driver.find_elements(By.TAG_NAME, "div")
            if selection.get_attribute("title") == "Ano (total)"
        ]
        assert len(years_elements) > 0
        years_elements[0].click()

    def element_select_year(year: int) -> WebElement:
        elements = [
            element
            for element in driver.find_elements(By.CLASS_NAME, "QvOptional")
            if element.get_attribute("title") is not None
        ]

        for e in elements:
            title = e.get_attribute("title")
            if title is not None and int(title) == year:
                return e

        elements_title = [i.get_attribute("title") for i in elements]

        raise Exception(
            f"Failed to select year {year}. Found {len(elements_title)} elements, {elements_title}"
        )

    def wait_for_export(xlsx_hrefs: list[tuple[int, str]], timeout=60 * 10) -> str:
        end_time = time.time() + timeout

        urls = [url for _, url in xlsx_hrefs]

        while time.time() < end_time:
            try:
                modal_text = driver.find_element(By.CLASS_NAME, "ModalDialog_Text")
                anchor = modal_text.find_element(By.TAG_NAME, "a")
                href = anchor.get_attribute("href")

                assert href is not None

                if href not in urls:
                    return href
            except (NoSuchElementException, StaleElementReferenceException):
                time.sleep(1.0)
                continue

        raise Exception("Timeout")

    years = range(year_start, year_end + 1)

    xlsx_hrefs: list[tuple[int, str]] = []

    for year in years:
        time.sleep(3.0)

        log(f"Starting download for {year}")

        open_menu_years()

        # Wait for render menu
        time.sleep(3.0)

        element_year = element_select_year(year)

        element_year.click()

        # NOTE: Depois de selecionar o ano temos que aguardar alguma renderizacao
        # Sem esse sleep o download nao funciona
        time.sleep(3.0)

        download, *_ = [
            element
            for element in driver.find_elements(By.CLASS_NAME, "QvCaptionIcon")
            if element.get_attribute("title") == "Send to Excel"
        ]

        download.click()
        log("Send to excel clicked")

        log("Exporting...")
        xlsx_href = wait_for_export(xlsx_hrefs)

        xlsx_hrefs.append((year, xlsx_href))

        log("XLSX Exported")

        try:
            modal = driver.find_element(By.CLASS_NAME, "ModalDialog_Body")
            button_ok = modal.find_element(By.TAG_NAME, "button")
            button_ok.click()
            log("ModalDialog Clicked")
        except ElementNotInteractableException:
            log("ModalDialog not found")

        # Wait for render
        time.sleep(2.0)

        remove_selected_year, *_ = [
            e
            for e in driver.find_elements(By.CLASS_NAME, "QvSelected")
            if e.get_attribute("title") is not None and e.get_attribute("title") == str(year)
        ]

        remove_selected_year.click()
        log("Removed selected year")

    log(f"XLSX URLs: {xlsx_hrefs}")

    driver.close()

    return xlsx_hrefs


@task
def download_xlsx(urls: list[tuple[int, str]]) -> None:
    def request_wrapper(url: str) -> requests.Response:
        attempt = 0
        while attempt < 5:
            try:
                return requests.get(url)
            except ConnectionError:
                attempt = attempt + 1
                time.sleep(10.0 * attempt)
                continue

        raise Exception(f"Failed to request at {url}")

    log(f"Download xlsx: {urls=}")
    for year, href in urls:
        response = request_wrapper(href)
        with open(os.path.join(constants.INPUT_DIR.value, f"{year}.xlsx"), "wb") as file:
            file.write(response.content)


@task
def clean_data() -> pd.DataFrame:
    files = os.listdir(constants.INPUT_DIR.value)

    log(f"Input dir files: {len(files)}, {files}")

    dfs = [
        pd.read_excel(
            os.path.join(constants.INPUT_DIR.value, file), skipfooter=4, engine="openpyxl"
        )
        for file in files
        if file.endswith(".xlsx")
    ]

    df = pd.concat(dfs).rename(columns=constants.RENAMES.value, errors="raise")

    df["raca_cor"] = df["raca_cor"].str.lower().str.title()
    df["escolaridade_servidor"] = df["escolaridade_servidor"].str.lower().str.title()

    replaces_by_col = get_normalized_values_by_col()

    for col in replaces_by_col:
        df[col] = df[col].replace(replaces_by_col[col])

    order_cols = [
        "ano",
        "mes",
        "funcao",
        "natureza_juridica",
        "orgao_superior",
        "escolaridade_servidor",
        "orgao",
        "regiao",
        "sexo",
        "nivel_funcao",
        "subnivel_funcao",
        "sigla_uf",
        "faixa_etaria",
        "raca_cor",
        "cce_e_fce",
        "das_e_correlatas",
    ]

    log("Clean data Finished")

    return df[order_cols]


@task
def make_partitions(df: pd.DataFrame) -> str:
    savepath = constants.OUTPUT_DIR.value

    if not os.path.exists(savepath):
        os.mkdir(savepath)

    to_partitions(
        data=df,
        partition_columns=["ano"],
        savepath=savepath,
    )
    return savepath


@task
def is_up_to_date(headless: bool = True) -> bool:
    options = webdriver.ChromeOptions()

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    # TODO: remove?
    options.add_argument("--crash-dumps-dir=/tmp")
    # TODO: remove?
    options.add_argument("--remote-debugging-port=9222")

    options.add_argument("--window-size=1920,1080")

    if headless:
        options.add_argument("--headless=new")

    driver = webdriver.Chrome(options=options)
    driver.get(constants.TARGET.value)

    attempts = 0
    max_attempts = 5

    date_element: WebElement | None = None

    # pattern for matching the year at the end of the string
    pattern = r"\b\d{4}$"

    while attempts < max_attempts:
        elements_div = try_find_elements(driver, By.TAG_NAME, "div", timeout=60 * 2)

        log(f"Found {len(elements_div)} divs. {attempts=}")

        elements_title = [i for i in elements_div if i.get_attribute("title") is not None]

        elements_with_valid_date = [
            e
            for e in elements_title
            if re.search(pattern, e.get_attribute("title").strip()) is not None  # type: ignore
        ]

        if len(elements_with_valid_date) > 0:
            date_element = elements_with_valid_date[0]
            break
        else:
            attempts = attempts + 1
            time.sleep(5.0)
            continue

    if date_element is None:
        raise Exception("Failed to get element with date")

    text = date_element.get_attribute("title").strip()  # type: ignore

    driver.close()

    months_names = {
        "Jan": 1,
        "Fev": 2,
        "Mar": 3,
        "Abr": 4,
        "Mai": 5,
        "Jun": 6,
        "Jul": 7,
        "Ago": 8,
        "Set": 9,
        "Out": 10,
        "Nov": 11,
        "Dez": 12,
    }

    month_text, year = text.split(" ")

    month = months_names[month_text]

    date_website = datetime.datetime(int(year), month, day=1)

    log(f"Last date website: {text}, parsed as {date_website}")

    last_date_in_api = get_api_most_recent_date(
        dataset_id="br_mp_pep", table_id="cargos_funcoes", date_format="%Y-%m"
    )

    log(f"Last date API: {last_date_in_api}")

    return last_date_in_api == date_website
