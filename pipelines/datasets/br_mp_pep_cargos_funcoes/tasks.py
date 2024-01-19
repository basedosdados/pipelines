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

import pandas as pd
import requests
from prefect import task
from selenium import webdriver
from selenium.common.exceptions import ElementNotInteractableException
from selenium.webdriver.common.by import By

from pipelines.datasets.br_mp_pep_cargos_funcoes.constants import constants
from pipelines.datasets.br_mp_pep_cargos_funcoes.utils import (
    get_normalized_values_by_col,
    move_from_tmp_dir,
    wait_file_download,
)
from pipelines.utils.metadata.utils import get_api_most_recent_date
from pipelines.utils.utils import log, to_partitions


@task
def setup_web_driver() -> None:
    r = requests.get(constants.CHROME_DRIVER.value, stream=True)
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        z.extractall(constants.PATH.value)

    os.environ["PATH"] += os.pathsep + constants.PATH.value


@task
def scraper(
    headless: bool = True,
    year_start: int = 1999,
    year_end: int = datetime.datetime.now().year,
) -> None:
    log(f"Scraper dates: from {year_start} to {year_end}")

    if not os.path.exists(constants.PATH.value):
        os.mkdir(constants.PATH.value)

    if not os.path.exists(constants.TMP_DATA_DIR.value):
        os.mkdir(constants.TMP_DATA_DIR.value)

    if not os.path.exists(constants.INPUT_DIR.value):
        os.mkdir(constants.INPUT_DIR.value)

    options = webdriver.ChromeOptions()

    # https://github.com/SeleniumHQ/selenium/issues/11637
    prefs = {
        "download.default_directory": constants.TMP_DATA_DIR.value,
        "download.prompt_for_download": False,
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
    driver.get(constants.TARGET.value)

    time.sleep(10)

    home_element = driver.find_element(
        By.XPATH, constants.XPATHS.value["card_home_funcoes"]
    )

    assert home_element.is_displayed()

    home_element.click()
    log("Card 'cargos e funções' clicked")

    time.sleep(15.0)

    tab_tabelas_element = driver.find_element(
        By.XPATH, constants.XPATHS.value["tabelas"]
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
    assert len(valid_selections) == len(
        [*constants.SELECTIONS_DIMENSIONS.value, *constants.SELECTIONS_METRICS.value]
    )

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
        time.sleep(5.0)
        elements = driver.find_elements(By.CLASS_NAME, "QvExcluded_LED_CHECK_363636")
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
        for selection in driver.find_elements(
            By.CLASS_NAME, "QvOptional_LED_CHECK_363636"
        )
        if selection.get_attribute("title") == "CCE & FCE"
    ]
    metrics_selection[0].click()
    log("CCE &  FCE clicked")

    time.sleep(3.0)

    _, *rest_metrics_selections = constants.SELECTIONS_METRICS.value
    metrics_selected: list[str] = []

    for _ in range(0, len(rest_metrics_selections)):
        # Wait for DOM changes
        time.sleep(5.0)
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
    time.sleep(3.0)

    def open_menu_years():
        years_elements = [
            selection
            for selection in driver.find_elements(By.TAG_NAME, "div")
            if selection.get_attribute("title") == "Ano (total)"
        ]
        assert len(years_elements) > 0
        years_elements[0].click()

    def element_select_year(year: int):
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

    def wait_hide_popup_element():
        popup_element_visible = True

        while popup_element_visible:
            elements_visible = [
                e
                for e in driver.find_elements(By.CLASS_NAME, "popupMask")
                if e.get_attribute("style") is not None
                and "display: block" in e.get_attribute("style")
            ]
            if len(elements_visible) == 0:
                break

    years = range(year_start, year_end + 1)

    for year in years:
        time.sleep(3.0)

        log(f"Starting download process for {year}")

        open_menu_years()

        time.sleep(3.0)

        element_year = element_select_year(year)

        element_year.click()

        time.sleep(10)

        download = [
            element
            for element in driver.find_elements(By.CLASS_NAME, "QvCaptionIcon")
            if element.get_attribute("title") == "Send to Excel"
        ]

        download[0].click()

        log(f"Waiting for download {year}")
        if wait_file_download(year):
            move_from_tmp_dir(year)

            log(f"Downloaded file for {year}")

            modal = driver.find_element(By.CLASS_NAME, "ModalDialog")
            try:
                modal.click()
                log("Modal Clicked")
            except ElementNotInteractableException:
                log("Modal not found")

            wait_hide_popup_element()

            remove_selected_year = [
                e
                for e in driver.find_elements(By.CLASS_NAME, "QvSelected")
                if e.get_attribute("title") is not None
                and e.get_attribute("title") == str(year)
            ]
            remove_selected_year[0].click()
            log(f"Removed selected year {year}")
        else:
            raise Exception(f"Failed to download xlsx for {year}")

    log(f"Done. Files: {os.listdir(constants.INPUT_DIR.value)}")

    driver.close()


@task
def clean_data() -> pd.DataFrame:
    files = os.listdir(constants.INPUT_DIR.value)

    log(f"Input dir files: {len(files)}, {files}")

    dfs = [
        pd.read_excel(os.path.join(constants.INPUT_DIR.value, file), skipfooter=4)
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
def is_up_to_date() -> bool:
    options = webdriver.ChromeOptions()

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--crash-dumps-dir=/tmp")
    options.add_argument("--remote-debugging-port=9222")
    options.add_argument("--headless=new")

    driver = webdriver.Chrome(options=options)
    driver.get(constants.TARGET.value)

    time.sleep(10)

    # pattern for matching the year at the end of the string
    pattern = r"\b\d{4}$"

    div_with_last_date = [
        e
        for e in driver.find_elements(By.TAG_NAME, "div")
        if re.search(pattern, e.get_attribute("title").strip()) is not None  # type: ignore
    ][0]

    text = div_with_last_date.get_attribute("title")

    assert text is not None

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
