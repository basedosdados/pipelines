# -*- coding: utf-8 -*-
"""
Tasks for br_mp_pep_cargos_funcoes
"""

import os
import time
import requests
import zipfile
import io
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By

from prefect import task

from pipelines.utils.utils import log, to_partitions
from pipelines.datasets.br_mp_pep_cargos_funcoes.constants import constants
from pipelines.datasets.br_mp_pep_cargos_funcoes.utils import (
    wait_file_download,
    move_from_tmp_dir,
    get_normalized_values_by_col,
)


@task
def setup_web_driver() -> None:
    r = requests.get(constants.CHROME_DRIVER.value, stream=True)
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        z.extractall(constants.PATH.value)

    os.environ["PATH"] += os.pathsep + constants.PATH.value


@task
def scraper(
    headless: bool = True, year_start: int = 1999, year_end: int = datetime.now().year
) -> None:
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
            modal.click()
            log("Modal Clicked")

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
    dfs = [
        pd.read_excel(f"{constants.INPUT_DIR.value}/{file}", skipfooter=4)
        for file in os.listdir(constants.INPUT_DIR.value)
    ]

    df = pd.concat(dfs).rename(columns=constants.RENAMES.value, errors="raise")

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
        "nome_cor_origem_etnica",
        "cce_e_fce",
        "das_e_correlatas",
    ]

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
