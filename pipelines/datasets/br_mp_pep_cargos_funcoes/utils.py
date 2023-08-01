# -*- coding: utf-8 -*-
"""
General purpose functions for the br_mp_pep_cargos_funcoes project
"""

###############################################################################
#
# Esse é um arquivo onde podem ser declaratas funções que serão usadas
# pelo projeto br_mp_pep_cargos_funcoes.
#
# Por ser um arquivo opcional, pode ser removido sem prejuízo ao funcionamento
# do projeto, caos não esteja em uso.
#
# Para declarar funções, basta fazer em código Python comum, como abaixo:
#
# ```
# def foo():
#     """
#     Function foo
#     """
#     print("foo")
# ```
#
# Para usá-las, basta fazer conforme o exemplo abaixo:
#
# ```py
# from pipelines.datasets.br_mp_pep_cargos_funcoes.utils import foo
# foo()
# ```
#
###############################################################################
import os
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
import requests
import zipfile
import io

from pipelines.datasets.br_mp_pep_cargos_funcoes.constants import constants
from pipelines.utils.utils import log


def download_driver_and_setup():
    r = requests.get(constants.CHROME_DRIVER)
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        z.extractall(constants.PATH)

    os.rename(f"{constants.PATH}/chromedriver", "/usr/bin/chromedriver")

    os.system("sudo chown root:root /usr/bin/chromedriver")
    os.system("sudo chmod +x /usr/bin/chromedriver")


def wait_file_download(year: int, timeout=60 * 6):
    start_time = time.time()
    end_time = start_time + timeout

    file_exists = False

    while not file_exists:
        time.sleep(1.0)
        if len(os.listdir(constants.TMP_DATA_DIR)) > 0:
            log(f"Time to download {year}, {time.time() - start_time} seconds")
            break
        if time.time() > end_time:
            raise Exception(f"Timeout to download xlsx for {year}")
        continue

    return True


def move_to_downloads(year: int):
    files = os.listdir(constants.TMP_DATA_DIR)
    assert len(files) == 1
    src = os.path.join(constants.TMP_DATA_DIR, files[0])
    dest_file_name = f"{str(year)}.xlsx"
    dest = os.path.join(constants.INPUT_DIR, dest_file_name)
    os.rename(src, dest)
    assert os.path.exists(dest)


def scraper(
    headless: bool = True, year_start: int = 199, year_end: int = datetime.now().year
):
    if not os.path.exists(constants.TMP_DATA_DIR):
        os.mkdir(constants.TMP_DATA_DIR)

    if not os.path.exists(constants.INPUT_DIR):
        os.mkdir(constants.INPUT_DIR)

    options = webdriver.ChromeOptions()

    # https://github.com/SeleniumHQ/selenium/issues/11637
    prefs = {
        "download.default_directory": constants.TMP_DATA_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    options.add_experimental_option(
        "prefs",
        prefs,
    )

    if headless:
        options.add_argument("--headless=new")

    driver = webdriver.Chrome(options=options)
    driver.get(constants.TARGET)

    time.sleep(10)

    home_element = driver.find_element(By.XPATH, constants.XPATHS["card_home_funcoes"])

    assert home_element.is_displayed()

    home_element.click()

    time.sleep(15.0)

    tab_tabelas_element = driver.find_element(By.XPATH, constants.XPATHS["tabelas"])

    assert tab_tabelas_element.is_displayed()

    tab_tabelas_element.click()

    time.sleep(4)

    # # Campos da "seção" que não estão secionados
    # assert len(driver.find_elements(By.CLASS_NAME, "QvExcluded_LED_CHECK_363636")) == 18

    # # Cargos e Funções por padrão esta selecionado
    # assert len(driver.find_elements(By.CLASS_NAME, "QvSelected_LED_CHECK_363636")) == 2

    # # Estão duplicados pq tem duas div com a mesma classe
    selectables = driver.find_elements(By.CLASS_NAME, "QvOptional_LED_CHECK_363636")
    # assert len(selectables) == 50

    valid_selections = [
        selection
        for selection in selectables
        if selection.get_attribute("title")
        in [*constants.SELECTIONS_DIMENSIONS, *constants.SELECTIONS_METRICS]
    ]
    # assert len(valid_selections) == len(SELECTIONS)

    # NOTE: nem sempre mes e cargos esta selecionado
    first_selection_title = valid_selections[0].get_attribute("title")
    valid_selections[0].click()
    log(f"{first_selection_title=}")

    # Wait for DOM changes
    time.sleep(5)

    # NOTE: Em cada seleção o DOM sofre alterações. Para cada click é
    # preciso buscar os mesmo elementos e filtrar.
    # Embora a classe seja a mesma as referências são diferentes
    _, *rest_dimensions_selections = constants.SELECTIONS_DIMENSIONS
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
        # print(f"Click for {year_title=}")

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
    log(f"{metrics_selection}")
    metrics_selection[0].click()
    log("CCE &  FCE clicked")

    time.sleep(3.0)

    _, *rest_metrics_selections = constants.SELECTIONS_METRICS
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
        # print(f"Click for {year_title=}")

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

        raise Exception(f"Failed to select year {year}. Found {elements=}")

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

        if wait_file_download(year):
            move_to_downloads(year)

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

            continue
        else:
            raise Exception(f"Failed to download xlsx for {year}")

    log("Done")
    log(f"Files: {os.listdir(constants.INPUT_DIR)}")

    driver.close()
