# -*- coding: utf-8 -*-
import os
from time import sleep

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


def select_selection_download(website):
    abas_dic = [
        [2, 18, "tableau_mvi-downloadData"],
        [7, 2, "tableau_estupro-downloadData"],
        [12, 5, "tableau_patrimonial-downloadData"],
        [17, 1, "tableau_mvi_armas-downloadData"],
        [22, 1, "tableau_mvi_gastos-downloadData"],
        [27, 1, "tableau_mvi_desap-downloadData"],
        [32, 1, "tableau_mvi_pop_pris-downloadData"],
    ]

    click(website, By.CSS_SELECTOR, "a.dropdown-toggle")
    dropdown = website.find_element(By.CSS_SELECTOR, "ul.dropdown-menu")
    dropdown = dropdown.find_elements(By.CSS_SELECTOR, "li a")

    for aba_n, aba in enumerate(dropdown):
        sleep(2)
        website.execute_script("arguments[0].click();", aba)

        for n in range(abas_dic[aba_n][1]):
            try:
                click(
                    website,
                    By.CSS_SELECTOR,
                    f'[aria-owns="bs-select-{abas_dic[aba_n][0]}"]',
                )

                click(website, By.ID, f"bs-select-{abas_dic[aba_n][0]}-{n}")

                click(website, By.ID, abas_dic[aba_n][2])

            except Exception as Error:
                print(Error)
                break


def create_website():
    options = Options()
    options.add_argument("-headless")
    options.set_preference("browser.download.folderList", 2)
    options.set_preference("browser.download.manager.showWhenStarting", False)
    options.set_preference(
        "browser.download.dir", os.getcwd().replace("code", "input")
    )
    options.set_preference(
        "browser.helperApps.neverAsk.saveToDisk", "application/x-gzip"
    )

    website = webdriver.Firefox(options=options)
    wait = WebDriverWait(website, 10)

    website.get("http://forumseguranca.org.br:3838/")
    wait.until(
        EC.visibility_of_element_located((By.ID, "tableau_mvi-downloadData"))
    )

    return website


def click(website, by, match, time_sleep=2):
    where_click = website.find_element(by, match)
    sleep(time_sleep)
    website.execute_script("arguments[0].click();", where_click)


def download_data():
    website = create_website()
    select_selection_download(website)
    sleep(5)
    website.quit()


if __name__ == "__main__":
    download_data()
