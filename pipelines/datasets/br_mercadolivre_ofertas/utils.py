# -*- coding: utf-8 -*-
""" Utility functions for the Mercado Livre Ofertas dataset. """

import asyncio
import hashlib
import re
from datetime import datetime
from pipelines.utils.tasks import log
import requests
from tqdm import tqdm
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import Levenshtein
import pandas as pd
from pipelines.datasets.br_mercadolivre_ofertas.decorators import retry
import json

ua = UserAgent()


# ! tratamento dos dados
def clean_experience(x):
    try:
        result = re.findall(r"\d+", x)[0]
    except Exception:
        result = None

    return result


# ! tratamento dos dados
def generate_unique_id(text: str):
    """
    Generates a unique ID based on the given text.

    Args:
        text (str): The input text to generate the unique ID from.

    Returns:
        str: The generated unique ID
    """
    # Convert the string to bytes
    text = text.lower().strip().replace(" ", "")
    string_bytes = text.encode("utf-8")

    # Generate the SHA-256 hash
    hash_object = hashlib.sha256(string_bytes)
    hash_hex = hash_object.hexdigest()

    # Convert the hexadecimal digits to an integer
    unique_id = int(hash_hex, 16)

    # Ensure the ID is positive
    unique_id = str(int(abs(unique_id)))
    # fill with zeros
    unique_id = unique_id.zfill(16)

    return unique_id


# ! tratamento dos dados
def get_id(input_string, dictionary):
    """
    Retrieves the value from a dictionary based on the input string, using the key with the closest Levenshtein distance.
    Args:
        input_string (str): The input string for which to find the closest matching key in the dictionary.
        dictionary (dict): The dictionary containing key-value pairs.

    Returns:
        Any: The value associated with the key that has the closest Levenshtein distance to the input string.
             Returns None if the input_string is not a string, the dictionary is empty, or no match is found.
    """
    if input_string is None:
        return None

    if not isinstance(input_string, str):
        return None

    best_match = None
    min_distance = float("inf")

    for key in dictionary:
        distance = Levenshtein.distance(input_string.lower(), key.lower())
        if distance < min_distance:
            min_distance = distance
            best_match = key

    return dictionary.get(best_match)


# ! função genérica na coleta de itens e vendedores
@retry
def get_byelement(soup, **kwargs):
    """
    Retrieves the content of an HTML element identified by the given attributes from a BeautifulSoup object.

    Args:
        soup (BeautifulSoup): The BeautifulSoup object representing the HTML document.
        **kwargs: Keyword arguments specifying the attributes to identify the HTML element.

    Returns:
        str: The text content of the identified HTML element, after removing leading and trailing whitespaces.
    """
    content = soup.find(**kwargs).text.strip()

    return content


# ! utilizado no processo da tabela de itens
def get_items_urls(urls):
    """
    Collects item URLs from the given table URL, performing HTML content extraction.
    Args:
        url (str): The URL of the webpage containing the items.

    Returns:
        list: A list of URLs of the items found on the webpage.
    """
    items_urls = []

    for url in urls:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")

        items = soup.find_all(class_="promotion-item__link-container")

        # Process each item and extract the URL
        for item in items:
            item_url = item["href"]
            items_urls.append(item_url)

    return items_urls


# ! utilizado no processo da tabela de itens
@retry
def get_features(soup):
    """
    Retrieves the features from the HTML content represented by a BeautifulSoup object.
    Args:
        soup (BeautifulSoup): The BeautifulSoup object representing the HTML document.

    Returns:
        dict: A dictionary containing the extracted features, with keys as feature names and values as feature values.
    """
    features = soup.find_all(
        class_="ui-pdp-container__row ui-vpp-highlighted-specs__attribute-columns__row"
    )
    features_dict = {
        k: v for k, v in [feature.text.strip().split(":") for feature in features]
    }
    return features_dict


# ! utilizado no processo da tabela de itens
@retry
def get_review(soup):
    script_elements = soup.find_all("script", type="application/ld+json")

    json_data = json.loads(script_elements[0].string)

    # Extrair o aggregateRating
    aggregate_rating = json_data.get("aggregateRating", {})
    stars = aggregate_rating.get("ratingValue")
    review_amount = aggregate_rating.get("reviewCount")

    if stars is None or review_amount is None:
        review_info = None
    else:
        review_info = {"stars": stars, "review_amount": review_amount}
    return review_info


# @retry
# def get_review(soup):
#     review_info_element = soup.find("span", class_="ui-pdp-review__amount")
#     review_amount = review_info_element.text.strip() if review_info_element else None

#     stars_element = soup.find("p", class_="ui-review-capability__rating__average ui-review-capability__rating__average--desktop")
#     stars = stars_element.text.strip() if stars_element else None
#     review_info = {"stars": stars, "review_amount": review_amount}
#     return review_info


@retry
def get_categories(soup):
    script_elements = soup.find_all("script", type="application/ld+json")
    categories = []
    # Loop through script elements and extract the desired content
    for script_content in script_elements:
        try:
            parsed_content = json.loads(script_content.string)
            if (
                "@type" in parsed_content
                and parsed_content["@type"] == "BreadcrumbList"
            ):
                for item in parsed_content["itemListElement"]:
                    categories.append(item["item"]["name"])
        except ValueError:
            pass

    return categories


# ! utilizado no processo da tabela de itens (coleta dos links de vendedores)
@retry
def get_seller_link(soup):
    """
    Retrieves the link to the seller from the HTML content represented by a BeautifulSoup object.
    """
    class_seller = "ui-box-component ui-box-component-pdp__visible--desktop"
    seller_link = soup.find(class_=class_seller)
    seller_link = seller_link.find("a")
    seller_link = seller_link["href"]

    return seller_link


@retry
def get_prices(soup, **kwargs):
    """
    Retrieves the price values from the HTML content represented by a BeautifulSoup object.
    Args:
        soup (BeautifulSoup): The BeautifulSoup object representing the HTML document.
        **kwargs: Keyword arguments specifying additional attributes to identify the HTML elements.

    Returns:
        dict: A dictionary containing the extracted price values.
    """
    price_element = soup.find(itemprop="price")
    original_price_element = soup.find("s", class_="andes-money-amount--previous")

    price = price_element["content"] if price_element else None

    if original_price_element:
        span_element = original_price_element.find(
            "span", class_="andes-visually-hidden"
        )
        text = span_element.get_text(strip=True).strip()
        parts = text.split()

        numerical_parts = [
            part
            for part in parts
            if part.isdigit() or part.replace(".", "", 1).isdigit()
        ]

        original_price = float(".".join(numerical_parts))
    else:
        original_price = None

    title_element = soup.find("h1", class_="ui-pdp-title")
    title = title_element.text.strip() if title_element else None

    discount_element = soup.find("span", class_="andes-money-amount__discount")
    discount = discount_element.text.strip() if discount_element else None

    transport_condition_element = soup.find(
        "p", class_="ui-pdp-color--BLACK ui-pdp-family--REGULAR ui-pdp-media__title"
    )
    transport_condition = (
        transport_condition_element.text.strip()
        if transport_condition_element
        else None
    )

    return {
        "price": price,
        "price_original": original_price,
        "title": title,
        "discount": discount,
        "transport_condition": transport_condition,
    }


# ! parte do processo da tabela de itens
async def process_item_url(item_url, kwargs_list):
    """
    Processes an item URL by retrieving various information using asynchronous operations.
    Args:
        item_url (str): The URL of the item to process.
    Returns:
        dict: A dictionary containing the extracted information about the item.
    """
    # tasks = [
    #     get_byelement(url=item_url, attempts=5, wait_time=20, **kwargs)
    #     for kwargs in kwargs_list
    # ]

    # results = await asyncio.gather(*tasks)
    # keys = ["review_amount", "stars"]
    # info = dict(zip(keys, results))
    info = {}
    review_info = await get_review(item_url, attempts=5, wait_time=10)
    prices = await get_prices(item_url, attempts=10, wait_time=20)

    if review_info is not None:
        info["stars"] = review_info["stars"]
        info["review_amount"] = review_info["review_amount"]
    else:
        info["stars"] = None
        info["review_amount"] = None

    info["price"] = prices["price"]
    info["price_original"] = prices["price_original"]
    info["title"] = prices["title"]
    info["discount"] = prices["discount"]
    info["transport_condition"] = prices["transport_condition"]
    log(info)

    # Gerando o ID item
    if info["title"] is not None:
        info["item_id_bd"] = generate_unique_id(info["title"])
    else:
        info["item_id_bd"] = None
    # Dados do vendedor
    seller_link = await get_seller_link(item_url, attempts=5, wait_time=20)
    info["seller_link"] = seller_link
    if info["seller_link"] is not None:
        seller = info["seller_link"]
        seller = " ".join(re.findall(r"([A-Z]+)+", seller.split("?")[0]))
        seller = seller.strip().title()
        info["seller_id"] = generate_unique_id(seller)
        info["seller"] = seller
    else:
        info["seller_id"] = None
        info["seller"] = None
    info["categories"] = await get_categories(item_url, attempts=2)
    info["datetime"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    info["features"] = await get_features(item_url, attempts=2)
    info["item_url"] = item_url
    return info


# ! parte do processo da tabela de itens
async def process_table(table, url, kwargs_list):
    """
    Processes a table of items by retrieving information for each item using asynchronous operations.
    Args:
        table (str): The name or identifier of the table.
        url (str): The URL of the webpage containing the items.
        kwargs_list (list): A list of keyword argument dictionaries for the 'process_item_url' function.

    Returns:
        list: A list of dictionaries containing the extracted information for each item.
    """
    log(f"Starting processing table '{table}'")
    items_urls = get_items_urls(url)
    tasks = [
        process_item_url(item_url, kwargs_list)
        for item_url in tqdm(items_urls, desc="link")
    ]
    results = await asyncio.gather(*tasks)

    for result in results:
        result["site_section"] = table
    log(f"Finished processing table '{table}'")

    return results


# ! processo da tabela de itens
async def main_item(dict_tables, kwargs_list):
    """
    Executes the main process by processing multiple tables and consolidating the results.
    Args:
        dict_tables (dict): A dictionary mapping table names or identifiers to their respective URLs.
        kwargs_list (list): A list of keyword argument dictionaries for the 'process_table' function.

    Returns:
        list: A list containing the consolidated results from processing all the tables.
    """
    contents = []
    coroutines = [
        process_table(table, url, kwargs_list) for table, url in dict_tables.items()
    ]
    results = await asyncio.gather(*coroutines)
    for table_results in results:
        contents.extend(table_results)
    return contents


# ! utilizado no processo da tabela de vendedor
@retry
def get_features_seller(soup):
    """
    Function to extract seller qualification information from <span> elements.
    """
    span_elements = soup.find_all("span", class_="buyers-feedback-qualification")

    # Initialize an empty dictionary
    result_dict = {}

    # Extract the text and numbers from each <span> element
    for span in span_elements:
        text = span.text.split("(")[0].strip()
        number = int(span.text.split("(")[1].split(")")[0])
        result_dict[text] = number

    return result_dict


# ! parte do processo da tabela de vendedor
async def get_seller_async(url, seller_id):
    kwargs_list = [
        {"class_": "experience"},
        {"class_": "seller-info__subtitle-sales"},
        {"class_": "message__title"},
        {"class_": "location__wrapper"},
    ]
    keys = ["experience", "reputation", "classification", "location"]
    tasks = [get_byelement(url=url, attempts=2, **kwargs) for kwargs in kwargs_list]
    results = await asyncio.gather(*tasks)
    info = {}
    info["title"] = (
        " ".join(re.findall(r"([A-Z]+)+", url.split("?")[0])).strip().title()
    )
    for key, value in dict(zip(keys, results)).items():
        info[key] = value
    info["opinions"] = await asyncio.gather(get_features_seller(url, attempts=2))
    info["date"] = datetime.now().strftime("%Y-%m-%d")
    info["seller_id"] = seller_id

    return info


# ! processo da tabela de vendedor
async def main_seller(seller_ids, seller_links, file_dest):
    # get list of unique sellers
    dict_id_link = dict(zip(seller_ids, seller_links))

    sellers = []
    for seller_id, link in tqdm(dict_id_link.items()):
        seller = await get_seller_async(link, seller_id)
        sellers.append(seller)

    # save sellers as a pandas dataframe
    df_sellers = pd.DataFrame(sellers)
    df_sellers = df_sellers.astype(str)
    df_sellers.to_csv(file_dest, index=False)
