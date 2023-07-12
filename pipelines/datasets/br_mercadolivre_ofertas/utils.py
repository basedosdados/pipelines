# -*- coding: utf-8 -*-
""" Utility functions for the Mercado Livre Ofertas dataset. """

import asyncio
import hashlib
import re
from datetime import datetime

import requests
from tqdm import tqdm
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import Levenshtein
import pandas as pd

ua = UserAgent()


def retry(content_function):
    """Decorator function that retries an asynchronous content retrieval function.

    Args:
        content_function (callable): An asynchronous function that takes BeautifulSoup object and additional keyword arguments as parameters, and returns the desired content.

    Returns:
        callable: A wrapper function that retries the content retrieval function.

    Raises:
        None

    Example:
        @retry
        async def get_title(soup):
            # Retrieves the title from a BeautifulSoup object
            title = soup.title.string
            return title

        # Usage
        url = 'https://example.com'
        title = await get_title(url)
    """

    async def wrapper(url, attempts=2, wait_time=2, **kwargs):
        content = None
        count = 0

        while content is None and count < attempts:
            # print(f'Attempt {count + 1} of {attempts}')
            headers = {"User-Agent": "Chrome/39.0.2171.95"}
            try:
                response = await asyncio.to_thread(
                    requests.get, url, headers=headers, timeout=100
                )
            except Exception:
                return None
            await asyncio.sleep(wait_time)
            soup = BeautifulSoup(response.text, "html.parser")
            try:
                content = content_function(soup, **kwargs)
            except Exception:
                if count == (attempts - 1):
                    # Could not get content
                    content = None
            count += 1
            # await asyncio.sleep(10)

        return content

    return wrapper


def generate_unique_id(text: str):
    """
    Generates a unique ID based on the given text.

    Args:
        text (str): The input text to generate the unique ID from.

    Returns:
        str: The generated unique ID as a 16-digit string.

    Raises:
        None
    """
    # Convert the string to bytes
    text = text.lower().strip().replace(" ", "")
    string_bytes = text.encode("utf-8")

    # Generate the SHA-256 hash
    hash_object = hashlib.sha256(string_bytes)
    hash_hex = hash_object.hexdigest()

    # Take the first 12 digits of the hexadecimal hash
    hash_digits = hash_hex[:12]

    # Convert the hexadecimal digits to an integer
    unique_id = int(hash_digits, 16)

    # Ensure the ID is positive
    unique_id = str(int(abs(unique_id)))
    # fill with zeros
    unique_id = unique_id.zfill(16)

    return unique_id


@retry
def get_byelement(soup, **kwargs):
    """
    Retrieves the content of an HTML element identified by the given attributes from a BeautifulSoup object.

    Args:
        soup (BeautifulSoup): The BeautifulSoup object representing the HTML document.
        **kwargs: Keyword arguments specifying the attributes to identify the HTML element.

    Returns:
        str: The text content of the identified HTML element, after removing leading and trailing whitespaces.

    Raises:
        None
    """
    content = soup.find(**kwargs).text.strip()

    return content


def get_id(input_string, dictionary):
    """
    Retrieves the value from a dictionary based on the input string, using the key with the closest Levenshtein distance.

    Args:
        input_string (str): The input string for which to find the closest matching key in the dictionary.
        dictionary (dict): The dictionary containing key-value pairs.

    Returns:
        Any: The value associated with the key that has the closest Levenshtein distance to the input string.
             Returns None if the dictionary is empty or no match is found.

    Raises:
        None
    """
    best_match = None
    min_distance = float("inf")

    for key in dictionary:
        distance = Levenshtein.distance(input_string.lower(), key.lower())
        if distance < min_distance:
            min_distance = distance
            best_match = key

    return dictionary.get(best_match)


def get_items_urls(url):
    """
    Retrieves the URLs of items from the given URL by scraping the HTML content.

    Args:
        url (str): The URL of the webpage containing the items.

    Returns:
        list: A list of URLs of the items found on the webpage.

    Raises:
        None
    """
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    items = soup.find_all(class_="promotion-item__link-container")

    items_urls = [item["href"] for item in items]

    return items_urls


@retry
def get_price(soup, **kwargs):
    """
    Retrieves the price value from the HTML content represented by a BeautifulSoup object.

    Args:
        soup (BeautifulSoup): The BeautifulSoup object representing the HTML document.
        **kwargs: Keyword arguments specifying additional attributes to identify the HTML element.

    Returns:
        str: The price value extracted from the identified HTML element.

    Raises:
        None
    """
    price = soup.find(itemprop="price")["content"]
    return price


@retry
def get_features(soup):
    """
    Retrieves the features from the HTML content represented by a BeautifulSoup object.

    Args:
        soup (BeautifulSoup): The BeautifulSoup object representing the HTML document.

    Returns:
        dict: A dictionary containing the extracted features, with keys as feature names and values as feature values.

    Raises:
        None
    """
    features = soup.find_all(
        class_="ui-pdp-variations__picker ui-pdp-variations__picker-single"
    )
    features_dict = {
        k: v for k, v in [feature.text.strip().split(":") for feature in features]
    }
    return features_dict


@retry
def get_features_seller(soup):
    span_elements = soup.find_all("span", class_="buyers-feedback-qualification")

    # Initialize an empty dictionary
    result_dict = {}

    # Extract the text and numbers from each <span> element
    for span in span_elements:
        text = span.text.split("(")[0].strip()
        number = int(span.text.split("(")[1].split(")")[0])
        result_dict[text] = number

    return result_dict


@retry
def get_seller_link(soup):
    """
    Retrieves the link to the seller from the HTML content represented by a BeautifulSoup object.

    Args:
        soup (BeautifulSoup): The BeautifulSoup object representing the HTML document.

    Returns:
        str: The link to the seller's page.

    Raises:
        None
    """
    class_seller = "ui-box-component ui-box-component-pdp__visible--desktop"
    seller_link = soup.find(class_=class_seller)
    seller_link = seller_link.find("a")
    seller_link = seller_link["href"]

    return seller_link


@retry
def get_categories(soup):
    """
    Retrieves the categories from the HTML content represented by a BeautifulSoup object.

    Args:
        soup (BeautifulSoup): The BeautifulSoup object representing the HTML document.

    Returns:
        list: A list of category names extracted from the HTML.

    Raises:
        None
    """
    categories = soup.find_all("a", class_="andes-breadcrumb__link")
    categories_list = [category.text.strip() for category in categories]
    return categories_list


@retry
def get_original_price(soup):
    """
    Retrieves the original price from the HTML content represented by a BeautifulSoup object.

    Args:
        soup (BeautifulSoup): The BeautifulSoup object representing the HTML document.

    Returns:
        float: The original price extracted from the HTML.

    Raises:
        None
    """
    s_element = soup.find("s", class_="andes-money-amount--previous")
    span_element = s_element.find("span", class_="andes-visually-hidden")
    text = span_element.get_text(strip=True).strip()
    parts = text.split()

    numerical_parts = [
        part for part in parts if part.isdigit() or part.replace(".", "", 1).isdigit()
    ]

    float_amount = float(".".join(numerical_parts))

    return float_amount


async def process_item_url(item_url, kwargs_list):
    """
    Processes an item URL by retrieving various information using asynchronous operations.

    Args:
        item_url (str): The URL of the item to process.
        kwargs_list (list): A list of keyword argument dictionaries for the 'get_byelement' function.

    Returns:
        dict: A dictionary containing the extracted information about the item.

    Raises:
        None
    """
    tasks = [
        get_byelement(url=item_url, attempts=5, wait_time=20, **kwargs)
        for kwargs in kwargs_list
    ]
    results = await asyncio.gather(*tasks)

    keys = ["title", "review_amount", "discount", "transport_condition", "stars"]

    info = dict(zip(keys, results))
    price = await get_price(item_url, attempts=2)
    info["price"] = price
    price_original = await get_original_price(item_url, attempts=2)
    info["price_original"] = price_original
    if info["title"] is not None:
        info["item_id_bd"] = generate_unique_id(info["title"])
    else:
        info["item_id_bd"] = None
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

    info["datetime"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    info["features"] = await get_features(item_url, attempts=2)
    info["item_url"] = item_url
    info["categories"] = await get_categories(item_url, attempts=2)
    return info


async def process_table(table, url, kwargs_list):
    """
    Processes a table of items by retrieving information for each item using asynchronous operations.

    Args:
        table (str): The name or identifier of the table.
        url (str): The URL of the webpage containing the items.
        kwargs_list (list): A list of keyword argument dictionaries for the 'process_item_url' function.

    Returns:
        list: A list of dictionaries containing the extracted information for each item.

    Raises:
        None
    """
    items_urls = get_items_urls(url)
    tasks = [process_item_url(item_url, kwargs_list) for item_url in items_urls]
    results = await asyncio.gather(*tasks)

    for result in results:
        result["site_section"] = table

    return results


async def main_item(dict_tables, kwargs_list):
    """
    Executes the main process by processing multiple tables and consolidating the results.

    Args:
        dict_tables (dict): A dictionary mapping table names or identifiers to their respective URLs.
        kwargs_list (list): A list of keyword argument dictionaries for the 'process_table' function.

    Returns:
        list: A list containing the consolidated results from processing all the tables.

    Raises:
        None
    """
    contents = []
    coroutines = [
        process_table(table, url, kwargs_list) for table, url in dict_tables.items()
    ]
    results = await asyncio.gather(*coroutines)
    for table_results in results:
        contents.extend(table_results)
    return contents


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


def clean_experience(x):
    try:
        result = re.findall(r"\d+", x)[0]
    except Exception as e:
        result = None

    return result
