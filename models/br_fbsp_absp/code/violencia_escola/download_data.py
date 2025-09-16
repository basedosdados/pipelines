import requests


def download_data():
    url = "https://forumseguranca.org.br/wp-content/uploads/2023/07/anuario-2023.xlsx"
    response = requests.get(url)
    response.raise_for_status()

    # TODO fix table names
    with open("../input/anuario-2023.xlsx", "wb") as f:
        content_as_string = response.content
        f.write(content_as_string)

    print("Successfully downloaded")


if __name__ == "__main__":
    download_data()
