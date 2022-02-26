"""
Tasks for br_ibge_ipca
"""

from prefect import task
from prefect.tasks.shell import ShellTask


@task
def crawler(ufs: list, method="multiprocess") -> None:
    """
    Uses _download_raw to download all partitions from a given list of ufs
    uf: acronym of a Brazilian state
    method: download method. Either multiprocess, multithreading or synchronous
    """
    ufs = [uf.upper() for uf in ufs]

    # nested dictionary where the upper level keys are the level of aggregation used in the available link. Links information inside the inner dictionary
    partition_info = {
        "partition": {
            "url": "https://opendatasus.saude.gov.br/dataset/covid-19-vacinacao/resource/301983f2-aa50-4977-8fec-cfab0806cb0b",
            "pattern": r"Dados\sCompletos\s-\sParte\s\d+",
        },
        "AC-MT": {
            "url": "https://opendatasus.saude.gov.br/dataset/covid-19-vacinacao/resource/5093679f-12c3-4d6b-b7bd-07694de54173?inner_span=True",
            "pattern": r"Dados\s[A-Z]+\s-\sParte\s\d+",
        },
        "PA-TO": {
            "url": "https://opendatasus.saude.gov.br/dataset/covid-19-vacinacao/resource/10aed154-04c8-4cf4-b78a-8f0fa1bc5af4?inner_span=True",
            "pattern": r"Dados\s[A-Z]+\s-\sParte\s\d+",
        },
    }

    ac_mt = [
        list(d.keys())[0]
        for d in _url_scrapper(
            partition_info["AC-MT"]["url"], partition_info["AC-MT"]["pattern"]
        )
    ]

    pa_to = [
        list(d.keys())[0]
        for d in _url_scrapper(
            partition_info["PA-TO"]["url"], partition_info["PA-TO"]["pattern"]
        )
    ]

    siglas_pa_to = set([re.search(r"[A-Z]{2}", name).group() for name in pa_to])
    siglas_ac_mt = set([re.search(r"[A-Z]{2}", name).group() for name in ac_mt])

    partitions_1=[]
    partitions_2=[]
    for uf in ufs:
      if uf in siglas_ac_mt:
          partitions_1.append(list(filter(lambda x: x.__contains__(uf), ac_mt)))
      elif uf in siglas_pa_to:
          partitions_2.append(list(filter(lambda x: x.__contains__(uf), pa_to)))
      else:
          raise ValueError(
              f"""The uf must correspond to am acronym of a Brazilian federation. '{uf}' was found
            If you type correctly, please check the MS website to see if the selected uf has available data for download.
            """
          )

    partitions_1 = list(chain(*partitions_1))   
    partitions_2 = list(chain(*partitions_2))   

    if method == "multiprocess":
      if len(ufs)>1:
        print(f"Started the download of {', '.join(ufs[:-1])} and {ufs[-1]} partitions")
      else:
        print(f"Started the download of {ufs[0]}'s partitions")
      start = time.perf_counter()
      
      with concurrent.futures.ProcessPoolExecutor() as executor:
          results = [
              executor.submit(_download_raw, "AC-MT", partition, partition_info)
              for partition in partitions_1
          ]
          for f in concurrent.futures.as_completed(results):
            print(f.result())

      with concurrent.futures.ProcessPoolExecutor() as executor:
          results = [
              executor.submit(_download_raw, "PA-TO", partition, partition_info)
              for partition in partitions_2
          ]
          for f in concurrent.futures.as_completed(results):
            print(f.result())

      end = time.perf_counter()
      total_time = end - start
      print(f"Total download time: " + str(datetime.timedelta(seconds=total_time)))
    elif method == "multithreading":
      if len(ufs)>1:
        print(f"Started the download of {', '.join(ufs[:-1])} and {ufs[-1]} partitions")
      else:
        print(f"Started the download of {ufs[0]}'s partitions")
      start = time.perf_counter()

      with concurrent.futures.ThreadPoolExecutor() as executor:
          results = [
              executor.submit(_download_raw, "AC-MT", partition, partition_info)
              for partition in partitions_1
          ]
          for f in concurrent.futures.as_completed(results):
            print(f.result())

      with concurrent.futures.ThreadPoolExecutor() as executor:
          results = [
              executor.submit(_download_raw, "PA-TO", partition, partition_info)
              for partition in partitions_2
          ]
          for f in concurrent.futures.as_completed(results):
            print(f.result())

      end = time.perf_counter()
      total_time = end - start
      print(f"Total download time: " + str(datetime.timedelta(seconds=total_time)))
    elif method == "synchronous":
      for uf in ufs:
        print(f"Started the download of {uf}'s partitions")
        start = time.perf_counter()
        for partition in partitions:
            _download_raw(group, partition, partition_info)
        end = time.perf_counter()
        total_time = end - start
        print(f"Total download time: " + str(datetime.timedelta(seconds=total_time)))
    else:
        raise ValueError(
            "method option should be either multiprocess, multithreading or synchronous"
        )

def _url_scrapper(url: str, pattern: str) -> list:
    """
    Get list of dictionaries where the values are the urls and the keys are the url's identifiers.
    url: source where the urls should be grabbed
    pattern: identifier's regex pattern
    """
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")

    all_links = soup.find_all("a")
    links = []
    for i in range(len(all_links)):
        text = all_links[i].text.strip()
        if bool(re.match(pattern, text)):
            link = all_links[i].attrs["href"]
            links.append({text: link})

    return links


def _download_raw(group: str, partition: str, partition_info: str) -> str:
    """
    Download raw files from MS website.
    group: group of files as defined in MS website (e.g., PA-TO is the group of files containing data from the states of PA to TO, ordered alphabetically)
    partition: identification of a particular file within the group. Also, accordingly to MS website.
    partition_info: dictionary containing information of all partitions (group's names, urls, and regex pattern)
    """
    partitions = _url_scrapper(
        url=partition_info[group]["url"], pattern=partition_info[group]["pattern"]
    )

    print(f'_url_scrapper used for {group}. Result:')
    for i in partitions:
      print(i)
    try:
        url = {
            list(d.keys())[0]: list(d.values())[0]
            for d in partitions
            if list(d.keys())[0] == partition
        }[partition]
        filename = partition.replace(" ", "_").replace("_-_", "_").lower()
        path = "/tmp/data/br_ms_vacinacao_covid19/input/" + filename + ".csv"
        r = requests.get(url, stream=True)
        if r.status_code == 200:
            with open(path, 'wb') as f:
                r.raw.decode_content = True
                shutil.copyfileobj(r.raw, f)
        return f"The {partition} raw files was downloaded."
    except Exception as e:
        print("The following error occurred:\n")
        print(e.__class__.__name__)
        print_exc()
