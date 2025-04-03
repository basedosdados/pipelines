# -*- coding: utf-8 -*-
"""
General purpose functions for the world_sofascore_competicoes_futebol project
"""
# pylint: disable=invalid-name,line-too-long

import concurrent.futures
import http.client
import json
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz
import requests
from bs4 import BeautifulSoup

from pipelines.utils.crawler_world_sofascore_competicoes_futebol.constants import (
    constants as sofascore_constants,
)
from pipelines.utils.utils import log, to_partitions


def form_row_match_halve(statistics: dict) -> dict:
    temp_df_tempo = pd.concat(
        [
            pd.DataFrame(table_json["statisticsItems"])
            for table_json in statistics["groups"]
        ]
    )

    mandante_columns = [f"mandante_{key}" for key in temp_df_tempo.key.values]
    visitante_columns = [
        f"visitante_{key}" for key in temp_df_tempo.key.values
    ]

    data = dict(zip(mandante_columns, temp_df_tempo.home.values)) | dict(
        zip(visitante_columns, temp_df_tempo.away.values)
    )

    return data


def form_row_match_halves(event_score: list, halves: list) -> list:
    # Primeiro index de halves são informaçoes de todos os tempos juntos. Ignoramos ele
    halves.pop(0)

    data = [
        event_score[tempo] | form_row_match_halve(estatisticas)
        for tempo, estatisticas in enumerate(halves)
    ]

    return data


def form_period_dict(response_event_short: dict, date_match: datetime) -> dict:
    data_event = {
        "ano": date_match.year,
        "id_partida": response_event_short["id_event"],
        "data": date_match.strftime("%Y-%m-%d"),
        "hora": date_match.strftime("%H:%M:%S"),
        "temporada": response_event_short["year"],
        "rodada": response_event_short["round"],
        "mandante_name": response_event_short["homeTeam"]["name"],
        "visitante_name": response_event_short["awayTeam"]["name"],
    }

    period_control = {
        "regular_period_list": list(),
        "extra_period_list": list(),
    }

    for period in range(1, 3):
        period_key = f"period{period}"
        period_extra_key = f"extra{period}"

        period_scores = {
            "mandante_score": response_event_short["homeScore"][period_key],
            "visitante_score": response_event_short["awayScore"][period_key],
            "tempo": period,
        }

        period_control["regular_period_list"].append(period_scores)

        if period_extra_key in response_event_short["homeScore"].keys():
            period_extra_scores = {
                "mandante_score": response_event_short["homeScore"][
                    period_extra_key
                ],
                "visitante_score": response_event_short["awayScore"][
                    period_extra_key
                ],
                "tempo": period + 2,
            }

            period_control["extra_period_list"].append(period_extra_scores)

    all_period = (
        period_control["regular_period_list"]
        + period_control["extra_period_list"]
    )

    all_period = [data_event | period_data for period_data in all_period]

    return all_period


def get_event_score(event_dict: dict) -> list:
    brasil_timezone = pytz.timezone("America/Sao_Paulo")

    conn = http.client.HTTPConnection("www.sofascore.com")

    conn.request("GET", f"/api/v1/event/{event_dict['id_event']}")

    response = conn.getresponse()

    response_event = json.loads(response.read().decode())

    response_event_short = response_event["event"]

    date_match = (
        datetime.fromtimestamp(response_event_short["startTimestamp"])
        .replace(tzinfo=pytz.UTC)
        .astimezone(brasil_timezone)
    )

    response_event_short = response_event_short | event_dict
    data_period_events = form_period_dict(response_event_short, date_match)

    return data_period_events


def get_statistics_rows_match_halves(event_dict: dict) -> list | bool:
    # Existe a possibilidade da primeira requisição voltar vazia.
    for _try_ in range(2):
        try:
            url_statistics = f"https://www.sofascore.com/api/v1/event/{event_dict['id_event']}/statistics"

            response_json_statistics = requests.get(url_statistics).json()

            data_event = get_event_score(event_dict)

            statistics_rows = form_row_match_halves(
                data_event, response_json_statistics["statistics"]
            )

            return statistics_rows

        except Exception:
            pass

    return False


def get_rounds_from_season(
    tournament_id: str | int, season_id: str | int, index_url: str | int
) -> dict:
    url_rounds = f"https://www.sofascore.com/api/v1/unique-tournament/{tournament_id}/season/{season_id}/events/last/{index_url}"

    response_json_rounds = requests.get(url_rounds).json()

    return response_json_rounds


def form_df_year(datas_rounds: list) -> pd.DataFrame:
    df_rounds_task = pd.DataFrame(datas_rounds, dtype=str)

    list_remove_percent = [
        "mandante_ballPossession",
        "visitante_ballPossession",
        "mandante_duelWonPercent",
        "visitante_duelWonPercent",
        "mandante_wonTacklePercent",
        "visitante_wonTacklePercent",
    ]

    list_selecet_value = [
        "mandante_accurateLongBalls",
        "visitante_accurateLongBalls",
        "mandante_accurateCross",
        "visitante_accurateCross",
        "mandante_groundDuelsPercentage",
        "visitante_groundDuelsPercentage",
        "mandante_aerialDuelsPercentage",
        "visitante_aerialDuelsPercentage",
        "mandante_dribblesPercentage",
        "visitante_dribblesPercentage",
    ]

    try:
        df_rounds_task = df_rounds_task[
            sofascore_constants.COLUNAS_DICT_ORDEM.value.keys()
        ]

    except KeyError:
        validador_colunas = pd.Series(
            sofascore_constants.COLUNAS_DICT_ORDEM.value.keys()
        )
        columns_to_fill = validador_colunas[
            ~validador_colunas.isin(df_rounds_task.columns)
        ].tolist()
        df_rounds_task[columns_to_fill] = ""
        df_rounds_task = df_rounds_task[
            sofascore_constants.COLUNAS_DICT_ORDEM.value.keys()
        ]

    df_rounds_task[list_selecet_value] = df_rounds_task[
        list_selecet_value
    ].map(lambda x: str(x).split("/")[0])
    df_rounds_task[list_remove_percent] = df_rounds_task[
        list_remove_percent
    ].map(lambda x: str(x).strip("%"))

    df_rounds_task.columns = (
        sofascore_constants.COLUNAS_DICT_ORDEM.value.values()
    )

    return df_rounds_task


def form_event_dict(round_dict: dict) -> dict:
    event_dict = {"id_event": round_dict["id"]}

    event_dict["round"] = round_dict["roundInfo"]["round"]

    if round_dict["roundInfo"].get("name"):
        event_dict["round"] = round_dict["roundInfo"]["name"]

    return event_dict


def form_extract_season(
    tournament_id: str | int, season_dict: dict, games_count: int
) -> pd.DataFrame:
    season_id = season_dict["id"]
    # Pega os rounds do evento
    rounds_raw = [
        get_rounds_from_season(tournament_id, season_id, index_url)
        for index_url in range(games_count)
    ]
    # Filtramos apenas os rounds que estão pronto para coleta
    rounds = [round for round in rounds_raw if "events" in round.keys()]

    try:
        id_events = [
            season_dict | form_event_dict(short_round_temp)
            for round_temp in rounds
            for short_round_temp in round_temp["events"]
            if short_round_temp["homeScore"]
        ]

        log(f"Vão ser coletados: {len(id_events)} do ID {season_id}")

        with concurrent.futures.ThreadPoolExecutor() as executor:
            tasks_done = [
                rounds_task
                for rounds_task in executor.map(
                    get_statistics_rows_match_halves, id_events
                )
                if rounds_task
            ]
            datas_rounds = [
                round_task
                for rounds_task in tasks_done
                for round_task in rounds_task
            ]

        log(f"ID {season_id} coletado com sucesso!")

        return form_df_year(datas_rounds)

    except KeyError:
        log("Problemas com encontrar Round para extrair")


def extract_season(table_id: str) -> pd.DataFrame:
    url_base = sofascore_constants.MODE_TO_PROJECT_DICT.value[table_id]

    tournament_id = url_base.split("/")[-1]

    response_campeonato = requests.get(url_base)

    soup_campeonato = BeautifulSoup(response_campeonato.text)

    json_campeonato = json.loads(soup_campeonato.find(id="__NEXT_DATA__").text)

    short_json = json_campeonato["props"]["pageProps"]["initialProps"]

    temporadas = short_json["seasons"]

    games_count = short_json["seo"]["gamesCount"]  # Numero para paginação
    games_count = games_count / 30  # A paginas retornam no maximo 30 eventos
    games_count = int(games_count) + (games_count > int(games_count))  #

    current_season = max(temporadas, key=lambda row: row["year"])

    df_seasons = form_extract_season(
        tournament_id, current_season, games_count
    )

    df_seasons.rodada = df_seasons.rodada.str.lower().replace(
        sofascore_constants.TRANSLETE.value
    )

    df_seasons.rodada = df_seasons.rodada.str.title()

    return df_seasons


def preparing_data_and_max_date(table_id: str) -> tuple[str, str]:
    base_path = Path(tempfile.gettempdir(), "data")

    df_season = extract_season(table_id)

    df_current_year = df_season[df_season["ano"] == df_season["ano"].max()]

    max_date = df_season["data"].max()

    to_partitions(
        df_current_year,
        partition_columns=[
            "ano",
        ],
        savepath=base_path,
    )

    return max_date, base_path
