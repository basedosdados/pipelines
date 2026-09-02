"""Generate the architecture CSVs for us_dot_bts_ontime.

The CSVs are the source of truth for column names, order and types: the
cleaning transform, the dbt models and the metadata step all read them.
Regenerate with this script rather than editing the CSVs by hand.

    uv run --no-project python models/us_dot_bts_ontime/code/architecture/build_architecture.py

Naming follows the house rules for an English-language dataset: English
snake_case names, ``_id`` suffix for identifiers, and types assigned by
arithmetic meaning rather than by how the source stores the value.

Two consequences of that last rule are worth stating, because the raw file
looks numeric in both places:

* The eight HHMM clock fields are **STRING**. ``1659 + 1`` is not ``1700``, and
  INT64 would silently drop the leading zero on ``0937``. Four of them get a
  derived TIME companion, and scheduled departure additionally gets a DATETIME
  (see ``DERIVED`` below).
* ``Cancelled``, ``Diverted``, ``DepDel15`` and ``ArrDel15`` ship as ``0.00`` /
  ``1.00`` floats but are booleans, so they are STRING and dictionary-covered.
"""

from __future__ import annotations

import csv
from pathlib import Path

HERE = Path(__file__).resolve().parent

HEADER = [
    "name",
    "bigquery_type",
    "description",
    "temporal_coverage",
    "covered_by_dictionary",
    "directory_column",
    "measurement_unit",
    "has_sensitive_data",
    "observations",
    "original_name",
    "description_en",
    "description_es",
]

MIN = "minute"


def col(
    name,
    typ,
    pt,
    en,
    es,
    *,
    dic="no",
    direc="",
    unit="",
    sens="no",
    obs="",
    orig="",
    cov="",
):
    return {
        "name": name,
        "bigquery_type": typ,
        "description": pt,
        "temporal_coverage": cov,
        "covered_by_dictionary": dic,
        "directory_column": direc,
        "measurement_unit": unit,
        "has_sensitive_data": sens,
        "observations": obs,
        "original_name": orig,
        "description_en": en,
        "description_es": es,
    }


# ---------------------------------------------------------------------------
# flight
# ---------------------------------------------------------------------------
def _endpoint_block(side: str) -> list[dict]:
    """The nine origin/destination columns, which mirror each other exactly."""
    pt_side = "de origem" if side == "origin" else "de destino"
    en_side = "origin" if side == "origin" else "destination"
    es_side = "de origen" if side == "origin" else "de destino"
    p = "origin" if side == "origin" else "destination"
    o = "Origin" if side == "origin" else "Dest"

    return [
        col(
            f"{p}_airport_id",
            "STRING",
            f"Identificador numérico do aeroporto {pt_side} atribuído pelo US DOT",
            f"Numeric US DOT identifier of the {en_side} airport",
            f"Identificador numérico del aeropuerto {es_side} asignado por el US DOT",
            obs="Stable across time, unlike the airport code. Resolved by the `airport` "
            "table in this dataset; no US airport directory exists yet.",
            orig=f"{o}AirportID",
        ),
        col(
            f"{p}_airport_seq_id",
            "STRING",
            f"Identificador da versão datada do aeroporto {pt_side}",
            f"Identifier of the time-specific version of the {en_side} airport",
            f"Identificador de la versión fechada del aeropuerto {es_side}",
            obs="Changes when airport attributes such as name or coordinates change.",
            orig=f"{o}AirportSeqID",
        ),
        col(
            f"{p}_city_market_id",
            "STRING",
            f"Identificador do mercado urbano {pt_side} atribuído pelo US DOT",
            f"US DOT city market identifier of the {en_side}",
            f"Identificador del mercado urbano {es_side} asignado por el US DOT",
            obs="Consolidates airports serving the same city market.",
            orig=f"{o}CityMarketID",
        ),
        col(
            p,
            "STRING",
            f"Código do aeroporto {pt_side}",
            f"Code of the {en_side} airport",
            f"Código del aeropuerto {es_side}",
            dic="yes",
            obs="Airport codes can be reused over time; join on "
            f"{p}_airport_id for analysis across a range of years.",
            orig=o,
        ),
        col(
            f"{p}_city_name",
            "STRING",
            f"Cidade do aeroporto {pt_side}",
            f"City of the {en_side} airport",
            f"Ciudad del aeropuerto {es_side}",
            orig=f"{o}CityName",
        ),
        col(
            f"{p}_state_abbreviation",
            "STRING",
            f"Sigla da unidade federativa do aeroporto {pt_side}",
            f"State abbreviation of the {en_side} airport",
            f"Sigla del estado del aeropuerto {es_side}",
            dic="yes",
            obs="Includes Canadian provinces and US territories.",
            orig=f"{o}State",
        ),
        col(
            f"{p}_state_fips",
            "STRING",
            f"Código FIPS do estado do aeroporto {pt_side}",
            f"FIPS state code of the {en_side} airport",
            f"Código FIPS del estado del aeropuerto {es_side}",
            dic="yes",
            orig=f"{o}StateFips",
        ),
        col(
            f"{p}_state_name",
            "STRING",
            f"Nome do estado do aeroporto {pt_side}",
            f"State name of the {en_side} airport",
            f"Nombre del estado del aeropuerto {es_side}",
            orig=f"{o}StateName",
        ),
        col(
            f"{p}_world_area_code",
            "STRING",
            f"Código de área mundial do aeroporto {pt_side}",
            f"World area code of the {en_side} airport",
            f"Código de área mundial del aeropuerto {es_side}",
            dic="yes",
            orig=f"{o}Wac",
        ),
    ]


def _diversion_block(n: int) -> list[dict]:
    """The eight columns describing the n-th diverted landing."""
    return [
        col(
            f"diversion_{n}_airport",
            "STRING",
            f"Código do aeroporto do {n}º pouso de desvio",
            f"Airport code of diverted landing {n}",
            f"Código del aeropuerto del {n}º aterrizaje de desvío",
            dic="yes",
            orig=f"Div{n}Airport",
        ),
        col(
            f"diversion_{n}_airport_id",
            "STRING",
            f"Identificador do aeroporto do {n}º pouso de desvio",
            f"Airport identifier of diverted landing {n}",
            f"Identificador del aeropuerto del {n}º aterrizaje de desvío",
            orig=f"Div{n}AirportID",
        ),
        col(
            f"diversion_{n}_airport_seq_id",
            "STRING",
            f"Identificador datado do aeroporto do {n}º pouso de desvio",
            f"Time-specific airport identifier of diverted landing {n}",
            f"Identificador fechado del aeropuerto del {n}º aterrizaje de desvío",
            orig=f"Div{n}AirportSeqID",
        ),
        col(
            f"diversion_{n}_wheels_on_time",
            "STRING",
            f"Horário de pouso (hora local, HHMM) no {n}º aeroporto de desvio",
            f"Wheels-on time (local time, HHMM) at diverted airport {n}",
            f"Hora de aterrizaje (hora local, HHMM) en el {n}º aeropuerto de desvío",
            obs="Clock label in HHMM, not a quantity.",
            orig=f"Div{n}WheelsOn",
        ),
        col(
            f"diversion_{n}_total_gate_time",
            "FLOAT64",
            f"Tempo total fora do portão no {n}º aeroporto de desvio",
            f"Total ground time away from gate at diverted airport {n}",
            f"Tiempo total fuera de la puerta en el {n}º aeropuerto de desvío",
            unit=MIN,
            orig=f"Div{n}TotalGTime",
        ),
        col(
            f"diversion_{n}_longest_gate_time",
            "FLOAT64",
            f"Maior tempo contínuo fora do portão no {n}º aeroporto de desvio",
            f"Longest ground time away from gate at diverted airport {n}",
            f"Mayor tiempo continuo fuera de la puerta en el {n}º aeropuerto de desvío",
            unit=MIN,
            orig=f"Div{n}LongestGTime",
        ),
        col(
            f"diversion_{n}_wheels_off_time",
            "STRING",
            f"Horário de decolagem (hora local, HHMM) no {n}º aeroporto de desvio",
            f"Wheels-off time (local time, HHMM) at diverted airport {n}",
            f"Hora de despegue (hora local, HHMM) en el {n}º aeropuerto de desvío",
            obs="Clock label in HHMM, not a quantity.",
            orig=f"Div{n}WheelsOff",
        ),
        col(
            f"diversion_{n}_tail_number",
            "STRING",
            f"Prefixo da aeronave no {n}º pouso de desvio",
            f"Aircraft tail number at diverted landing {n}",
            f"Matrícula de la aeronave en el {n}º aterrizaje de desvío",
            orig=f"Div{n}TailNum",
        ),
    ]


HHMM_OBS = (
    "Clock label in HHMM as published, kept as STRING so the leading zero "
    "survives and no arithmetic is implied."
)

FLIGHT: list[dict] = [
    col(
        "year",
        "INT64",
        "Ano do voo programado",
        "Year of the scheduled flight",
        "Año del vuelo programado",
        direc="br_bd_diretorios_data_tempo.ano:ano",
        unit="year",
        obs="Partition column.",
        orig="Year",
    ),
    col(
        "quarter",
        "INT64",
        "Trimestre do voo programado, de 1 a 4",
        "Quarter of the scheduled flight, 1 to 4",
        "Trimestre del vuelo programado, de 1 a 4",
        unit="quarter",
        obs="Derivable from month; kept as published.",
        orig="Quarter",
    ),
    col(
        "month",
        "INT64",
        "Mês do voo programado, de 1 a 12",
        "Month of the scheduled flight, 1 to 12",
        "Mes del vuelo programado, de 1 a 12",
        direc="br_bd_diretorios_data_tempo.mes:mes",
        unit="month",
        orig="Month",
    ),
    col(
        "day_of_month",
        "INT64",
        "Dia do mês do voo programado",
        "Day of month of the scheduled flight",
        "Día del mes del vuelo programado",
        unit="day",
        orig="DayofMonth",
    ),
    col(
        "day_of_week",
        "STRING",
        "Dia da semana do voo programado, com 1 para segunda-feira",
        "Day of week of the scheduled flight, with 1 for Monday",
        "Día de la semana del vuelo programado, con 1 para lunes",
        dic="yes",
        obs="A weekday label rather than a quantity, so STRING.",
        orig="DayOfWeek",
    ),
    col(
        "flight_date",
        "DATE",
        "Data do voo programado, na hora local do aeroporto de origem",
        "Date of the scheduled flight, in origin-airport local time",
        "Fecha del vuelo programado, en hora local del aeropuerto de origen",
        orig="FlightDate",
    ),
    col(
        "reporting_carrier",
        "STRING",
        "Código único da empresa aérea que reportou o voo",
        "Unique carrier code of the airline that reported the flight",
        "Código único de la aerolínea que reportó el vuelo",
        dic="yes",
        obs="When a code has been reused, earlier users carry a numeric suffix "
        "such as PA(1). This is the field to use for analysis across a range "
        "of years.",
        orig="Reporting_Airline",
    ),
    col(
        "reporting_carrier_airline_id",
        "STRING",
        "Identificador numérico da empresa aérea atribuído pelo US DOT",
        "Numeric US DOT identifier of the reporting airline",
        "Identificador numérico de la aerolínea asignado por el US DOT",
        dic="yes",
        obs="One identifier per DOT certificate, stable regardless of code, "
        "name or holding company.",
        orig="DOT_ID_Reporting_Airline",
    ),
    col(
        "reporting_carrier_iata_code",
        "STRING",
        "Código IATA da empresa aérea que reportou o voo",
        "IATA code of the reporting airline",
        "Código IATA de la aerolínea que reportó el vuelo",
        obs="Not unique over time; the same code has been assigned to different "
        "carriers. Use reporting_carrier for analysis.",
        orig="IATA_CODE_Reporting_Airline",
    ),
    col(
        "tail_number",
        "STRING",
        "Prefixo de registro da aeronave",
        "Aircraft tail number",
        "Matrícula de la aeronave",
        orig="Tail_Number",
    ),
    col(
        "flight_number",
        "STRING",
        "Número do voo atribuído pela empresa aérea",
        "Flight number assigned by the reporting carrier",
        "Número de vuelo asignado por la aerolínea",
        obs="An identifier, not a quantity.",
        orig="Flight_Number_Reporting_Airline",
    ),
    *_endpoint_block("origin"),
    *_endpoint_block("destination"),
    # --- departure ---------------------------------------------------------
    col(
        "scheduled_departure_time",
        "STRING",
        "Horário de partida programado (hora local, HHMM)",
        "Scheduled departure time (local time, HHMM)",
        "Hora de salida programada (hora local, HHMM)",
        obs=HHMM_OBS,
        orig="CRSDepTime",
    ),
    col(
        "scheduled_departure_time_local",
        "TIME",
        "Horário de partida programado como hora do dia, derivado de scheduled_departure_time",
        "Scheduled departure time as a time of day, derived from scheduled_departure_time",
        "Hora de salida programada como hora del día, derivada de scheduled_departure_time",
        obs="Derived. HHMM 2400 is normalised to 00:00:00.",
        orig="CRSDepTime",
    ),
    col(
        "scheduled_departure_datetime_local",
        "DATETIME",
        "Data e hora de partida programada na hora local do aeroporto de origem",
        "Scheduled departure date and time in origin-airport local time",
        "Fecha y hora de salida programada en hora local del aeropuerto de origen",
        obs="Derived by combining flight_date with scheduled_departure_time. This is "
        "the only well-defined datetime in the table: arrival clocks are in "
        "destination local time and may fall on the following day, and the source "
        "carries no timezone, so no arrival datetime is derived.",
        orig="FlightDate + CRSDepTime",
    ),
    col(
        "departure_time",
        "STRING",
        "Horário de partida efetivo (hora local, HHMM)",
        "Actual departure time (local time, HHMM)",
        "Hora de salida real (hora local, HHMM)",
        obs=HHMM_OBS,
        orig="DepTime",
    ),
    col(
        "departure_time_local",
        "TIME",
        "Horário de partida efetivo como hora do dia, derivado de departure_time",
        "Actual departure time as a time of day, derived from departure_time",
        "Hora de salida real como hora del día, derivada de departure_time",
        obs="Derived. HHMM 2400 is normalised to 00:00:00.",
        orig="DepTime",
    ),
    col(
        "departure_delay",
        "FLOAT64",
        "Diferença entre a partida efetiva e a programada; partidas adiantadas são negativas",
        "Difference between actual and scheduled departure; early departures are negative",
        "Diferencia entre la salida real y la programada; las salidas adelantadas son negativas",
        unit=MIN,
        orig="DepDelay",
    ),
    col(
        "departure_delay_minutes",
        "FLOAT64",
        "Atraso na partida com partidas adiantadas fixadas em zero",
        "Departure delay with early departures set to zero",
        "Retraso en la salida con salidas adelantadas fijadas en cero",
        unit=MIN,
        orig="DepDelayMinutes",
    ),
    col(
        "departure_delay_15min",
        "STRING",
        "Indica se a partida atrasou 15 minutos ou mais, com 1 para sim",
        "Whether the departure was delayed 15 minutes or more, with 1 for yes",
        "Indica si la salida se retrasó 15 minutos o más, con 1 para sí",
        dic="yes",
        obs="A boolean published as 0.00/1.00, so STRING.",
        orig="DepDel15",
    ),
    col(
        "departure_delay_group",
        "STRING",
        "Faixa de atraso na partida, em intervalos de 15 minutos",
        "Departure delay interval, in 15-minute bands",
        "Rango de retraso en la salida, en intervalos de 15 minutos",
        dic="yes",
        orig="DepartureDelayGroups",
    ),
    col(
        "scheduled_departure_time_block",
        "STRING",
        "Faixa horária da partida programada, em intervalos de uma hora",
        "Scheduled departure time block, in hourly intervals",
        "Franja horaria de la salida programada, en intervalos de una hora",
        dic="yes",
        orig="DepTimeBlk",
    ),
    col(
        "taxi_out",
        "FLOAT64",
        "Tempo de taxiamento na saída",
        "Taxi out time",
        "Tiempo de rodaje a la salida",
        unit=MIN,
        orig="TaxiOut",
    ),
    col(
        "wheels_off_time",
        "STRING",
        "Horário de decolagem (hora local, HHMM)",
        "Wheels-off time (local time, HHMM)",
        "Hora de despegue (hora local, HHMM)",
        obs=HHMM_OBS,
        orig="WheelsOff",
    ),
    # --- arrival -----------------------------------------------------------
    col(
        "wheels_on_time",
        "STRING",
        "Horário de aterrissagem (hora local, HHMM)",
        "Wheels-on time (local time, HHMM)",
        "Hora de aterrizaje (hora local, HHMM)",
        obs=HHMM_OBS,
        orig="WheelsOn",
    ),
    col(
        "taxi_in",
        "FLOAT64",
        "Tempo de taxiamento na chegada",
        "Taxi in time",
        "Tiempo de rodaje a la llegada",
        unit=MIN,
        orig="TaxiIn",
    ),
    col(
        "scheduled_arrival_time",
        "STRING",
        "Horário de chegada programado (hora local, HHMM)",
        "Scheduled arrival time (local time, HHMM)",
        "Hora de llegada programada (hora local, HHMM)",
        obs=HHMM_OBS,
        orig="CRSArrTime",
    ),
    col(
        "scheduled_arrival_time_local",
        "TIME",
        "Horário de chegada programado como hora do dia, derivado de scheduled_arrival_time",
        "Scheduled arrival time as a time of day, derived from scheduled_arrival_time",
        "Hora de llegada programada como hora del día, derivada de scheduled_arrival_time",
        obs="Derived, in destination local time. HHMM 2400 is normalised to 00:00:00.",
        orig="CRSArrTime",
    ),
    col(
        "arrival_time",
        "STRING",
        "Horário de chegada efetivo (hora local, HHMM)",
        "Actual arrival time (local time, HHMM)",
        "Hora de llegada real (hora local, HHMM)",
        obs=HHMM_OBS,
        orig="ArrTime",
    ),
    col(
        "arrival_time_local",
        "TIME",
        "Horário de chegada efetivo como hora do dia, derivado de arrival_time",
        "Actual arrival time as a time of day, derived from arrival_time",
        "Hora de llegada real como hora del día, derivada de arrival_time",
        obs="Derived, in destination local time. HHMM 2400 is normalised to 00:00:00.",
        orig="ArrTime",
    ),
    col(
        "arrival_delay",
        "FLOAT64",
        "Diferença entre a chegada efetiva e a programada; chegadas adiantadas são negativas",
        "Difference between actual and scheduled arrival; early arrivals are negative",
        "Diferencia entre la llegada real y la programada; las llegadas adelantadas son negativas",
        unit=MIN,
        orig="ArrDelay",
    ),
    col(
        "arrival_delay_minutes",
        "FLOAT64",
        "Atraso na chegada com chegadas adiantadas fixadas em zero",
        "Arrival delay with early arrivals set to zero",
        "Retraso en la llegada con llegadas adelantadas fijadas en cero",
        unit=MIN,
        orig="ArrDelayMinutes",
    ),
    col(
        "arrival_delay_15min",
        "STRING",
        "Indica se a chegada atrasou 15 minutos ou mais, com 1 para sim",
        "Whether the arrival was delayed 15 minutes or more, with 1 for yes",
        "Indica si la llegada se retrasó 15 minutos o más, con 1 para sí",
        dic="yes",
        obs="A boolean published as 0.00/1.00, so STRING.",
        orig="ArrDel15",
    ),
    col(
        "arrival_delay_group",
        "STRING",
        "Faixa de atraso na chegada, em intervalos de 15 minutos",
        "Arrival delay interval, in 15-minute bands",
        "Rango de retraso en la llegada, en intervalos de 15 minutos",
        dic="yes",
        orig="ArrivalDelayGroups",
    ),
    col(
        "scheduled_arrival_time_block",
        "STRING",
        "Faixa horária da chegada programada, em intervalos de uma hora",
        "Scheduled arrival time block, in hourly intervals",
        "Franja horaria de la llegada programada, en intervalos de una hora",
        dic="yes",
        orig="ArrTimeBlk",
    ),
    # --- outcome -----------------------------------------------------------
    col(
        "cancelled",
        "STRING",
        "Indica se o voo foi cancelado, com 1 para sim",
        "Whether the flight was cancelled, with 1 for yes",
        "Indica si el vuelo fue cancelado, con 1 para sí",
        dic="yes",
        obs="A boolean published as 0.00/1.00, so STRING.",
        orig="Cancelled",
    ),
    col(
        "cancellation_code",
        "STRING",
        "Motivo do cancelamento do voo",
        "Reason for the flight cancellation",
        "Motivo de la cancelación del vuelo",
        dic="yes",
        obs="Populated only for cancelled flights.",
        orig="CancellationCode",
    ),
    col(
        "diverted",
        "STRING",
        "Indica se o voo foi desviado, com 1 para sim",
        "Whether the flight was diverted, with 1 for yes",
        "Indica si el vuelo fue desviado, con 1 para sí",
        dic="yes",
        obs="A boolean published as 0.00/1.00, so STRING.",
        orig="Diverted",
    ),
    col(
        "scheduled_elapsed_time",
        "FLOAT64",
        "Duração programada do voo",
        "Scheduled elapsed time of the flight",
        "Duración programada del vuelo",
        unit=MIN,
        orig="CRSElapsedTime",
    ),
    col(
        "actual_elapsed_time",
        "FLOAT64",
        "Duração efetiva do voo",
        "Actual elapsed time of the flight",
        "Duración real del vuelo",
        unit=MIN,
        obs="Null for every diverted flight; see diverted_actual_elapsed_time.",
        orig="ActualElapsedTime",
    ),
    col(
        "air_time",
        "FLOAT64",
        "Tempo de voo entre a decolagem e a aterrissagem",
        "Flight time between wheels off and wheels on",
        "Tiempo de vuelo entre el despegue y el aterrizaje",
        unit=MIN,
        orig="AirTime",
    ),
    col(
        "flights",
        "INT64",
        "Número de voos representados pelo registro",
        "Number of flights represented by the record",
        "Número de vuelos representados por el registro",
        unit="flight",
        obs="Always 1; the record grain is one flight.",
        orig="Flights",
    ),
    col(
        "distance",
        "FLOAT64",
        "Distância entre os aeroportos de origem e destino",
        "Distance between the origin and destination airports",
        "Distancia entre los aeropuertos de origen y destino",
        unit="mile",
        orig="Distance",
    ),
    col(
        "distance_group",
        "STRING",
        "Faixa de distância do trecho, em intervalos de 250 milhas",
        "Distance band of the segment, in 250-mile intervals",
        "Rango de distancia del tramo, en intervalos de 250 millas",
        dic="yes",
        orig="DistanceGroup",
    ),
    # --- delay attribution -------------------------------------------------
    col(
        "carrier_delay",
        "FLOAT64",
        "Minutos de atraso atribuídos à empresa aérea",
        "Minutes of delay attributed to the carrier",
        "Minutos de retraso atribuidos a la aerolínea",
        unit=MIN,
        orig="CarrierDelay",
    ),
    col(
        "weather_delay",
        "FLOAT64",
        "Minutos de atraso atribuídos ao clima",
        "Minutes of delay attributed to weather",
        "Minutos de retraso atribuidos al clima",
        unit=MIN,
        orig="WeatherDelay",
    ),
    col(
        "national_air_system_delay",
        "FLOAT64",
        "Minutos de atraso atribuídos ao sistema nacional de espaço aéreo",
        "Minutes of delay attributed to the National Air System",
        "Minutos de retraso atribuidos al sistema nacional de espacio aéreo",
        unit=MIN,
        orig="NASDelay",
    ),
    col(
        "security_delay",
        "FLOAT64",
        "Minutos de atraso atribuídos à segurança",
        "Minutes of delay attributed to security",
        "Minutos de retraso atribuidos a la seguridad",
        unit=MIN,
        orig="SecurityDelay",
    ),
    col(
        "late_aircraft_delay",
        "FLOAT64",
        "Minutos de atraso atribuídos ao atraso da aeronave no trecho anterior",
        "Minutes of delay attributed to a late inbound aircraft",
        "Minutos de retraso atribuidos al retraso de la aeronave en el tramo anterior",
        unit=MIN,
        orig="LateAircraftDelay",
    ),
    # --- gate return / diversion ------------------------------------------
    col(
        "first_departure_time",
        "STRING",
        "Primeiro horário de saída do portão no aeroporto de origem (hora local, HHMM)",
        "First gate departure time at the origin airport (local time, HHMM)",
        "Primera hora de salida de puerta en el aeropuerto de origen (hora local, HHMM)",
        obs=HHMM_OBS,
        orig="FirstDepTime",
    ),
    col(
        "total_additional_gate_time",
        "FLOAT64",
        "Tempo total fora do portão em retorno ao portão ou voo cancelado",
        "Total ground time away from gate for a gate return or cancelled flight",
        "Tiempo total fuera de la puerta en retorno a puerta o vuelo cancelado",
        unit=MIN,
        orig="TotalAddGTime",
    ),
    col(
        "longest_additional_gate_time",
        "FLOAT64",
        "Maior tempo contínuo fora do portão em retorno ao portão ou voo cancelado",
        "Longest time away from gate for a gate return or cancelled flight",
        "Mayor tiempo continuo fuera de la puerta en retorno a puerta o vuelo cancelado",
        unit=MIN,
        orig="LongestAddGTime",
    ),
    col(
        "diverted_airport_landings",
        "INT64",
        "Número de pousos em aeroportos de desvio",
        "Number of diverted airport landings",
        "Número de aterrizajes en aeropuertos de desvío",
        unit="landing",
        orig="DivAirportLandings",
    ),
    col(
        "diverted_reached_destination",
        "STRING",
        "Indica se o voo desviado alcançou o destino programado, com 1 para sim",
        "Whether the diverted flight reached its scheduled destination, with 1 for yes",
        "Indica si el vuelo desviado alcanzó su destino programado, con 1 para sí",
        dic="yes",
        orig="DivReachedDest",
    ),
    col(
        "diverted_actual_elapsed_time",
        "FLOAT64",
        "Duração do voo desviado que alcançou o destino programado",
        "Elapsed time of a diverted flight that reached its scheduled destination",
        "Duración del vuelo desviado que alcanzó su destino programado",
        unit=MIN,
        obs="actual_elapsed_time is null for diverted flights; this column carries "
        "the value instead.",
        orig="DivActualElapsedTime",
    ),
    col(
        "diverted_arrival_delay",
        "FLOAT64",
        "Atraso na chegada do voo desviado que alcançou o destino programado",
        "Arrival delay of a diverted flight that reached its scheduled destination",
        "Retraso en la llegada del vuelo desviado que alcanzó su destino programado",
        unit=MIN,
        obs="arrival_delay is null for diverted flights; this column carries the "
        "value instead.",
        orig="DivArrDelay",
    ),
    col(
        "diverted_distance",
        "FLOAT64",
        "Distância entre o destino programado e o aeroporto final de desvio",
        "Distance between the scheduled destination and the final diverted airport",
        "Distancia entre el destino programado y el aeropuerto final de desvío",
        unit="mile",
        obs="Zero when the diverted flight reached its destination.",
        orig="DivDistance",
    ),
    *[c for n in (1, 2, 3, 4, 5) for c in _diversion_block(n)],
]


# ---------------------------------------------------------------------------
# airport
# ---------------------------------------------------------------------------
AIRPORT: list[dict] = [
    col(
        "airport_id",
        "STRING",
        "Identificador numérico do aeroporto atribuído pelo US DOT",
        "Numeric US DOT identifier of the airport",
        "Identificador numérico del aeropuerto asignado por el US DOT",
        obs="Joins to origin_airport_id and destination_airport_id in `flight`.",
        orig="Code",
    ),
    col(
        "city_name",
        "STRING",
        "Cidade servida pelo aeroporto",
        "City served by the airport",
        "Ciudad servida por el aeropuerto",
        obs="Parsed from the source description.",
        orig="Description",
    ),
    col(
        "state_abbreviation",
        "STRING",
        "Sigla do estado ou território dos Estados Unidos",
        "Abbreviation of the US state or territory",
        "Sigla del estado o territorio de los Estados Unidos",
        dic="yes",
        obs="Filled only for airports in the United States and its territories; "
        "null for foreign airports, which carry country_name instead.",
        orig="Description",
    ),
    col(
        "country_name",
        "STRING",
        "País do aeroporto quando situado fora dos Estados Unidos",
        "Country of the airport when located outside the United States",
        "País del aeropuerto cuando se ubica fuera de los Estados Unidos",
        obs="Null for US airports, which carry state_abbreviation instead.",
        orig="Description",
    ),
    col(
        "airport_name",
        "STRING",
        "Nome do aeroporto",
        "Name of the airport",
        "Nombre del aeropuerto",
        obs="Parsed from the source description.",
        orig="Description",
    ),
    col(
        "airport_description",
        "STRING",
        "Descrição do aeroporto exatamente como publicada pelo BTS",
        "Airport description exactly as published by BTS",
        "Descripción del aeropuerto exactamente como la publica el BTS",
        obs="Kept verbatim so the parsed columns can always be audited against "
        "the source.",
        orig="Description",
    ),
]


# ---------------------------------------------------------------------------
# dicionario
# ---------------------------------------------------------------------------
DICIONARIO: list[dict] = [
    col(
        "id_tabela",
        "STRING",
        "Nome da tabela que contém a coluna traduzida",
        "Name of the table holding the translated column",
        "Nombre de la tabla que contiene la columna traducida",
    ),
    col(
        "nome_coluna",
        "STRING",
        "Nome da coluna traduzida",
        "Name of the translated column",
        "Nombre de la columna traducida",
    ),
    col(
        "chave",
        "STRING",
        "Valor codificado presente na coluna",
        "Coded value present in the column",
        "Valor codificado presente en la columna",
    ),
    col(
        "cobertura_temporal",
        "STRING",
        "Cobertura temporal do par chave-valor",
        "Temporal coverage of the key-value pair",
        "Cobertura temporal del par clave-valor",
    ),
    col(
        "valor",
        "STRING",
        "Significado do valor codificado",
        "Meaning of the coded value",
        "Significado del valor codificado",
    ),
]

TABLES = {"flight": FLIGHT, "airport": AIRPORT, "dicionario": DICIONARIO}


def main() -> None:
    for name, cols in TABLES.items():
        seen = set()
        for c in cols:
            if c["name"] in seen:
                raise SystemExit(f"{name}: duplicate column {c['name']}")
            seen.add(c["name"])
            if (
                c["bigquery_type"] in ("INT64", "FLOAT64")
                and not c["measurement_unit"]
            ):
                raise SystemExit(
                    f"{name}.{c['name']}: numeric column needs a measurement_unit"
                )
            if (
                c["covered_by_dictionary"] == "yes"
                and c["bigquery_type"] != "STRING"
            ):
                raise SystemExit(
                    f"{name}.{c['name']}: dictionary-covered must be STRING"
                )
            for k in ("description", "description_en", "description_es"):
                if c[k].endswith("."):
                    raise SystemExit(
                        f"{name}.{c['name']}: {k} must not end with a period"
                    )
                if c[k] and not c[k][0].isupper():
                    raise SystemExit(
                        f"{name}.{c['name']}: {k} must start capitalised"
                    )
        out = HERE / f"{name}.csv"
        with open(out, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=HEADER)
            w.writeheader()
            w.writerows(cols)
        print(f"{out.name}: {len(cols)} columns")


if __name__ == "__main__":
    main()
