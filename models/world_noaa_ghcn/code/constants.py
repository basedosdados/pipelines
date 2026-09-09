"""Constants for the GHCN-Daily (world_noaa_ghcn) onboarding.

Every unit and scale factor below is transcribed from section III of the
GHCN-Daily readme.txt (version 3.34), which is the authority for element
semantics.  See SCOPE_DECISION.md for why only CORE_ELEMENTS are loaded into
the `observation` table.
"""

BASE_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily"
BY_YEAR_URL = BASE_URL + "/by_year/{year}.csv.gz"

# --- Scope (see SCOPE_DECISION.md) -----------------------------------------
CORE_ELEMENTS = ("TMAX", "TMIN", "PRCP", "SNOW", "SNWD")
FIRST_YEAR = 1950

# The raw by_year CSV has no header; these are its 8 fields in order.
RAW_COLUMNS = (
    "station_id",
    "date",
    "element",
    "value",
    "measurement_flag",
    "quality_flag",
    "source_flag",
    "observation_time",
)

MISSING_VALUE = -9999  # readme III, VALUE1
MISSING_ELEVATION = -999.9  # readme IV, ELEVATION

# --- Element units and scale factors ---------------------------------------
# (unit, divisor).  value_in_unit = raw_integer / divisor.
#
# The trap: PRCP is *tenths* of a millimetre but SNOW and SNWD are already
# *whole* millimetres.  A blanket /10 across "precipitation-like" elements
# silently divides snowfall by ten.
_TENTHS_C = ("celsius", 10)
_TENTHS_MM = ("millimeter", 10)
_WHOLE_MM = ("millimeter", 1)
_TENTHS_MS = ("meter_per_second", 10)
_TENTHS_HPA = ("hectopascal", 10)
_PERCENT = ("percent", 1)
_DEGREE = ("degree", 1)
_CM = ("centimeter", 1)
_KM = ("kilometer", 1)
_MINUTE = ("minute", 1)
_DAY = ("day", 1)
_HHMM = (None, 1)  # clock time, not a quantity
_OCCURRENCE = (None, 1)  # weather-type occurrence flag, not a quantity

ELEMENT_UNITS = {
    # five core elements
    "TMAX": _TENTHS_C,
    "TMIN": _TENTHS_C,
    "PRCP": _TENTHS_MM,
    "SNOW": _WHOLE_MM,
    "SNWD": _WHOLE_MM,
    # other temperatures (tenths of degrees C)
    "TAVG": _TENTHS_C,
    "TAXN": _TENTHS_C,
    "TOBS": _TENTHS_C,
    "ADPT": _TENTHS_C,
    "AWBT": _TENTHS_C,
    "MDTN": _TENTHS_C,
    "MDTX": _TENTHS_C,
    "MNPN": _TENTHS_C,
    "MXPN": _TENTHS_C,
    # depths / water equivalents (tenths of mm)
    "EVAP": _TENTHS_MM,
    "MDEV": _TENTHS_MM,
    "MDPR": _TENTHS_MM,
    "THIC": _TENTHS_MM,
    "WESD": _TENTHS_MM,
    "WESF": _TENTHS_MM,
    "MDSF": _WHOLE_MM,
    # wind speed (tenths of m/s)
    "AWND": _TENTHS_MS,
    "WSF1": _TENTHS_MS,
    "WSF2": _TENTHS_MS,
    "WSF5": _TENTHS_MS,
    "WSFG": _TENTHS_MS,
    "WSFI": _TENTHS_MS,
    "WSFM": _TENTHS_MS,
    # pressure (hPa * 10)
    "ASLP": _TENTHS_HPA,
    "ASTP": _TENTHS_HPA,
    # percent
    "ACMC": _PERCENT,
    "ACMH": _PERCENT,
    "ACSC": _PERCENT,
    "ACSH": _PERCENT,
    "PSUN": _PERCENT,
    "RHAV": _PERCENT,
    "RHMN": _PERCENT,
    "RHMX": _PERCENT,
    # direction (degrees)
    "AWDR": _DEGREE,
    "WDF1": _DEGREE,
    "WDF2": _DEGREE,
    "WDF5": _DEGREE,
    "WDFG": _DEGREE,
    "WDFI": _DEGREE,
    "WDFM": _DEGREE,
    # frozen ground / gauge height (cm)
    "FRGB": _CM,
    "FRGT": _CM,
    "FRTH": _CM,
    "GAHT": _CM,
    # wind movement (km)
    "MDWM": _KM,
    "WDMV": _KM,
    # sunshine (minutes)
    "TSUN": _MINUTE,
    # multiday counters (days)
    "DAEV": _DAY,
    "DAPR": _DAY,
    "DASF": _DAY,
    "DATN": _DAY,
    "DATX": _DAY,
    "DAWM": _DAY,
    "DWPR": _DAY,
    # clock times, not quantities
    "FMTM": _HHMM,
    "PGTM": _HHMM,
}
# Soil temperatures SN*# / SX*# are all tenths of degrees C.
for _cover in "012345678":
    for _depth in "1234567":
        ELEMENT_UNITS[f"SN{_cover}{_depth}"] = _TENTHS_C
        ELEMENT_UNITS[f"SX{_cover}{_depth}"] = _TENTHS_C
# Weather type / weather in vicinity are occurrence flags.
for _n in list(range(1, 23)):
    ELEMENT_UNITS[f"WT{_n:02d}"] = _OCCURRENCE
    ELEMENT_UNITS[f"WV{_n:02d}"] = _OCCURRENCE

ELEMENT_DESCRIPTIONS = {
    "TMAX": "Maximum temperature",
    "TMIN": "Minimum temperature",
    "PRCP": "Precipitation",
    "SNOW": "Snowfall",
    "SNWD": "Snow depth",
}

# --- Flag code tables (readme section III) ---------------------------------
MEASUREMENT_FLAGS = {
    "B": "Precipitation total formed from two 12-hour totals",
    "D": "Precipitation total formed from four six-hour totals",
    "H": "Highest or lowest hourly temperature, or average of hourly values",
    "K": "Converted from knots",
    "L": "Temperature appears to be lagged with respect to reported hour of observation",
    "O": "Converted from oktas",
    "P": "Identified as missing presumed zero in DSI 3200 and 3206",
    "T": "Trace of precipitation, snowfall, or snow depth",
    "W": "Converted from 16-point WBAN code (for wind direction)",
}

# A non-blank quality flag means the value FAILED the named check.
QUALITY_FLAGS = {
    "D": "Failed duplicate check",
    "G": "Failed gap check",
    "I": "Failed internal consistency check",
    "K": "Failed streak/frequent-value check",
    "L": "Failed check on length of multiday period",
    "M": "Failed megaconsistency check",
    "N": "Failed naught check",
    "O": "Failed climatological outlier check",
    "R": "Failed lagged range check",
    "S": "Failed spatial consistency check",
    "T": "Failed temporal consistency check",
    "W": "Temperature too warm for snow",
    "X": "Failed bounds check",
    "Z": "Flagged as a result of an official Datzilla investigation",
}

SOURCE_FLAGS = {
    "0": "U.S. Cooperative Summary of the Day (NCDC DSI-3200)",
    "1": "CF6 daily climate summaries from the U.S. National Weather Service",
    "2": "Synoptic Summary of the Day version 2, successor to GSOD",
    "6": "CDMP Cooperative Summary of the Day (NCDC DSI-3206)",
    "7": "U.S. Cooperative Summary of the Day transmitted via WxCoder3 (NCDC DSI-3207)",
    "A": "U.S. Automated Surface Observing System (ASOS) real-time data since 2006",
    "a": "Australian data from the Australian Bureau of Meteorology",
    "B": "U.S. ASOS data for October 2000 to December 2005 (NCDC DSI-3211)",
    "b": "Belarus update",
    "C": "Environment Canada",
    "D": "Short time delay U.S. National Weather Service CF6 daily summaries",
    "d": "Short time delay U.S. National Weather Service Daily Summary Messages",
    "E": "European Climate Assessment and Dataset",
    "F": "U.S. Fort data",
    "f": "Data provided courtesy of the Fiji Met Service",
    "G": "Official GCOS or other government-supplied data",
    "H": "High Plains Regional Climate Center real-time data",
    "I": "International collection received through personal contacts",
    "K": "U.S. Cooperative Summary of the Day digitized from paper observer forms",
    "M": "Monthly METAR Extract (additional ASOS data)",
    "m": "Mexican National Water Commission (CONAGUA)",
    "N": "Community Collaborative Rain, Hail and Snow (CoCoRaHS)",
    "Q": "Data from several African countries previously withheld from public release",
    "R": "NCEI Reference Network Database (CRN and Regional CRN)",
    "r": "All-Russian Research Institute of Hydrometeorological Information",
    "S": "Global Summary of the Day (NCDC DSI-9618)",
    "s": "China Meteorological Administration National Meteorological Information Center",
    "T": "SNOwpack TELemetry (SNOTEL) data from the U.S. NRCS",
    "U": "Remote Automatic Weather Station (RAWS) data from the Western Regional Climate Center",
    "u": "Ukraine update",
    "W": "WBAN/ASOS Summary of the Day from NCDC Integrated Surface Data",
    "X": "U.S. First-Order Summary of the Day (NCDC DSI-3210)",
    "Z": "Datzilla official additions or replacements",
    "z": "Uzbekistan update",
}

# Third character of the station id (readme section IV).
NETWORK_CODES = {
    "0": "Unspecified station numbering system",
    "1": "Community Collaborative Rain, Hail and Snow (CoCoRaHS) identifier",
    "C": "U.S. Cooperative Network identifier",
    "E": "European Climate Assessment and Dataset non-blended identifier",
    "M": "World Meteorological Organization identifier",
    "N": "Identifier supplied by a National Meteorological or Hydrological Centre",
    "P": "Pre-Coop internal NCEI identifier",
    "R": "U.S. Interagency Remote Automatic Weather Station (RAWS) identifier",
    "S": "U.S. NRCS SNOwpack TELemetry (SNOTEL) identifier",
    "W": "WBAN identifier",
}

GSN_FLAGS = {"GSN": "GCOS Surface Network station"}
HCN_CRN_FLAGS = {
    "HCN": "U.S. Historical Climatology Network station",
    "CRN": "U.S. Climate Reference Network or U.S. Regional Climate Network station",
}
