"""Constants for the BTS Reporting Carrier On-Time Performance pipeline."""

from enum import Enum
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Dataset-level constants."""

    DATASET_ID = "us_dot_bts_ontime"

    # --- source -----------------------------------------------------------
    # Prezipped monthly archives. Covers 1987-10..1989-12 and 2000-01..present;
    # the 1990s are absent here and must come from the on-demand form below.
    PREZIP_URL = (
        "https://transtats.bts.gov/PREZIP/"
        "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"
    )
    # ASP.NET WebForms page that generates any year-month on demand.
    FORM_URL = (
        "https://www.transtats.bts.gov/DL_SelectFields.aspx"
        "?gnoyr_VQ=FGJ&QO_fu146_anzr=b0-gvzr"
    )
    LOOKUP_URL = (
        "https://www.transtats.bts.gov/Download_Lookup.asp?Y11x72={key}"
    )

    # TranStats serves .zip only to a browser-like client.
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    )

    # First month of the series. BTS on-time reporting begins 1987-10.
    FIRST_YEAR = 1987
    FIRST_MONTH = 10
    # Months available as prezipped archives; everything else uses the form.
    PREZIP_GAP_YEARS = tuple(range(1990, 2000))

    # --- lookup tables (the querystring key is the ROT13 of the table name) --
    LOOKUPS = {
        "L_AIRPORT": "Y_NVecbeg",
        "L_AIRPORT_ID": "Y_NVecbeg_VQ",
        "L_AIRPORT_SEQ_ID": "Y_NVecbeg_fRd_VQ",
        "L_CITY_MARKET_ID": "Y_PVgl_ZNeXRg_VQ",
        "L_AIRLINE_ID": "Y_NVeYVaR_VQ",
        "L_UNIQUE_CARRIERS": "Y_haVdhR_PNeeVRef",
        "L_CARRIER_HISTORY": "Y_PNeeVRe_UVfgbel",
        "L_CANCELLATION": "Y_PNaPRYYNgVba",
        "L_DIVERSIONS": "Y_QViRefVbaf",
        "L_DEPARRBLK": "Y_QRcNeeOYX",
        "L_DISTANCE_GROUP_250": "Y_QVfgNaPR_Tebhc_FID",
        "L_ONTIME_DELAY_GROUPS": "Y_bagVZR_QRYNl_Tebhcf",
        "L_MONTHS": "Y_ZbagUf",
        "L_QUARTERS": "Y_dhNegRef",
        "L_WEEKDAYS": "Y_jRRXQNlf",
        "L_STATE_ABR_AVIATION": "Y_fgNgR_NOe_NiVNgVba",
        "L_STATE_FIPS": "Y_fgNgR_SVcf",
        "L_WORLD_AREA_CODES": "Y_jbeYQ_NeRN_PbQRf",
        "L_YESNO_RESP": "Y_lRfab_eRfc",
    }

    # --- local paths ------------------------------------------------------
    ARCHITECTURE_DIR = (
        _REPO_ROOT / "models" / "us_dot_bts_ontime" / "code" / "architecture"
    )
    TABLES = ("flight", "airport", "dicionario")
