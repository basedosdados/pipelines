# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""


from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_ms_cnes project
    """

    # to build paths
    PATH = [
        "/tmp/br_ms_cnes/input/",
        "/tmp/br_ms_cnes/output/",
    ]

    # to build paths
    TABLE = ["estabelecimento", "profissionais"]

    # to download files from datasus FTP server
    DATABASE_GROUPS = {
        "CNES": ["ST", "PF"],
    }

    # generate YYYYMM to parse correct files from FTP server
    # usually the files are released with a 2 month delay. So this dict
    # maps the representative values of months to it as an int - 2
    GENERATE_MONTH_TO_PARSE = {
        # january : november
        1: "11",
        # february : december an so on
        2: "12",
        3: "01",
        4: "02",
        5: "03",
        6: "04",
        7: "05",
        8: "06",
        9: "07",
        10: "08",
        12: "09",
        11: "10",
    }
