# -*- coding: utf-8 -*-
"""
General purpose functions for the br_anatel_telefonia_movel project
"""
# pylint: disable=too-few-public-methods,invalid-name

import os
import re


def find_csv_files(directory):
    """
    Find all CSV files in a directory with names matching the pattern "Acessos_Telefonia_Movel_YYYYMM-YYYYMM.csv",
    where "YYYY" represents a four-digit year and "MM" represents a two-digit month.

    Parameters:
        directory (str): Path to the directory to search for CSV files.

    Returns:
        List of strings: Full paths to all CSV files that match the pattern.
    """
    pattern = r"Acessos_Telefonia_Movel_\d{6}-\d{6}\.csv"
    csv_files = []
    for filename in os.listdir(directory):
        if re.match(pattern, filename):
            file_path = os.path.join(directory, filename)
            csv_files.append(file_path)
    return csv_files
