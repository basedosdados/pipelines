# -*- coding: utf-8 -*-
"""
Utils for br_rf_cno
"""

import os
import unicodedata
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

from pipelines.utils.utils import log
