# -*- coding: utf-8 -*-
from SICAR import Sicar

car = Sicar()

d = car.get_release_dates()

print(d['PA'])
