#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

from ret.loguru import logger

def haversine_distance(lat1, lon1, lat2, lon2):
    logger.info(f'haversine_distance:')

    km_constant = 6372.795477598
    lat1, lon1, lat2, lon2 = list(map(np.deg2rad,\
    [lat1, lon1, lat2, lon2]))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2)**2 + np.cos(lat1) *\
    np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    mi = km_constant * c
    return mi

def bearing(lat1, lon1, lat2, lon2):
    logger.info(f'bearing:')

    lat1, lon1, lat2, lon2 = list(map(np.deg2rad,\
    [lat1, lon1, lat2, lon2]))

    dlon = lon2 - lon1

    x = np.sin(dlon) * np.cos(lat2)
    y = np.cos(lat1) * np.sin(lat2) - (np.sin(lat1)
        * np.cos(lat2) * np.cos(dlon))

    initial_bearing = np.arctan2(x, y)

    # Now we have the initial bearing but math.atan2 return values
    # from -180° to + 180° which is not what we want for a compass bearing
    # The solution is to normalize the initial bearing as shown below
    initial_bearing = np.degrees(initial_bearing)
    bearing = (initial_bearing + 360) % 360
    return bearing
