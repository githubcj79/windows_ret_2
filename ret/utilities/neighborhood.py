#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import pandas as pd
# import numpy as np
from ret.loguru import logger

from ret.config.settings import (
        ENV,
        KM,
        D,
        N_DISTANCE,
        TERRAIN_DELTA,
    )

from ret.utilities.input_data import (
        get_cells_df,
    )

from ret.utilities.numpy_functions import (
        haversine_distance,
        bearing,
    )

def neighborhood(time_=None):
    logger.debug(f'ENV {ENV}')

    if not time_:
        logger.info(f'time_ {time_}')
        return

    cells_df = get_cells_df(time_=time_)

    l = ['SITE', 'LAT', 'LON']
    sites_df = cells_df[l].drop_duplicates()

    sites_df['key'] = 1
    merged_df = pd.merge(sites_df, sites_df, on ='key').drop("key", 1)

    merged_df = merged_df[merged_df['SITE_x'] != merged_df['SITE_y']]

    merged_df['distance_'] = haversine_distance(
                                merged_df['LAT_x'].values ,
                                merged_df['LON_x'].values,
                                merged_df['LAT_y'].values ,
                                merged_df['LON_y'].values
                                )

    merged_df['bearing_'] = bearing(
                                merged_df['LAT_x'].values ,
                                merged_df['LON_x'].values,
                                merged_df['LAT_y'].values ,
                                merged_df['LON_y'].values
                                )

    # logger.info(f'neighborhood: merged_df.columns {merged_df.columns}')
    l = ['SITE_x', 'SITE_y', 'distance_','bearing_']
    merged_df = merged_df[l]

    merged_df = merged_df[merged_df['distance_'] <= KM]

    l = ['SITE', 'CELLNAME', 'AZIMUTH']
    merged_df = pd.merge(cells_df[l], merged_df, how="inner", left_on='SITE', right_on='SITE_x')

    merged_df = merged_df[(merged_df['bearing_'] > merged_df['AZIMUTH'] - D) & (merged_df['bearing_'] < merged_df['AZIMUTH'] + D)]

    l = ['CELLNAME', 'distance_']
    merged_df.sort_values(by=l, inplace=True)

    l = ['CELLNAME']
    neighborhood_df = merged_df.groupby(l).head(N_DISTANCE)

    l = ['CELLNAME', 'AZIMUTH', 'SITE_x', 'SITE_y', 'distance_',
       'bearing_']

    logger.info(f'neighborhood_df[l].shape {neighborhood_df[l].shape}')
    return neighborhood_df[l], cells_df

def main():
        # time_ = datetime.datetime(2021, 2, 25, 10, 30, 0, 0)
        now_ = datetime.datetime.now()
        day_before = now_  - datetime.timedelta(days=1)
        neighborhood_df, cells_df = neighborhood(time_=day_before)


if __name__ == '__main__':
    main()
