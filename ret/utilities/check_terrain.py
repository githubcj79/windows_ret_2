#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
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

from ret.utilities.neighborhood import (
        neighborhood,
    )

def check_terrain(time_=None, neighborhood_df=pd.DataFrame(),
    cells_df=pd.DataFrame()):
    logger.info(f'check_terrain:')

    if not time_:
        return

    if neighborhood_df.empty:
        neighborhood_df, cells_df = neighborhood(time_)

    if cells_df.empty:
        cells_df = get_cells_df(time_=time_)

    # Adding GEO_HEIGHT_x to neighborhood
    l = ['SITE', 'GEO_HEIGHT']
    aux = pd.merge(neighborhood_df, cells_df[l], how="inner", left_on='SITE_x', right_on='SITE').drop_duplicates()

    l = ['SITE_x', 'GEO_HEIGHT']
    aux = aux[l][aux['SITE_x'] == aux['SITE']].drop_duplicates()

    l = ['SITE_x', 'GEO_HEIGHT_x']
    aux.columns = l

    neighborhood_df_ = pd.merge(neighborhood_df, aux, how="inner", left_on='SITE_x', right_on='SITE_x').drop_duplicates()

    # Adding GEO_HEIGHT_y to neighborhood
    l = ['SITE', 'GEO_HEIGHT']
    aux = pd.merge(neighborhood_df_, cells_df[l], how="inner", left_on='SITE_y', right_on='SITE').drop_duplicates()

    l = ['SITE_y', 'GEO_HEIGHT']
    aux = aux[l][aux['SITE_y'] == aux['SITE']].drop_duplicates()

    l = ['SITE_y', 'GEO_HEIGHT_y']
    aux.columns = l

    neighborhood_df_ = pd.merge(neighborhood_df_, aux, how="inner", left_on='SITE_y', right_on='SITE_y').drop_duplicates()

    # Detecting cells' terrain

    # neighborhood_df_['HEIGHT_DIFF'] = np.abs((neighborhood_df_['GEO_HEIGHT_x'] - neighborhood_df_['GEO_HEIGHT_y']).values)

    neighborhood_df_['HEIGHT_DIFF'] = (neighborhood_df_['GEO_HEIGHT_x'] - neighborhood_df_['GEO_HEIGHT_y']).values

    l = ['CELLNAME', 'SITE_x', 'SITE_y', 'HEIGHT_DIFF']
    t = ['CELLNAME', 'SITE_x']
    terrain_df = neighborhood_df_[l].groupby(t).mean()
    terrain_df['HILL'] = np.abs(terrain_df['HEIGHT_DIFF']) > TERRAIN_DELTA
    terrain_df.reset_index(inplace=True)
    # neighborhood_df.reset_index(inplace=True)
    return terrain_df, neighborhood_df, cells_df


if __name__ == '__main__':
    neighborhood_df, terrain_df = check_terrain()
    neighborhood_df.to_excel(r'data/neighborhood_df.xlsx', index = False)
    terrain_df.to_excel(r'data/terrain_df.xlsx', index = False)
