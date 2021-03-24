#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import pandas as pd

from ret.loguru import logger

from ret.utilities.check_terrain import check_terrain

from ret.config.settings import (
        ENV,
    )

from ret.database.tables import (
        Terrain,
        get_engine,
        get_session,
    )

def load_terrains(time_=None,
        neighborhood_df=pd.DataFrame(),
        cells_df=pd.DataFrame()):
    logger.info(f'ENV {ENV}')

    if not time_:
        return

    list_ = [
                {
                    'datetimeid' : time_,
                    'cellname' : 'AIS_4G_003_3',
                    'is_plain' : True,
                    'slope' : 0,
                },
                {
                    'datetimeid' : time_,
                    'cellname' : 'ARA_4G_013_3',
                    'is_plain' : True,
                    'slope' : 0,
                },
            ]

    if ENV == 'sim':
        terrain_df = pd.DataFrame.from_dict(list_)

    if ENV == 'prod':
        terrain_df, neighborhood_df, cells_df = check_terrain(
                                        time_=time_,
                                        neighborhood_df=neighborhood_df,
                                        cells_df=cells_df)

    logger.info(f'terrain_df.shape {terrain_df.shape}')
    logger.info(f'terrain_df.columns {terrain_df.columns}')

    terrains_dict = terrain_df.to_dict('index')
    engine = get_engine()
    session = get_session(engine=engine)

    now_ = datetime.datetime.now()
    for index, dict_ in terrains_dict.items():
        is_plain_ = not dict_['HILL']
        if is_plain_:
            slope_ = 0
        else:
            slope_ = 1 if int(dict_['HEIGHT_DIFF']) > 0 else -1
        obj_ = Terrain(
            datetimeid = now_,
            cellname = dict_['CELLNAME'],
            slope = slope_,
            is_plain = is_plain_
            )
        session.add(obj_)
    session.commit()
    session.close()

    return neighborhood_df, cells_df


if __name__ == '__main__':
    # load_terrains(time_=datetime.datetime(2021, 1, 10, 10, 30, 0, 0))
    now_ = datetime.datetime.now()
    day_before = now_  - datetime.timedelta(days=1)
    when_ = day_before
    when_ = now_

    df = load_terrains(time_=when_)
