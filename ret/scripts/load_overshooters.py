#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import numpy as np
import pandas as pd
from ret.loguru import logger

from ret.utilities.overshooting import overshooters

from ret.config.settings import (
        ENV,
    )

from ret.database.tables import (
        get_engine,
        get_session,
    )

def load_overshooters(time_=None,
        neighborhood_df=pd.DataFrame(),
        cells_df=pd.DataFrame()):
    logger.info(f'load_overshooters:')

    if not time_:
        return

    list_ = [
                {
                    'datetimeid' : time_,
                    'cellname' : 'AIS_4G_003_3',
                    'ta_calculated' : 14.4,
                    'average_distance' : 2.43982546537283,
                    'overshooter' : True,
                    'intensity' : 'High',
                },
                {
                    'datetimeid' : time_,
                    'cellname' : 'ARA_4G_013_3',
                    'ta_calculated' : 14.4,
                    'average_distance' : 6.14587256200947,
                    'overshooter' : True,
                    'intensity' : 'High',
                },
            ]

    if ENV == 'sim':
        df = pd.DataFrame.from_dict(list_)

    if ENV == 'prod':
        df = overshooters(
                time_=time_,
                neighborhood_df=neighborhood_df,
                cells_df=cells_df)

    engine = get_engine()
    session = get_session(engine=engine)
    df.to_sql('overshooters', con=engine, if_exists='append', index=False)
    session.commit()
    session.close()


if __name__ == '__main__':
    # load_overshooters(time_=datetime.datetime(2021, 1, 10, 10, 30, 0, 0))
    now_ = datetime.datetime.now()
    # day_before = now_  - datetime.timedelta(days=1)
    when_ = now_
    load_overshooters(time_=when_)
