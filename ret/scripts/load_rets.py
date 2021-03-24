#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import pandas as pd

from ret.loguru import logger

from ret.database.rets_data import rets_data

from ret.config.settings import (
        ENV,
    )

from ret.database.tables import (
        get_engine,
        get_session,
    )

def load_rets(time_=None):
    logger.info(f'ENV {ENV}')

    if not time_:
        return

    list_ = [
                {
                    'datetimeid' : time_,
                    'node' : 'MBTS-AIS_3G_003',
                    'cellname' : 'AIS_4G_003_3',
                    'eci' : 2816002,
                    'devicename' : 'RET82',
                    'deviceno' : 2,
                    'tilt' : 30,
                    'subname' : 'RET82',
                    'subunitno' : 1,
                    'localcellid' : 2,
                },
                {
                    'datetimeid' : time_,
                    'node' : 'MBTS-ARA_3G_013',
                    'cellname' : 'ARA_4G_013_3',
                    'eci' : 2304258,
                    'devicename' : 'RET82R_S3',
                    'deviceno' : 2,
                    'tilt' : 40,
                    'subname' : 'RET82R_S3',
                    'subunitno' : 1,
                    'localcellid' : 2,
                },
                {
                    'datetimeid' : time_,
                    'node' : 'MBTS-ARA_3G_013',
                    'cellname' : 'ARA_4G_013_3',
                    'eci' : 2304258,
                    'devicename' : 'RET82L_S3',
                    'deviceno' : 12,
                    'tilt' : 40,
                    'subname' : 'RET82L_S3',
                    'subunitno' : 1,
                    'localcellid' : 2,
                },
            ]

    if ENV == 'sim':
        df = pd.DataFrame.from_dict(list_)

    if ENV == 'prod':
        df = rets_data(time_=time_)

    engine = get_engine()
    session = get_session(engine=engine)
    df.to_sql('rets', con=engine, if_exists='append', index=False)
    session.commit()
    session.close()


if __name__ == '__main__':
    # load_rets(time_=datetime.datetime(2021, 2, 25, 10, 30, 0, 0))
    # load_rets(time_=datetime.datetime(2021, 2, 26, 10, 30, 0, 0))
    now_ = datetime.datetime.now()
    # day_before = now_  - datetime.timedelta(days=1)
    when_ = now_
    # period = when_.strftime("%Y-%m-%d")

    load_rets(time_=when_)
