#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import pandas as pd

from ret.loguru import logger

from ret.scripts.load_rets import (
        load_rets,
    )

from ret.scripts.load_terrains import (
        load_terrains,
    )

from ret.scripts.load_overshooters import (
        load_overshooters,
    )

from ret.utilities.neighborhood import (
        neighborhood,
    )

from ret.config.settings import (
        ENV,
    )

# from load_rets import load_rets
# from load_terrains import load_terrains
# from neighborhood import neighborhood
# from load_overshooters import load_overshooters

# from settings import (
#         ENV,
#     )

def loads():
    logger.info(f'ENV {ENV}')

    # time_=datetime.datetime(2021, 1, 10, 10, 30, 0, 0)
    # day_before = now_  - datetime.timedelta(days=1)

    now_ = datetime.datetime.now()
    neighborhood_df, cells_df = neighborhood(time_=now_)

    now_ = datetime.datetime.now()
    load_rets(time_=now_) # ok

    now_ = datetime.datetime.now()
    neighborhood_df, cells_df = load_terrains(
                        time_=now_,
                        neighborhood_df=neighborhood_df,
                        cells_df=cells_df,
                        )

    now_ = datetime.datetime.now()
    load_overshooters(time_=now_,
                        neighborhood_df=neighborhood_df,
                        cells_df=cells_df,
                        )

def main():
    loads()


if __name__ == '__main__':
    main()
