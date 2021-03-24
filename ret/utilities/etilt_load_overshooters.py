#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

from datetime import datetime

from ret.loguru import (
        logger,
    )

from ret.database.tables import (
        Overshooter,
        get_engine,
        get_session,
    )

from ret.utilities.overshooting import overshooting

def load_overshooters():
    logger.info(f'load_overshooters:')
    neighborhood_df, overshooters_df = overshooting()
    overshooters_dict = overshooters_df.to_dict('index')
    engine = get_engine()
    session = get_session(engine=engine)

    now_ = datetime.now()
    for index, dict_ in overshooters_dict.items():
        obj_ = Overshooter(
            date_time = now_,
            cell_name = dict_['CELLNAME'],
            time_advanced = int(dict_['ta_']),
            average_distance = int(dict_['distance_']),
            is_overshooter = dict_['overshooter']
            )
        session.add(obj_)
    session.commit()
    session.close()


if __name__ == '__main__':
    load_overshooters()

