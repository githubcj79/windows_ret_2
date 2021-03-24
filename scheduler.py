#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
from ret.loguru import logger
import pandas as pd

from ret.utilities.giver_of_times import (
        giver_of_times,
    )

from ret.utilities.mid_term_evaluator import (
        mid_term_evaluator,
    )

from ret.database.tables import (
        get_engine,
    )

def scheduler(time_=None):
    logger.debug(f"time_ {time_}")

    if not time_:
        return

    engine = get_engine()
    db_connection = engine.connect()

    # set más reciente de 'High' overshooters en terreno plano
    query_ = '''
            select o.cellname
            from overshooters o, terrains t
            where o.cellname = t.cellname
                and o.overshooter and t.is_plain
                and o.intensity = 'High'
                and o.datetimeid = (select max(datetimeid) from overshooters)
                and t.datetimeid = (select max(datetimeid) from terrains);
            '''

    candidates_df = pd.read_sql(query_, db_connection)
    db_connection.close()

    # entregar los candidatos a mid_term_evaluator()
    mid_term_evaluator(time_=time_, candidates_df=candidates_df)


def main():
    for time_ in giver_of_times():
        scheduler(time_)


if __name__ == '__main__':
    main()
