#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
from ret.loguru import logger

from ret.database.pd_sql import pd_sql

from ret.config.settings import (
        ENV,
    )

def cells_data(time_=None):
    logger.debug(f'ENV {ENV} time_ {time_}')

    if not time_:
        logger.info(f'time_ {time_}')
        return

    when_ = time_
    period = when_.strftime("%Y-%m-%d")
    logger.debug(f'period {period}')

    query_ = f'''
    select distinct * from lcellreference as l
    where STR_TO_DATE(l.dateid, '%Y-%m-%d') = '{period}';
    '''

    return pd_sql(time_=time_, query_=query_)

def main():
    # when_ = datetime.datetime.now()
    # day_before = time_  - datetime.timedelta(days=1)
    time_ = datetime.datetime.now()
    df = cells_data(time_=time_)


if __name__ == '__main__':
    main()
