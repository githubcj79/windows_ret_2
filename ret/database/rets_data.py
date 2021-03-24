#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
from ret.loguru import logger

from ret.database.pd_sql import pd_sql

from ret.config.settings import (
        ENV,
    )

def rets_data(time_=None):
    logger.debug(f'ENV {ENV}')

    if not time_:
        logger.info(f'time_ {time_}')
        return

    when_ = time_
    period = when_.strftime("%Y-%m-%d")

    query_ = f'''
    select x.datetimeid, x.node, x.devicename, x.deviceno, x.tilt,
    x.subname, x.subunitno, y.localcellid, y.eci, y.cellname
    from (select
    ret.dateid as datetimeid,
    ret.node as node,
    ret.devicename as devicename,
    ret.deviceno as deviceno,
    sub.tilt as tilt,
    sub.subname as subname,
    sub.subunitno as subunitno
    from ret
    inner join retsubunit sub on
    date(ret.dateid) = date(sub.dateid) and
    ret.node = sub.node and
    ret.deviceno = sub.deviceno
    where date(ret.dateid) = current_date) as x
    inner join lcellreference as y
    on (x.node = y.node
    and (x.deviceno = y.localcellid or x.deviceno = y.localcellid + 10)
    and STR_TO_DATE(y.dateid, '%Y-%m-%d') = '{period}');
    '''

    return pd_sql(time_=time_, query_=query_)

def main():
    # now_ = datetime.datetime.now()
    # day_before = now_  - datetime.timedelta(days=1)
    time_ = datetime.datetime.now()
    df = rets_data(time_=time_)


if __name__ == '__main__':
    main()
