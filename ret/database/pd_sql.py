#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import mysql.connector
import pandas as pd
from ret.loguru import logger
from mysql.connector import errorcode
# from mysql.connector.cursor import MySQLCursor

from ret.config.settings import (
        ENV,
        host_,
        database_,
        user_,
        password_,
        port_,
    )

def pd_sql(time_=None, query_=None):
    logger.debug(f'ENV {ENV}')

    if not time_:
        logger.info(f'time_ {time_}')
        return

    try:

        cnx = mysql.connector.connect(
                user=user_,
                password=password_,
                host=host_,
                database=database_,
                use_pure=True,
            )

        query = query_
        df = pd.read_sql(query,cnx)
        logger.info(f'df.shape {df.shape}')
        cnx.close()
        return df

    except mysql.connector.Error as err:
      if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        logger.error("Something is wrong with your user name or password")
      elif err.errno == errorcode.ER_BAD_DB_ERROR:
        logger.error("Database does not exist")
      else:
        logger.error(err)
    else:
      cnx.close()

def main():
    # time_=datetime.datetime(2021, 2, 25, 10, 30, 0, 0)
    # day_before = time_  - datetime.timedelta(days=1)

    now_ = datetime.datetime.now()
    time_ = now_
    period = time_.strftime("%Y-%m-%d")

    query_ = f'''
    select distinct * from lcellreference as l
    where STR_TO_DATE(l.dateid, '%Y-%m-%d') = '{period}';
    '''

    df = pd_sql(time_=time_, query_=query_)
    logger.debug(f'df.shape {df.shape}')

if __name__ == '__main__':
    main()

'''
/*
This query uses a subquery in the FROM clause.
The subquery is given an alias x so that we can
refer to it in the outer select statement.
*/
select x.ProductID,
    y.ProductName,
    x.max_unit_price
from
(
    select ProductID, max(UnitPrice) as max_unit_price
    from order_details
    group by ProductID
) as x
inner join products as y on x.ProductID = y.ProductID
'''
