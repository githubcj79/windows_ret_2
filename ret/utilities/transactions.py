#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlalchemy import null
from ret.loguru import logger
import pandas as pd

from ret.utilities.giver_of_times import giver_of_times
from ret.utilities.processor import processor
from ret.database.tables import (
        Ret,
        Transaction,
        get_engine,
        get_session,
    )

def transactions(time_=None):
    '''
    Esta función detecta las transacciones en la tabla transactions.
    y las ejecuta sobre el nbi.
    Por ahora no se reintentan transacciones fallidas.
    '''
    logger.debug(f"time_ {time_}")

    if not time_:
        return

    engine = get_engine()
    session = get_session(engine=engine)

    # detectar las transacciones a procesar
    trxs = session.query(Transaction).filter(Transaction.sent.is_(null()))
    for trx in trxs:
        # logger.info(f"trx \n{trx}")
        processor(time_=time_,session_=session,trx_=trx)

    session.commit()
    session.close()


def main():
    for time_ in giver_of_times():
        transactions(time_)


if __name__ == '__main__':
    main()
