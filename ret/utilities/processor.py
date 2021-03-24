#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlalchemy import null, or_
from ret.loguru import logger
import pandas as pd

from ret.utilities.giver_of_times import giver_of_times
from ret.utilities.nbi_processor import nbi_processor
from ret.utilities.nbi_simulator import nbi_simulator
from ret.config.settings import ENV
from ret.database.tables import (
        # Ret,
        Transaction,
        get_engine,
        get_session,
    )

def processor(time_=None):
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
    # trxs = session.query(Transaction).filter(Transaction.sent.is_(null()))

    # trxs = session.query(Transaction).filter(
    #     or_(Transaction.sent.is_(null()),
    #         Transaction.oldtilt != Transaction.newtilt)).first()

    trxs = session.query(Transaction).filter(
        or_(Transaction.sent.is_(null()),
            Transaction.oldtilt != Transaction.newtilt))

    if ENV == 'sim':
        for trx in trxs:
            # logger.info(f"trx \n{trx}")
            nbi_simulator(time_=time_,session_=session,trx_=trx)

    if ENV == 'prod':
        nbi_processor(time_=time_,session_=session,trxs_=trxs)

    session.commit()
    session.close()


def main():
    for time_ in giver_of_times():
        processor(time_)


if __name__ == '__main__':
    main()
