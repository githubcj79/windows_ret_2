#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ret.loguru import logger
from sqlalchemy import and_, func, DateTime

from ret.config.settings import (
        ENV,
    )

from ret.database.tables import (
        Ret,
        Transaction,
        get_engine,
        get_session,
    )

import datetime

def ret_updater(node=None, deviceno=None, tilt=None, session=None):
    logger.debug(f"ENV {ENV}")

    if not node or not deviceno or not tilt or not session:
        return

    ret = session.query(func.max(Ret.datetimeid)).first()
    logger.debug(f"ret {ret} type {type(ret)}")

    for datetimeid_ in ret:
        pass

    if not datetimeid_:
        return

    trx = session.query(Ret).filter(
                and_(
                    Ret.node == node,
                    Ret.deviceno == deviceno,
                    Ret.datetimeid == datetimeid_
                    )
                ).first()

    if not trx:
        return

    trx.tilt = tilt
    session.commit()

def trx_updater(commands=None, sent_=None):
    '''
    Esta función recibe una lista de diccionarios, con las respuestas
    a los comandos de cambio de tilt ejecutados en el NBI.
    Si el resultado es exitoso se actualizan las tablas rets y
    transactions.
    '''
    logger.debug(f"ENV {ENV}")

    if not commands:
        return

    engine = get_engine()
    session = get_session(engine=engine)

    for command in commands:
        result = command['data']['result']
        logger.debug(f"result {result}")
        executed_time_stamp_str = command['data']['executed_time_stamp']
        executed_time_stamp = datetime.datetime.strptime(
                            executed_time_stamp_str, '%Y-%m-%d %H:%M:%S')
        object_id = command['object_id']
        trx = session.query(Transaction).filter(
                Transaction.id==object_id).first()
        if not trx:
            session.commit()
            session.close()
            return

        trx.sent = sent_
        if result:
            trx.oldtilt = trx.newtilt
            trx.success = executed_time_stamp
            ret_updater(node=trx.node, deviceno=trx.deviceno,
                        tilt=trx.newtilt, session=session)
        else:
            logger.info(f"result {result}")
            trx.failure = executed_time_stamp

    session.commit()
    session.close()
