#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime
from ret.loguru import logger
from sqlalchemy import and_

import pandas as pd

from ret.config.settings import (
        MAX_TILT,
        DELTA_TILT,
        MAX_DELTA_USER_THRP_DL_PERCENTAJE,
        MAX_DELTA_TRAFFIC_DL_PERCENTAJE,
        MIN_USER_AVG,
        MAX_USER_AVG,
    )

from ret.database.tables import (
        Ret,
        Transaction,
        get_engine,
        get_session,
    )

def newtilt(tilt=None):
    logger.debug(f"tilt {tilt}")

    if not tilt:
        return

    return tilt + DELTA_TILT if tilt + DELTA_TILT < MAX_TILT else tilt

def delta_percentaje(reference=None, value=None):
    logger.debug(f"reference {reference} value {value}")

    if not reference or not value:
        return

    delta = reference - value
    return delta * 100 / reference

def evaluator(time_=None, candidates_kpis_df=pd.DataFrame()):
    '''
    Esta función recibe todas las celdas candidatas y sus kpis promedio,
    para el instante actual.
    Dependiendo de si la celda existe en la tabla transactions,
    hay comparaciones con kpis promedio iniciales.
    En base a reglas pueden entrar transacciones a la tabla transactions.
    '''
    logger.debug(f"time_ {time_}")

    if not time_:
        return

    if candidates_kpis_df.empty:
        return

    logger.debug(f"candidates_kpis_df \n{candidates_kpis_df}")

    engine = get_engine()
    session = get_session(engine=engine)

    for idx in candidates_kpis_df.index: # overshooters plain terrain
        node = candidates_kpis_df['eNodeB_Name'][idx]
        user_avg = candidates_kpis_df['user_avg'][idx]
        user_thrp_dl = candidates_kpis_df['user_thrp_dl'][idx]
        traffic_dl = candidates_kpis_df['traffic_dl'][idx]

        antennas = session.query(Ret).filter(Ret.node==node,)
        for antenna in antennas:
            if not antenna.enabled:
                continue
            logger.debug(f"node {antenna.node} deviceno {antenna.deviceno}")
            trx = session.query(Transaction).filter(
                and_(Transaction.node==antenna.node,
                    Transaction.deviceno==antenna.deviceno)).first()
            if trx:
                # si trx anterior no fue exitosa
                if not trx.success:
                    logger.debug(f"continue: success {trx.success}")
                    continue
                cond_ = delta_percentaje(
                    trx.user_thrp_dl_initial, user_thrp_dl) > MAX_DELTA_USER_THRP_DL_PERCENTAJE
                cond_ = cond_ or delta_percentaje(
                        trx.traffic_dl_initial, traffic_dl) > MAX_DELTA_TRAFFIC_DL_PERCENTAJE
                if cond_:
                    # rollback
                    logger.debug(f"rollback")
                    newtilt_  = trx.oldtilt
                else:
                    newtilt_ = newtilt(trx.newtilt)

                if trx.newtilt == newtilt_:
                    logger.debug(f"continue: newtilt_ {newtilt_}")
                    continue

                # si nuevo tilt es distinto al último
                trx.newtilt = newtilt_
                trx.generated = datetime.now()
            else:
                if not (user_avg >= MIN_USER_AVG and user_avg <= MAX_USER_AVG):
                    logger.debug(f"continue: user_avg {user_avg}")
                    continue
                if antenna.tilt == newtilt(antenna.tilt):
                    logger.debug(f"continue: antenna.tilt == newtilt(antenna.tilt)")
                    continue
                # se crea entrada en tabla transactions
                trx = Transaction(
                        node = antenna.node,
                        cellname = antenna.cellname,
                        deviceno = antenna.deviceno,
                        subunitno = antenna.subunitno,
                        tilt_initial = antenna.tilt,

                        # oldtilt = tilt_initial,
                        oldtilt = antenna.tilt,

                        # originalmente
                        # user_thrp_dl_initial = user_thrp_dl,
                        # traffic_dl_initial = traffic_dl,

                        # para ver si pasa
                        user_thrp_dl_initial = float(user_thrp_dl),
                        traffic_dl_initial = float(traffic_dl),

                        newtilt = newtilt(antenna.tilt),
                        datetimeid = time_,
                        generated = datetime.now(),
                        )
                logger.debug(f"trx \n{trx}")
                session.add(trx)
            session.commit()

    session.commit()
    session.close()
