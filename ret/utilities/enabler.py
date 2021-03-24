#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ret.loguru import logger

from ret.config.settings import (
        ENV,
    )

from ret.database.tables import (
        Ret,
        get_engine,
        get_session,
    )

def enabler(cellnames=None):
    logger.debug(f'ENV {ENV}')

    engine = get_engine()
    session = get_session(engine=engine)

    for cellname in cellnames:
        # logger.debug(f'cellname {cellname}')
        antennas = session.query(Ret).filter(Ret.cellname==cellname,)
        for antenna in antennas:
            antenna.enabled = True
            # logger.info(f'node {antenna.node} deviceno {antenna.deviceno}')
            # session.commit()

    session.commit()
    session.close()

def main():
    cellnames = ['AIS_4G_003_3', 'ARA_4G_013_3',]
    enabler(cellnames=cellnames)


if __name__ == '__main__':
    main()
