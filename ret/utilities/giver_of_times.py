#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ret.loguru import logger

import datetime
import time

from ret.config.settings import ENV

def giver_of_times():
    '''
    Esta funcion es un generador de tiempos.
    Estos tiempos se asocian a los ciclos.
    Es el reloj del simulador.
    '''
    logger.debug(f"")

    time_list = [
                    datetime.datetime(2021, 1, 10, 10, 30, 0, 0),
                    datetime.datetime(2021, 1, 11, 10, 30, 0, 0),
                    datetime.datetime(2021, 1, 12, 10, 30, 0, 0),
                    datetime.datetime(2021, 1, 13, 10, 30, 0, 0),
                ]

    seconds = 1

    if ENV == 'sim':
        for time_ in time_list:
            time.sleep(seconds)
            yield time_

    if ENV == 'prod':
        yield datetime.datetime.now()

def main():
    for time_ in giver_of_times():
        logger.debug(f"time_ {time_}")


if __name__ == '__main__':
    main()
