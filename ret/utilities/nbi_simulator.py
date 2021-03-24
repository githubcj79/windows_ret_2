#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime
from ret.loguru import logger
from random import randint

# from settings import ENV

def failure_percentage():
    FAILURE_PERCENTAGE=5

    random_int = randint(1, 100)

    if random_int <= FAILURE_PERCENTAGE:
        return False
    return True

def nbi_simulator(time_=None,session_=None,trx_=None):
    logger.debug(f"time_ {time_}")

    if not time_ or not session_ or not trx_:
        pass

    logger.info(f"trx_ \n{trx_}")
    # logger.info(f"ENV {ENV}")

    # if ENV == 'sim':

    logger.info(f"ENV {ENV}")

    trx_.sent = datetime.now()

    nbi_response = failure_percentage()
    logger.info(f"nbi_response {nbi_response}")

    if nbi_response:
        trx_.success = datetime.now()
    else:
        trx_.failure = datetime.now()

    session_.commit()
