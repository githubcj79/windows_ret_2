#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ret.loguru import logger
import pandas as pd

from ret.utilities.giver_of_times import giver_of_times
from ret.utilities.input_data import get_ta_df
from ret.config.settings import (
        ENV,
    )

import datetime
import time

def average_kpis(time_=None):
    '''
    Esta función recibe time_ de modo de poder inferir el periodo.
    Para el periodo anterior, esta función devuelve para todas las
    celdas (cellname) : user_avg, user_thrp_dl, traffic_dl.

    Los datos anteriores corresponden al promedio de los valores
    de esas muestras para la data del periodo por celda.

    Esta función devuelve un dataframe con la data anterior.
    '''
    logger.debug(f"ENV {ENV} time_ {time_}")

    if not time_:
        return

    # period = time_.strftime("%Y%m%d")
    # logger.debug(f"period {period}")

    data_df = pd.DataFrame() # empty df
    if ENV == 'sim':
        dict_ = {
                    'eNodeB_Name': ['MBTS-AIS_3G_003', 'MBTS-ARA_3G_013',],
                    # 'cellname': ['AIS_4G_003_3', 'ARA_4G_013_3',],
                    'Cell_Name': ['AIS_4G_003_3', 'ARA_4G_013_3',],
                    'user_avg': [81.0, 200.0,],
                    'user_thrp_dl': [25.4, 23.2,],
                    'traffic_dl': [8285.170, 7660.760],
                }
        data_df = pd.DataFrame.from_dict(dict_)

    if ENV == 'prod':
        data_df = get_ta_df(time_=time_)

    return data_df

def main():
    for time_ in giver_of_times():
        dict_ = average_kpis(time_)
        logger.debug(f"dict_ \n{dict_}")


if __name__ == '__main__':
    main()

'''
{"DateTimeId":1610550000000,"dateid_date":1610496000000,"dateid_hour":15,"eNodeB_Name":"MBTS-AIS_3G_003","eNodeB_identity":11000,"Cell_Name":"AIS_4G_003_3","LocalCell_Id":2,"Cellname_Id":null,"user_avg":22.2733,"user_max":33.0,"user_thrp_dl":18.6495,"delay_dl":40.2016,"delay_dl_QCI9":40.9247,"cqi_avg":8.7359,"prb_usage_dl":33.36,"user_thrp_ul":0.8735,"traffic_dl":3805.17,"traffic_ul":364.76,"prb_usage_ul":21.79,"user_tti_avg":1.2078,"user_tti_max":10.0,"cell_thrp_dl":20464.8,"cell_thrp_ul":1320.31,"drop_lte":0.17,"L_Cell_Unavail_Dur_Manual":0.0,"L_Cell_Unavail_Dur_Sys":0.0,"L_RA_TA_UE_Index0":0,"L_RA_TA_UE_Index1":0,"L_RA_TA_UE_Index2":0,"L_RA_TA_UE_Index3":48,"L_RA_TA_UE_Index4":1072,"L_RA_TA_UE_Index5":1182,"L_RA_TA_UE_Index6":397,"L_RA_TA_UE_Index7":219,"L_RA_TA_UE_Index8":2,"L_RA_TA_UE_Index9":0,"L_RA_TA_UE_Index10":0,"L_RA_TA_UE_Index11":0,"Integrity":"100%"}

{"DateTimeId":1610553600000,"dateid_date":1610496000000,"dateid_hour":16,"eNodeB_Name":"MBTS-AIS_3G_003","eNodeB_identity":11000,"Cell_Name":"AIS_4G_003_3","LocalCell_Id":2,"Cellname_Id":null,"user_avg":29.1795,"user_max":40.0,"user_thrp_dl":15.7123,"delay_dl":53.313,"delay_dl_QCI9":54.8246,"cqi_avg":9.1415,"prb_usage_dl":48.5,"user_thrp_ul":1.941,"traffic_dl":5230.18,"traffic_ul":512.543,"prb_usage_ul":22.99,"user_tti_avg":1.6526,"user_tti_max":11.0,"cell_thrp_dl":21517.3,"cell_thrp_ul":2368.17,"drop_lte":0.2,"L_Cell_Unavail_Dur_Manual":0.0,"L_Cell_Unavail_Dur_Sys":0.0,"L_RA_TA_UE_Index0":0,"L_RA_TA_UE_Index1":0,"L_RA_TA_UE_Index2":0,"L_RA_TA_UE_Index3":2,"L_RA_TA_UE_Index4":1488,"L_RA_TA_UE_Index5":1510,"L_RA_TA_UE_Index6":641,"L_RA_TA_UE_Index7":393,"L_RA_TA_UE_Index8":0,"L_RA_TA_UE_Index9":0,"L_RA_TA_UE_Index10":0,"L_RA_TA_UE_Index11":0,"Integrity":"100%"}

{"DateTimeId":1610557200000,"dateid_date":1610496000000,"dateid_hour":17,"eNodeB_Name":"MBTS-ARA_3G_013","eNodeB_identity":9001,"Cell_Name":"ARA_4G_013_3","LocalCell_Id":2,"Cellname_Id":null,"user_avg":101.156,"user_max":131.0,"user_thrp_dl":5.6445,"delay_dl":162.777,"delay_dl_QCI9":164.669,"cqi_avg":10.2781,"prb_usage_dl":92.3,"user_thrp_ul":4.5771,"traffic_dl":13268.6,"traffic_ul":2578.78,"prb_usage_ul":55.72,"user_tti_avg":7.462,"user_tti_max":26.0,"cell_thrp_dl":32289.6,"cell_thrp_ul":7400.43,"drop_lte":0.15,"L_Cell_Unavail_Dur_Manual":0.0,"L_Cell_Unavail_Dur_Sys":0.0,"L_RA_TA_UE_Index0":0,"L_RA_TA_UE_Index1":44,"L_RA_TA_UE_Index2":502,"L_RA_TA_UE_Index3":762,"L_RA_TA_UE_Index4":1982,"L_RA_TA_UE_Index5":2538,"L_RA_TA_UE_Index6":103,"L_RA_TA_UE_Index7":10294,"L_RA_TA_UE_Index8":24,"L_RA_TA_UE_Index9":0,"L_RA_TA_UE_Index10":0,"L_RA_TA_UE_Index11":0,"Integrity":"100%"}

{"DateTimeId":1610550000000,"dateid_date":1610496000000,"dateid_hour":15,"eNodeB_Name":"MBTS-ARA_3G_013","eNodeB_identity":9001,"Cell_Name":"ARA_4G_013_3","LocalCell_Id":2,"Cellname_Id":null,"user_avg":80.4986,"user_max":101.0,"user_thrp_dl":9.2923,"delay_dl":96.1259,"delay_dl_QCI9":97.207,"cqi_avg":10.5846,"prb_usage_dl":81.55,"user_thrp_ul":2.8848,"traffic_dl":12951.9,"traffic_ul":1180.85,"prb_usage_ul":42.4,"user_tti_avg":5.3647,"user_tti_max":23.0,"cell_thrp_dl":33405.9,"cell_thrp_ul":3917.33,"drop_lte":0.17,"L_Cell_Unavail_Dur_Manual":0.0,"L_Cell_Unavail_Dur_Sys":0.0,"L_RA_TA_UE_Index0":0,"L_RA_TA_UE_Index1":44,"L_RA_TA_UE_Index2":603,"L_RA_TA_UE_Index3":788,"L_RA_TA_UE_Index4":2113,"L_RA_TA_UE_Index5":2695,"L_RA_TA_UE_Index6":65,"L_RA_TA_UE_Index7":5960,"L_RA_TA_UE_Index8":5,"L_RA_TA_UE_Index9":0,"L_RA_TA_UE_Index10":0,"L_RA_TA_UE_Index11":0,"Integrity":"100%"}

{"DateTimeId":1610553600000,"dateid_date":1610496000000,"dateid_hour":16,"eNodeB_Name":"MBTS-ARA_3G_013","eNodeB_identity":9001,"Cell_Name":"ARA_4G_013_3","LocalCell_Id":2,"Cellname_Id":null,"user_avg":84.6389,"user_max":111.0,"user_thrp_dl":6.5625,"delay_dl":169.307,"delay_dl_QCI9":170.579,"cqi_avg":10.367,"prb_usage_dl":87.34,"user_thrp_ul":3.7688,"traffic_dl":13299.3,"traffic_ul":1564.47,"prb_usage_ul":45.55,"user_tti_avg":6.5894,"user_tti_max":27.0,"cell_thrp_dl":33279.8,"cell_thrp_ul":5030.48,"drop_lte":0.14,"L_Cell_Unavail_Dur_Manual":0.0,"L_Cell_Unavail_Dur_Sys":0.0,"L_RA_TA_UE_Index0":0,"L_RA_TA_UE_Index1":43,"L_RA_TA_UE_Index2":584,"L_RA_TA_UE_Index3":914,"L_RA_TA_UE_Index4":2049,"L_RA_TA_UE_Index5":2288,"L_RA_TA_UE_Index6":68,"L_RA_TA_UE_Index7":7693,"L_RA_TA_UE_Index8":5,"L_RA_TA_UE_Index9":0,"L_RA_TA_UE_Index10":0,"L_RA_TA_UE_Index11":0,"Integrity":"100%"}
'''
