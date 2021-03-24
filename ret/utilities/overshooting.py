#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import pandas as pd
import numpy as np

from ret.loguru import logger

from ret.config.settings import (
        ENV,
        KM,
        D,
        N_DISTANCE,
        TERRAIN_DELTA,
        SAMPLES_PERCENTAGE,
    )

from ret.utilities.input_data import (
        get_cells_df,
        get_ta_df,
    )

from ret.utilities.neighborhood import (
        neighborhood,
    )

TA_COLUMNS = None
TA_INDEX = None

str_translation = [
                        '0 - 156 mts',
                        '156 - 234 mts',
                        '234 - 546 mts',
                        '546 - 1014 mts',
                        '1.01 - 1.9 Km',
                        '1.9 - 3.5 Km',
                        '3.5 - 6.6 Km',
                        '6.6 - 14.4 Km',
                        '14.4  - 30 Km',
                        '30 - 53 Km',
                        '53 - 76 Km',
                        '76.8 - ... Km',
                    ]
num_translation = [
                        156,
                        234,
                        546,
                        1014,
                        1900,
                        3500,
                        6600,
                        14400,
                        30000,
                        53000,
                        76000,
                        100000,
                    ]

def ta_percentaje_distance( row ):
    # print(f'TA_COLUMNS={TA_COLUMNS}') # debug
    total = 0.0
    for i in range(1,13):
        total += row[TA_INDEX[i]]
    parcial_percentage = int(total * SAMPLES_PERCENTAGE / 100)
    parcial = 0.0
    for i in range(1,13):
        parcial += row[TA_INDEX[i]]
        if parcial >= parcial_percentage:
            return num_translation[i-1]/1000

def ta_percentaje_index( row ):
    # print(f'TA_COLUMNS={TA_COLUMNS}') # debug
    total = 0.0
    for i in range(1,13):
        total += row[TA_INDEX[i]]
    parcial_percentage = int(total * SAMPLES_PERCENTAGE / 100)
    parcial = 0.0
    for i in range(1,13):
        parcial += row[TA_INDEX[i]]
        if parcial >= parcial_percentage:
            # return num_translation[i-1]/1000
            return i-1

def distance_percentaje_index( row ):
    for i in range(len(num_translation)):
        if (row['distance_'] * 1000) <= num_translation[i]:
            return i

def _overshooting_intensity( row ):
    # parcial_sum = row['L_RA_TA_UE_Index11'] - row[f"L_RA_TA_UE_Index{row['distance_index']}"]
    parcial_percentage = row['cum_sum_out']
    if parcial_percentage < 5.0:
        return 'Low'
    elif parcial_percentage >= 15.0:
        return 'High'
    else:
        return 'Medium'

def cum_sum_out(row):
    parcial_sum = row['L_RA_TA_UE_Index11'] - row[f"L_RA_TA_UE_Index{row['distance_index']}"]
    return parcial_sum * 100 / (row['L_RA_TA_UE_Index11']+1)

def overshooting_intensity(neighborhood_df=pd.DataFrame(),
                    ta_df=pd.DataFrame()):
    global TA_COLUMNS, TA_INDEX
    logger.info(f'overshooting_intensity:')

    if neighborhood_df.empty:
        neighborhood_df = neighborhood()
        neighborhood_df.reset_index(inplace=True)

    l = ['CELLNAME', 'distance_']
    t = ['CELLNAME']
    overshooters_intensity_df = neighborhood_df[l].groupby(t).mean()
    overshooters_intensity_df.reset_index(inplace=True)
    overshooters_intensity_df['distance_index'] = overshooters_intensity_df.apply(distance_percentaje_index, axis=1)

    if ta_df.empty:
        ta_df = get_ta_df()

    l = ['L_RA_TA_UE_Index0','L_RA_TA_UE_Index1', 'L_RA_TA_UE_Index2',
        'L_RA_TA_UE_Index3', 'L_RA_TA_UE_Index4','L_RA_TA_UE_Index5',
        'L_RA_TA_UE_Index6','L_RA_TA_UE_Index7', 'L_RA_TA_UE_Index8',
        'L_RA_TA_UE_Index9', 'L_RA_TA_UE_Index10','L_RA_TA_UE_Index11',]

    ra_ta_df = ta_df[l]
    cumulative_df = ra_ta_df.cumsum(axis = 1, skipna = True)

    l = ['Cell_Name']
    ta_df_ = pd.concat([ta_df[l], cumulative_df], axis=1)

    merged_df = pd.merge(overshooters_intensity_df, ta_df_, how="inner", left_on='CELLNAME', right_on='Cell_Name').drop_duplicates()

    # merged_df['overshooting'] = merged_df.apply(_overshooting_intensity, axis=1)

    # ---------------------------------------
    merged_df['cum_sum_out'] = merged_df.apply(cum_sum_out, axis=1)
    merged_df['overs_intensity'] = merged_df.apply(_overshooting_intensity, axis=1)
    # ---------------------------------------

    new_merged_df = merged_df.drop(['Cell_Name'], axis = 1)

    return neighborhood_df, new_merged_df

def overshooting(neighborhood_df=pd.DataFrame(),
                    ta_df=pd.DataFrame()):
    global TA_COLUMNS, TA_INDEX
    logger.info(f'overshooting:')

    if neighborhood_df.empty:
        neighborhood_df = neighborhood()
        neighborhood_df.reset_index(inplace=True)

    # Detecting cells' overshooting

    l = ['CELLNAME', 'distance_']
    t = ['CELLNAME']
    overshooters_df = neighborhood_df[l].groupby(t).mean()
    overshooters_df.reset_index(inplace=True)

    if ta_df.empty:
        ta_df = get_ta_df()

    l = ['Cell_Name', 'L_RA_TA_UE_Index0',
                            'L_RA_TA_UE_Index1', 'L_RA_TA_UE_Index2',
                            'L_RA_TA_UE_Index3', 'L_RA_TA_UE_Index4',
                            'L_RA_TA_UE_Index5', 'L_RA_TA_UE_Index6',
                            'L_RA_TA_UE_Index7', 'L_RA_TA_UE_Index8',
                            'L_RA_TA_UE_Index9', 'L_RA_TA_UE_Index10',
                            'L_RA_TA_UE_Index11',
                            ]
    TA_COLUMNS = l
    TA_INDEX = {i:f for i,f in enumerate(l)}

    ta_df = ta_df[l].drop_duplicates().copy()
    ta_df['ta_'] = ta_df.apply(ta_percentaje_distance, axis=1)

    l = ['Cell_Name', 'ta_']
    t =['CELLNAME', 'ta_', 'distance_']
    overshooters_df = pd.merge(overshooters_df, ta_df[l], how="inner", left_on='CELLNAME', right_on='Cell_Name')[t].drop_duplicates()

    overshooters_df['overshooter'] = overshooters_df['ta_'] > overshooters_df['distance_']

    return neighborhood_df, overshooters_df

def overshooters(time_=None, neighborhood_df=pd.DataFrame(),
    cells_df=pd.DataFrame()):
    logger.debug(f'ENV {ENV}')

    if not time_:
        logger.info(f'time_ {time_}')
        return
    # ------------------------------------------
    # fue necesario setear tiempos para avanzar con las pruebas ..
    # now_ = datetime.datetime.now()


    if neighborhood_df.empty:
        neighborhood_df, cells_df = neighborhood(time_)

    day_before = time_  - datetime.timedelta(days=1)
    ta_df = get_ta_df(time_=day_before)

    # neighborhood_df.reset_index(inplace=True)
    # cells_df.reset_index(inplace=True)
    # ta_df.reset_index(inplace=True)
    # ------------------------------------------

    neighborhood_df, overshooters_df = overshooting(neighborhood_df=neighborhood_df, ta_df=ta_df)
    # neighborhood_df.to_excel(r'data/neighborhood_df.xlsx', index = False)
    # overshooters_df.to_excel(r'data/overshooters_df.xlsx', index = False)

    # neighborhood_df.reset_index(inplace=True)
    # overshooters_df.reset_index(inplace=True)

    neighborhood_df, overshooters_intensity_df = overshooting_intensity(neighborhood_df=neighborhood_df, ta_df=ta_df)
    # neighborhood_df.to_excel(r'data/neighborhood_df.xlsx', index = False)
    overshooters_intensity_df.to_excel(r'data/overshooters_intensity_df.xlsx', index = False)

    # neighborhood_df.reset_index(inplace=True)
    # overshooters_intensity_df.reset_index(inplace=True)

    intensity_df = overshooters_intensity_df.drop(['distance_'], axis = 1)
    # intensity_df.reset_index(inplace=True)

    merged_df = pd.merge(overshooters_df, intensity_df, how="inner", left_on='CELLNAME', right_on='CELLNAME').drop_duplicates()
    # merged_df.to_excel(r'data/merged_df.xlsx', index = False)
    # merged_df.reset_index(inplace=True)

    # ------------------------------------------------
    l = ['CELLNAME', 'ta_', 'distance_', 'overshooter', 'overs_intensity']
    overshooters_df = merged_df[l].drop_duplicates()

    l = ['cellname', 'ta_calculated', 'average_distance', 'overshooter', 'intensity']
    overshooters_df.columns = l

    overshooters_df['datetimeid'] = cells_df.iloc[0]['Dateid']
    # ------------------------------------------------

    # return overshooters_df, intensity_df, merged_df
    return overshooters_df

def main():
    now_ = datetime.datetime.now()
    # when_ = now_  - datetime.timedelta(days=2)
    when_ = now_
    # overshooters_df, intensity_df, merged_df = overshooters(time_=when_)
    overshooters_df = overshooters(time_=when_)


if __name__ == '__main__':
    main()
