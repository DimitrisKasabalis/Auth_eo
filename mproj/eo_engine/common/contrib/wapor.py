# -*- coding: utf-8 -*-
"""
WaterSat
author: Tim Martijn Hessels
Created on Sun Sep 15 18:40:44 2019
"""

import datetime
import os
import urllib

import json
import pandas as pd
import requests
import time
from pyproj import Proj, transform


def WAPOR(output_folder,
          start_date, end_date, lat_lim, lon_lim, auth_token, parameter, area=None, version="2"):
    time_steps = parameter.split("_")[-1]
    level = parameter.split("_")[0]

    # Find the time frequency of the parameter
    if time_steps == "A":
        freq = "AS"
    if time_steps == "M":
        freq = "MS"
    if time_steps == "D":
        freq = "MS"

        # Find LEVEL type
    if level == "L3":
        Parameter_first_part = "_".join(parameter.split("_")[0:-1])
        parameter = "_".join([Parameter_first_part, "{AREA}", parameter.split("_")[-1]])
        version = "1"  # LEVEL 3 is only available in Version 1 dataset

    # Define dates
    firstDateOfMonth = start_date[0:8] + "01"
    Dates = pd.date_range(firstDateOfMonth, end_date, freq='MS')

    if time_steps == "D":
        for Date in Dates:
            if Date == Dates[0]:
                # Dates_end = [pd.Timestamp(datetime.datetime(Date.year, Date.month, 1)), pd.Timestamp(datetime.datetime(Date.year, Date.month, 11)), pd.Timestamp(datetime.datetime(Date.year, Date.month, 21))]
                Dates_end = []
            # else:
            # Dates_end.append(pd.Timestamp(datetime.datetime(Date.year, Date.month, 1)))
            # Dates_end.append(pd.Timestamp(datetime.datetime(Date.year, Date.month, 11)))
            # Dates_end.append(pd.Timestamp(datetime.datetime(Date.year, Date.month, 21)))
            for i in [1, 11, 21]:
                newdate = pd.Timestamp(datetime.datetime(Date.year, Date.month, i))
                if ((newdate <= pd.Timestamp(
                        datetime.datetime(int(end_date[0:4]), int(end_date[5:7]), int(end_date[8:10])))) & (
                        newdate >= pd.Timestamp(
                    datetime.datetime(int(start_date[0:4]), int(start_date[5:7]), int(start_date[8:10]))))):
                    Dates_end.append(newdate)
    else:
        Dates_end = Dates

    # Define server
    # provides query operations on data and metadata stored in the system
    # https://io.apps.fao.org/gismgr/api/v1/swagger-ui.html#/
    url = 'https://io.apps.fao.org/gismgr/api/v1/query'

    # Login into WAPOR
    sign_in = 'https://io.apps.fao.org/gismgr/api/v1/iam/sign-in'
    resp_vp = requests.post(sign_in, headers={'X-GISMGR-API-KEY': auth_token})
    resp_vp = resp_vp.json()
    token = resp_vp['response']['accessToken']

    # Set header type
    header = {"Authorization": "Bearer " + token,
              "Content-type": "application/json;charset=UTF-8",
              "Accept": "application/json"
              }

    # Get constant variables for payload
    measure = VariablesInfo.measures[parameter]
    dimension = VariablesInfo.dimensions[parameter]
    version = VariablesInfo.versions[version]

    output_folder_para = os.path.join(output_folder, parameter.format(AREA=area))
    if not os.path.exists(output_folder_para):
        os.makedirs(output_folder_para)

    if parameter.split("_")[0] == "L1":
        lat_lim[0] = lat_lim[0] - 0.1
        lat_lim[1] = lat_lim[1] + 0.1
        lon_lim[0] = lon_lim[0] - 0.1
        lon_lim[1] = lon_lim[1] + 0.1

    if level == "L3":
        Projection = LEVEL3.Projection[area]
        inProj = Proj(init='epsg:4326')
        outProj = Proj(init='epsg:%d' % Projection)
        Projection = Projection

        lon_lim[0], lat_lim[1] = transform(inProj, outProj, lon_lim[0], lat_lim[1])
        lon_lim[1], lat_lim[0] = transform(inProj, outProj, lon_lim[1], lat_lim[0])

    else:
        Projection = 4326

    if level == "L3":
        parameter = parameter.format(AREA=area)

    # Loop over the dates
    for Date_end in Dates_end:

        # Set the required time period
        if time_steps == "D":
            Start_day_payload = Date_end.day
            End_year_payload = Date_end.year
            if Start_day_payload == 21:
                End_day_payload = 1
                End_month_payload = Date_end.month + 1
                if End_month_payload == 13:
                    End_month_payload = 1
                    End_year_payload += 1
            else:
                End_day_payload = Start_day_payload + 10
                End_month_payload = Date_end.month

        if time_steps == "M":
            Start_day_payload = Date_end.day
            End_day_payload = 1
            End_month_payload = Date_end.month + 1
            End_year_payload = Date_end.year
            if End_month_payload == 13:
                End_month_payload = 1
                End_year_payload += 1

        if time_steps == "A":
            Start_day_payload = Date_end.day
            End_day_payload = 1
            End_month_payload = 1
            End_year_payload = Date_end.year + 1

        file_name_temp = os.path.join(output_folder_para, "%s_WAPOR_%s_%s.%02d.%02d.tif" % (
            parameter, dimension, Date_end.year, Date_end.month, Start_day_payload))

        if not os.path.exists(file_name_temp):

            # Create payload file
            payload = Create_Payload_JSON(parameter, Date_end, Start_day_payload, End_year_payload, End_month_payload,
                                          End_day_payload, lat_lim, lon_lim, version, dimension, measure, Projection)
            success = 0
            no_succes = 0

            while success == 0 and no_succes < 10:

                try:
                    # Collect the date by using the payload file
                    response = requests.post(url, data=json.dumps(payload), headers=header)
                    response.raise_for_status()

                    response_json = response.json()
                    result = response_json['response']

                    job_url = result['links'][0]['href']

                    # output filename
                    print("Try to create %s" % file_name_temp)

                    time.sleep(10)
                    job_response = requests.get(job_url, headers=header)
                    if job_response.status_code == 200:
                        while job_response.json()['response']['status'] == 'RUNNING':
                            time.sleep(30)
                            job_response = requests.get(job_url, headers=header)

                        if job_response.json()['response']['status'] == 'COMPLETED':
                            job_result = job_response.json()['response']['output']['downloadUrl']
                            urllib.request.urlretrieve(job_result, file_name_temp)
                            print("Created %s succesfully!!!" % file_name_temp)
                            success = 1
                        else:
                            print("ERROR: Was not able to create output")

                    else:
                        print("ERROR: Was not able to connect to WAPOR server")
                except Exception:
                    success = 0
                    no_succes += 1
                    if no_succes == 10:
                        print("ERROR: already tried 10 times, and no connection with server. Please run code again")
    return ()


def Create_Payload_JSON(parameter,
                        date_end,
                        start_day_payload,
                        end_year_payload,
                        end_month_payload,
                        end_day_payload,
                        lat_lim,
                        lon_lim, version, dimension, measure, projection):
    payload = {
        "type": "CropRaster",
        "params": {
            "properties": {
                "outputFileName": "%s_clipped.tif" % parameter,
                "cutline": True,
                "tiled": True,
                "compressed": True,
                "overviews": True
            },
            "cube": {
                "code": parameter,
                "workspaceCode": version,
                "language": "en"
            },
            "dimensions": [
                {
                    "code": dimension,
                    "values": [
                        "[%d-%02d-%02d,%d-%02d-%02d)" % (
                            date_end.year, date_end.month, start_day_payload, end_year_payload, end_month_payload,
                            end_day_payload)
                    ]
                }
            ],
            "measures": [
                measure
            ],
            "shape": {
                "crs": "EPSG:%d" % projection,
                "type": "Polygon",
                "coordinates": [
                    [
                        [
                            lon_lim[0],
                            lat_lim[1]
                        ],
                        [
                            lon_lim[1],
                            lat_lim[1]
                        ],
                        [
                            lon_lim[1],
                            lat_lim[0]
                        ],
                        [
                            lon_lim[0],
                            lat_lim[0]
                        ],
                        [
                            lon_lim[0],
                            lat_lim[1]
                        ]
                    ]
                ]
            }
        }
    }

    return payload


class VariablesInfo:
    """
    This class contains the information about the WAPOR variables
    """

    descriptions = {'L1_GBWP_A': 'Gross Biomass Water Productivity',
                    'L1_NBWP_A': 'Net Biomass Water Productivity',
                    'L1_AETI_A': 'Actual EvapoTranspiration and Interception (Annual)',
                    'L1_AETI_M': 'Actual EvapoTranspiration and Interception (Monthly)',
                    'L1_AETI_D': 'Actual EvapoTranspiration and Interception (Dekadal)',
                    'L1_T_A': 'Transpiration (Annual)',
                    'L1_E_A': 'Evaporation (Annual)',
                    'L1_I_A': 'Interception (Annual)',
                    'L1_T_D': 'Transpiration (Dekadal)',
                    'L1_E_D': 'Evaporation (Dekadal)',
                    'L1_I_D': 'Interception (Dekadal)',
                    'L1_NPP_D': 'Net Primary Production',
                    'L1_TBP_A': 'Total Biomass Production (Annual)',
                    'L1_LCC_A': 'Land Cover Classification',
                    'L1_RET_A': 'Reference EvapoTranspiration (Annual)',
                    'L1_PCP_A': 'Precipitation (Annual)',
                    'L1_RET_M': 'Reference EvapoTranspiration (Monthly)',
                    'L1_PCP_M': 'Precipitation (Monthly)',
                    'L1_RET_D': 'Reference EvapoTranspiration (Dekadal)',
                    'L1_PCP_D': 'Precipitation (Dekadal)',
                    'L1_RET_E': 'Reference EvapoTranspiration (Daily)',
                    'L1_PCP_E': 'Precipitation (Daily)',
                    'L1_QUAL_NDVI_D': 'Quality of Normalized Difference Vegetation Index (Dekadal)',
                    'L1_QUAL_LST_D': 'Quality Land Surface Temperature (Dekadal)',
                    # 'L2_GBWP_S': 'Gross Biomass Water Productivity (Seasonal)',
                    'L2_AETI_A': 'Actual EvapoTranspiration and Interception (Annual)',
                    'L2_AETI_M': 'Actual EvapoTranspiration and Interception (Monthly)',
                    'L2_AETI_D': 'Actual EvapoTranspiration and Interception (Dekadal)',
                    'L2_T_A': 'Transpiration (Annual)',
                    'L2_E_A': 'Evaporation (Annual)',
                    'L2_I_A': 'Interception (Annual)',
                    'L2_T_D': 'Transpiration (Dekadal)',
                    'L2_E_D': 'Evaporation (Dekadal)',
                    'L2_I_D': 'Interception (Dekadal)',
                    'L2_NPP_D': 'Net Primary Production',
                    # 'L2_TBP_S': 'Total Biomass Production (Seasonal)',
                    'L2_LCC_A': 'Land Cover Classification',
                    # 'L2_PHE_S': 'Phenology (Seasonal)',
                    'L2_QUAL_NDVI_D': 'Quality of Normalized Difference Vegetation Index (Dekadal)',
                    'L2_QUAL_LST_D': 'Quality Land Surface Temperature (Dekadal)',
                    'L3_AETI_A': 'Actual EvapoTranspiration and Interception (Annual)',
                    'L3_AETI_M': 'Actual EvapoTranspiration and Interception (Monthly)',
                    'L3_AETI_D': 'Actual EvapoTranspiration and Interception (Dekadal)',
                    'L3_T_A': 'Transpiration (Annual)',
                    'L3_E_A': 'Evaporation (Annual)',
                    'L3_I_A': 'Interception (Annual)',
                    'L3_T_D': 'Transpiration (Dekadal)',
                    'L3_E_D': 'Evaporation (Dekadal)',
                    'L3_I_D': 'Interception (Dekadal)',
                    'L3_NPP_D': 'Net Primary Production',
                    'L3_QUAL_NDVI_D': 'Quality of Normalized Difference Vegetation Index (Dekadal)',
                    'L3_QUAL_LST_D': 'Quality Land Surface Temperature (Dekadal)',
                    'L3_LCC_A': 'Land Cover Classification'}

    measures = {'L1_GBWP_A': 'WPR',
                'L1_NBWP_A': 'WPR',
                'L1_AETI_A': 'WATER_MM',
                'L1_AETI_M': 'WATER_MM',
                'L1_AETI_D': 'WATER_MM',
                'L1_T_A': 'WATER_MM',
                'L1_E_A': 'WATER_MM',
                'L1_I_A': 'WATER_MM',
                'L1_T_D': 'WATER_MM',
                'L1_E_D': 'WATER_MM',
                'L1_I_D': 'WATER_MM',
                'L1_NPP_D': 'NPP',
                'L1_TBP_A': 'LPR',
                'L1_LCC_A': 'LCC',
                'L1_RET_A': 'WATER_MM',
                'L1_PCP_A': 'WATER_MM',
                'L1_RET_M': 'WATER_MM',
                'L1_PCP_M': 'WATER_MM',
                'L1_RET_D': 'WATER_MM',
                'L1_PCP_D': 'WATER_MM',
                'L1_RET_E': 'WATER_MM',
                'L1_PCP_E': 'WATER_MM',
                'L1_QUAL_NDVI_D': 'N_DEKADS',
                'L1_QUAL_LST_D': 'N_DAYS',
                # 'L2_GBWP_S': 'WPR',
                'L2_AETI_A': 'WATER_MM',
                'L2_AETI_M': 'WATER_MM',
                'L2_AETI_D': 'WATER_MM',
                'L2_T_A': 'WATER_MM',
                'L2_E_A': 'WATER_MM',
                'L2_I_A': 'WATER_MM',
                'L2_T_D': 'WATER_MM',
                'L2_E_D': 'WATER_MM',
                'L2_I_D': 'WATER_MM',
                'L2_NPP_D': 'NPP',
                # 'L2_TBP_S': 'LPR',
                'L2_LCC_A': 'LCC',
                # 'L2_PHE_S': 'PHE',
                'L2_QUAL_NDVI_D': 'N_DEKADS',
                'L2_QUAL_LST_D': 'N_DAYS',
                'L3_AETI_{AREA}_A': 'WATER_MM',
                'L3_AETI_{AREA}_M': 'WATER_MM',
                'L3_AETI_{AREA}_D': 'WATER_MM',
                'L3_T_{AREA}_A': 'WATER_MM',
                'L3_E_{AREA}_A': 'WATER_MM',
                'L3_I_{AREA}_A': 'WATER_MM',
                'L3_T_{AREA}_D': 'WATER_MM',
                'L3_E_{AREA}_D': 'WATER_MM',
                'L3_I_{AREA}_D': 'WATER_MM',
                'L3_NPP_{AREA}_D': 'NPP',
                'L3_QUAL_NDVI_{AREA}_D': 'N_DEKADS',
                'L3_QUAL_LST_{AREA}_D': 'N_DAYS',
                'L3_LCC_{AREA}_A': 'LCC'}

    dimensions = {
        'L1_GBWP_A': 'YEAR',
        'L1_NBWP_A': 'YEAR',
        'L1_AETI_A': 'YEAR',
        'L1_AETI_M': 'MONTH',
        'L1_AETI_D': 'DEKAD',
        'L1_T_A': 'YEAR',
        'L1_E_A': 'YEAR',
        'L1_I_A': 'YEAR',
        'L1_T_D': 'DEKAD',
        'L1_E_D': 'DEKAD',
        'L1_I_D': 'DEKAD',
        'L1_NPP_D': 'DEKAD',
        'L1_TBP_A': 'YEAR',
        'L1_LCC_A': 'YEAR',
        'L1_RET_A': 'YEAR',
        'L1_PCP_A': 'YEAR',
        'L1_RET_M': 'MONTH',
        'L1_PCP_M': 'MONTH',
        'L1_RET_D': 'DEKAD',
        'L1_PCP_D': 'DEKAD',
        'L1_RET_E': 'DAY',
        'L1_PCP_E': 'DAY',
        'L1_QUAL_NDVI_D': 'DEKAD',
        'L1_QUAL_LST_D': 'DEKAD',
        # 'L2_GBWP_S': 'SEASON',
        'L2_AETI_A': 'YEAR',
        'L2_AETI_M': 'MONTH',
        'L2_AETI_D': 'DEKAD',
        'L2_T_A': 'YEAR',
        'L2_E_A': 'YEAR',
        'L2_I_A': 'YEAR',
        'L2_T_D': 'DEKAD',
        'L2_E_D': 'DEKAD',
        'L2_I_D': 'DEKAD',
        'L2_NPP_D': 'DEKAD',
        # 'L2_TBP_S': 'SEASON',
        'L2_LCC_A': 'YEAR',
        # 'L2_PHE_S': 'SEASON',
        'L2_QUAL_NDVI_D': 'DEKAD',
        'L2_QUAL_LST_D': 'DEKAD',
        'L3_AETI_{AREA}_A': 'YEAR',
        'L3_AETI_{AREA}_M': 'MONTH',
        'L3_AETI_{AREA}_D': 'DEKAD',
        'L3_T_{AREA}_A': 'YEAR',
        'L3_E_{AREA}_A': 'YEAR',
        'L3_I_{AREA}_A': 'YEAR',
        'L3_T_{AREA}_D': 'DEKAD',
        'L3_E_{AREA}_D': 'DEKAD',
        'L3_I_{AREA}_D': 'DEKAD',
        'L3_NPP_{AREA}_D': 'DEKAD',
        'L3_QUAL_NDVI_{AREA}_D': 'DEKAD',
        'L3_QUAL_LST_{AREA}_D': 'DEKAD',
        'L3_LCC_{AREA}_A': 'YEAR'}

    versions = {'1': 'WAPOR',
                '2': 'WAPOR_2'}


class LEVEL3:
    """
    This class contains the information of the areas in LEVEL 3
    """

    Area = {'AWA': 'Awash, Ethiopia',
            'BKA': 'Bekaa, Lebanon',
            'KOG': 'Koga, Ethiopia',
            'ODN': 'Office du Niger, Mali',
            'ZAN': 'Zankalon, Egypt'}

    Projection = {'AWA': 32637,
                  'BKA': 32636,
                  'KOG': 32637,
                  'ODN': 32630,
                  'ZAN': 32636}
