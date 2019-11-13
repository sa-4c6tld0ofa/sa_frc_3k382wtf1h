""" Model trend calculation """
import sys
import os
import csv
from pathlib import Path
import gc
import pymysql.cursors
PDIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.abspath(PDIR))
from settings import SmartAlphaPath, debug
SETT = SmartAlphaPath()
sys.path.append(os.path.abspath(SETT.get_path_pwd()))
from sa_access import sa_db_access
ACCESS_OBJ = sa_db_access()
DB_USR = ACCESS_OBJ.username()
DB_PWD = ACCESS_OBJ.password()
DB_NAME = ACCESS_OBJ.db_name()
DB_SRV = ACCESS_OBJ.db_server()

class trend_data:
    """
    price trend data calculation
    Args:
        String: Instrument symbol
        String: Date in string format YYYYMMDD
    """
    ta_3d_trend = ''
    ta_5d_trend = ''
    ta_7d_trend = ''

    def __init__(self, symbol, datestr):
        """ """
        ta_3d_count_up = 0 ; ta_3d_count_down = 0
        ta_5d_count_up = 0 ; ta_5d_count_down = 0
        ta_7d_count_up = 0 ; ta_7d_count_down = 0

        connection = pymysql.connect(host=DB_SRV,
                                     user=DB_USR,
                                     password=DB_PWD,
                                     db=DB_NAME,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        cr = connection.cursor(pymysql.cursors.SSCursor)
        sql = "SELECT price_close FROM price_instruments_data "+\
        "WHERE symbol='"+ str(symbol) +"' AND date <= "+ str(datestr) + " AND date >= DATE_SUB("+ str(datestr) +",INTERVAL 7 DAY) ORDER BY date"
        cr.execute(sql)
        rs = cr.fetchall()
        i = 0
        previous_close = 0
        for row in rs:
            price_close = row[0]

            if i > 0:
                if price_close > previous_close: ta_7d_count_up += 1
                if price_close < previous_close: ta_7d_count_down += 1
                if i >= 2:
                    if price_close > previous_close: ta_5d_count_up += 1
                    if price_close < previous_close: ta_5d_count_down += 1
                if i >= 4:
                    if price_close > previous_close: ta_3d_count_up += 1
                    if price_close < previous_close: ta_3d_count_down += 1

            previous_close = price_close
            i += 1

        if ta_7d_count_up >= ta_7d_count_down:
            trend_data.ta_7d_trend = 'u'
        else:
            trend_data.ta_7d_trend = 'd'

        if ta_5d_count_up >= ta_5d_count_down:
            trend_data.ta_5d_trend = 'u'
        else:
            trend_data.ta_5d_trend = 'd'

        if ta_3d_count_up >= ta_3d_count_down:
            trend_data.ta_3d_trend = 'u'
        else:
            trend_data.ta_3d_trend = 'd'

        cr.close()
        connection.close()
        gc.collect()

    def get_3d_trend(self):
        """ """
        return trend_data.ta_3d_trend

    def get_5d_trend(self):
        """ """
        return trend_data.ta_5d_trend

    def get_7d_trend(self):
        """ """
        return trend_data.ta_7d_trend
