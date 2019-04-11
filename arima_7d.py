# Copyright (c) 2018-present, Taatu Ltd.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
import sys
import os
import datetime
import time
from datetime import timedelta
import csv
from pathlib import Path

pdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.abspath(pdir) )
from settings import *
sett = sa_path()

sys.path.append(os.path.abspath( sett.get_path_pwd() ))
from sa_access import *
access_obj = sa_db_access()

sys.path.append(os.path.abspath(sett.get_path_core() ))
from get_instr_perf_summ import *
from sa_numeric import *

db_usr = access_obj.username(); db_pwd = access_obj.password(); db_name = access_obj.db_name(); db_srv = access_obj.db_server()


def set_model_arima_7d(uid):
    try:


        forc_src = sett.get_path_src()
        ext = ".csv"
        file_str = forc_src+str(uid)+'f.csv'
        filepath = Path(file_str)
        if filepath.exists():
            with open(file_str) as csvfile:
                readCSV = csv.reader(csvfile, delimiter=',')
                i = 1
                for row in readCSV:
                    if (i == 8): point_forecast = row[1]
                    i +=1


        import pymysql.cursors
        connection = pymysql.connect(host=db_srv,
                                     user=db_usr,
                                     password=db_pwd,
                                     db=db_name,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        #sql = "SELECT symbol_list.symbol, price_instruments_data.date, price_instruments_data.price_close, price_instruments_data.target_price, price_instruments_data.pnl, price_instruments_data.pnl_long, price_instruments_data.pnl_short FROM price_instruments_data JOIN symbol_list ON symbol_list.symbol = price_instruments_data.symbol WHERE symbol_list.uid = 1 AND date >= DATE_SUB(20190410, INTERVAL 7 DAY) ORDER BY date DESC"
        cr = connection.cursor(pymysql.cursors.SSCursor)
        sql = "SELECT price_instruments_data.date FROM price_instruments_data JOIN symbol_list ON symbol_list.symbol = price_instruments_data.symbol WHERE symbol_list.uid = "+ str(uid) +" ORDER BY date DESC LIMIT 1"
        cr.execute(sql)
        rs = cr.fetchall()
        for row in rs: last_date = row[0].strftime('%Y%m%d')

        sql = "SELECT symbol_list.symbol, price_instruments_data.date, price_instruments_data.price_close, price_instruments_data.target_price, price_instruments_data.pnl, price_instruments_data.pnl_long, price_instruments_data.pnl_short FROM price_instruments_data JOIN symbol_list ON symbol_list.symbol = price_instruments_data.symbol WHERE symbol_list.uid = 1 AND date >= DATE_SUB(20190410, INTERVAL 7 DAY) ORDER BY date DESC"



        cr.close()
        connection.close()

    except Exception as e: print(e)
