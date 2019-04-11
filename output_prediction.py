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

def get_all_prediction_model_value(uid):
    try:
        forc_src = sett.get_path_src()
        ext = ".csv"
        file_str = forc_src+str(uid)+'_arima_7d.csv'
        filepath = Path(file_str)
        if filepath.exists():
            with open(file_str) as csvfile:
                readCSV = csv.reader(csvfile, delimiter=',')
                for row in readCSV:
                    point_number = row[0]
                    point_forecast = row[1]
                    low80 = row[2]
                    high80 = row[3]
                    low95 = row[4]
                    high95 = row[5]

                    with open( forc_src + str(uid) + 'f.csv', 'a') as s_csvfile:
                        s_csvfile_writer = csv.writer(s_csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                        s_csvfile_writer.writerow([str(point_number), str(point_forecast), str(low80), str(high80), str(low95), str(high95)])

    except Exception as e: print(e)

def output_prediction():
    try:

        import pymysql.cursors
        connection = pymysql.connect(host=db_srv,
                                     user=db_usr,
                                     password=db_pwd,
                                     db=db_name,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        cr = connection.cursor(pymysql.cursors.SSCursor)
        sql = "SELECT uid FROM symbol_list WHERE symbol NOT LIKE '%"+ get_portf_suffix() +"%' ORDER BY symbol"
        cr.execute(sql)
        rs = cr.fetchall()
        for row in rs:
            uid = row[0]
            get_all_prediction_model_value(uid)

        cr.close()
        connection.close()

    except Exception as e: print(e)


output_prediction()
