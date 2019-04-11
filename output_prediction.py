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
from arima_7d import *

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

def set_all_prediction_model_target_price_n_score(uid):
    try:
        pass
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
            set_all_prediction_model_target_price_n_score(uid)

        cr.close()
        connection.close()

    except Exception as e: print(e)


output_prediction()
