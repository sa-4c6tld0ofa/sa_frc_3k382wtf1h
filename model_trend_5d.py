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
from model_trend_calc import *

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

######################################################################################################################################
# Notes to add additional model to the system:
# a. Add a column in table "price_instruments_data" containing value of indicator
# b. Develop indicator. Add this py file to sa_data_collection in folder named "core", available for reference in ta_main_update_data.py
# c. In sa_data_collection repository, reference the new indicator in file ta_main_update_data.py in function get_update_instr_data()
# 1. Add a column in table "instruments" named score_modelXX
# 2. Add a column in table "price_instruments_data" named modelXX_tp
# 3. Follow instruction in the following py file as well as for output_prediction.py
######################################################################################################################################

def get_model_5d_trend(uid,date_str):
    ################################################
    # (1) Logic according to model
    # Logic as per specific to the model
    ################################################
    r = 0
    try:
        import pymysql.cursors
        connection = pymysql.connect(host=db_srv,
                                     user=db_usr,
                                     password=db_pwd,
                                     db=db_name,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        target_price = 0
        price_close = 0
        ta_5dtrend = ''

        cr = connection.cursor(pymysql.cursors.SSCursor)
        sql = "SELECT instruments.symbol, instruments.stdev_st FROM instruments JOIN symbol_list ON symbol_list.symbol = instruments.symbol WHERE symbol_list.uid = " + str(uid)
        cr.execute(sql)
        rs = cr.fetchall()
        for row in rs:
            symbol = row[0]
            stdev_st = row[1]

        sql = "SELECT price_close, 5dtrend FROM price_instruments_data WHERE symbol = '"+ str(symbol) +"' AND date = " + str(date_str)
        cr.execute(sql)
        rs = cr.fetchall()
        for row in rs:
            price_close = row[0]
            ta_5dtrend = row[1]

        if ta_5dtrend == 'u':
            target_price = float(price_close) + float(stdev_st)
        if ta_5dtrend == 'd':
            target_price = float(price_close) - float(stdev_st)
        if ta_5dtrend == '':
            target_price = price_close
        r = target_price

        cr.close()
        connection.close()
    #---------------------------------------------------------------------------

    except Exception as e: print("get_model_price_5d_trend() " + str(e) )
    return r

########################################################################
# (2) Set the name of the model function
########################################################################
def set_model_5d_trend(uid,force_full_update):
    #-------------------------------------------------------------------
    r = 0
    try:
        ########################################################################
        # (2.1) Define names of column in use by the model
        ########################################################################
        model_tp_column = 'price_instruments_data.5dtrend_tp'
        model_score_column = 'instruments.score_5dtrend'
        #-----------------------------------------------------------------------

        day_to_process = 400
        score = 0

        import pymysql.cursors
        connection = pymysql.connect(host=db_srv,
                                     user=db_usr,
                                     password=db_pwd,
                                     db=db_name,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        if force_full_update:
            sql_selection = "SELECT price_instruments_data.symbol, price_instruments_data.date, price_instruments_data.price_close, " + str(model_tp_column) + " FROM price_instruments_data JOIN symbol_list ON symbol_list.symbol = price_instruments_data.symbol WHERE symbol_list.uid = "+ str(uid) +" ORDER BY date DESC LIMIT "+ str(day_to_process)
        else:
            sql_selection = "SELECT price_instruments_data.symbol, price_instruments_data.date, price_instruments_data.price_close, " + str(model_tp_column) + " FROM price_instruments_data JOIN symbol_list ON symbol_list.symbol = price_instruments_data.symbol WHERE symbol_list.uid = "+ str(uid) +" AND price_instruments_data.is_ta_calc = 0 ORDER BY date DESC"

        cr = connection.cursor(pymysql.cursors.SSCursor)
        sql = sql_selection
        cr.execute(sql)
        rs = cr.fetchall()
        for row in rs:
            symbol = row[0]
            last_date = row[1].strftime('%Y%m%d')
            last_price = row[2]
            model_tp = row[3]

            cr_c = connection.cursor(pymysql.cursors.SSCursor)
            sql_c = "SELECT " + str(model_tp_column) + ", price_instruments_data.price_close FROM price_instruments_data JOIN symbol_list ON symbol_list.symbol = price_instruments_data.symbol WHERE symbol_list.uid = "+ str(uid) +" AND date = DATE_SUB("+ str(last_date) +", INTERVAL 7 DAY)"
            cr_c.execute(sql_c)
            rs_c = cr_c.fetchall()
            model_prediction_tp = 0
            previous_price = 0
            for row in rs_c:
                model_prediction_tp = row[0]
                previous_price = row[1]


            if model_prediction_tp != 0 and previous_price != 0:
                type_of_trade = ''
                if previous_price <= model_prediction_tp: type_of_trade = 'b'
                if previous_price > model_prediction_tp: type_of_trade = 's'
                if (previous_price >= last_price) and (type_of_trade == 'b'):
                    if score > 0: score = score - 0.01
                if (previous_price >= last_price) and (type_of_trade == 's'): score = score + 0.01
                if (previous_price < last_price) and (type_of_trade == 'b'): score = score + 0.01
                if (previous_price < last_price) and (type_of_trade == 's'):
                    if score > 0: score = score - 0.01
                print("### score calc "+ str(model_score_column) +": current score = " + str(score) )

            if model_tp == 0:
                trend = trend_data(symbol, last_date)
                trend_3d_value = trend.get_3d_trend(); trend_5d_value = trend.get_5d_trend(); trend_7d_value = trend.get_7d_trend()
                cr_u = connection.cursor(pymysql.cursors.SSCursor)
                sql_u = "UPDATE price_instruments_data SET 3dtrend = '"+ str(trend_3d_value) +"', 5dtrend = '"+ str(trend_5d_value) +"', 7dtrend = '"+ str(trend_7d_value) +"'  WHERE symbol = '"+ str(symbol) +"' AND date = " + str(last_date)
                cr_u.execute(sql_u)
                connection.commit()

                ########################################################################
                # (3) Define function that calc the model target price
                ########################################################################
                last_model_tp = get_model_5d_trend(uid,last_date)
                #-----------------------------------------------------------------------
                sql_u = "UPDATE price_instruments_data SET " + str(model_tp_column) + " = " + str( last_model_tp ) + " WHERE symbol = '"+ str(symbol) +"' AND date = " + str(last_date)
                cr_u.execute(sql_u)
                connection.commit()
                r = last_model_tp
                cr_u.close()

        model_score = 0
        if force_full_update == False:
            sql = "SELECT "+ str(model_score_column) +" FROM instruments WHERE symbol = '"+ str(symbol) +"'"
            cr.execute(sql)
            rs = cr.fetchall()
            for row in rs: model_score = row[0]
        print("### Total score calc "+ str(model_score_column) +": " + str(model_score) + " + " + str(score) )
        model_score = round(model_score + score,2)
        print("### Total score "+ str(model_score_column) +": " + str(model_score) )

        sql = "UPDATE instruments SET " + str(model_score_column) + " = " + str(model_score) + " WHERE symbol = '"+ str(symbol) +"'"
        cr.execute(sql)
        connection.commit()

        cr_c.close()
        cr.close()
        connection.close()

    except Exception as e: print("set_model_5d_trend() " + str(e) )
    return r
