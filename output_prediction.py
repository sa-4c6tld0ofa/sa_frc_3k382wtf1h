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


############################################################################################
# There is 6 steps to follow in this file.
# 1. Import the model
# 2. Reference model score value column
# 3. Reference model score value column to retrieve from query
# 4. Add model score column to the model score list
# 5. Reference model target price with index
# 6. Reference model function to be called
############################################################################################









############################################################################################
# (1) Import model
############################################################################################
from model_arima_7d import *
from model_ma10 import *
from model_ma20 import *
from model_ma30 import *
from model_ma40 import *
from model_ma50 import *
from model_ma10ctt import *
from model_arima_7dr import *
from model_trend_3d import *
from model_trend_5d import *
from model_trend_7d import *
from model_price_action_20d import *
from model_price_action_10d import *
from model_price_action_10dr import *

pdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.abspath(pdir) )
from settings import *
sett = sa_path()

sys.path.append(os.path.abspath( sett.get_path_pwd() ))
from sa_access import *
access_obj = sa_db_access()

sys.path.append(os.path.abspath(sett.get_path_core() ))
from ta_instr_sum import *
from ta_gen_chart_data import *
from get_frc_pnl import *
from get_trades import *

db_usr = access_obj.username(); db_pwd = access_obj.password(); db_name = access_obj.db_name(); db_srv = access_obj.db_server()

def clear_chart_data(s):
    try:
        import pymysql.cursors
        connection = pymysql.connect(host=db_srv,
                                     user=db_usr,
                                     password=db_pwd,
                                     db=db_name,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        cr = connection.cursor(pymysql.cursors.SSCursor)
        sql = "DELETE FROM chart_data WHERE symbol = '"+ str(s) +"'"
        cr.execute(sql)
        connection.commit()
        
        cr.close()
        connection.close()
    except Exception as e: print(e)

def clear_trades(s):
    try:
        import pymysql.cursors
        connection = pymysql.connect(host=db_srv,
                                     user=db_usr,
                                     password=db_pwd,
                                     db=db_name,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        cr = connection.cursor(pymysql.cursors.SSCursor)
        sql = "DELETE FROM trades WHERE symbol = '"+ str(s) +"'"
        cr.execute(sql)
        connection.commit()
        
        cr.close()
        connection.close()

    except Exception as e: print(e)

def get_instr_decimal_places(s):
    r = 5
    try:
        import pymysql.cursors
        connection = pymysql.connect(host=db_srv,
                                     user=db_usr,
                                     password=db_pwd,
                                     db=db_name,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        cr = connection.cursor(pymysql.cursors.SSCursor)
        sql = "SELECT decimal_places FROM instruments WHERE symbol = '"+ str(s) +"'"
        cr.execute(sql)
        rs = cr.fetchall()
        for row in rs: r = row[0]
        cr.close()
        connection.close()
        

    except Exception as e: print("get_instr_decimal_places() " + str(e) )
    return r

def compute_target_price(uid,force_full_update):
    try:
        nd = 370
        dn = datetime.datetime.now() - timedelta(days=10)
        dn = dn.strftime("%Y%m%d")


        #name column of each model in the same order...
        selected_model_column = 'price_instruments_data.arima_7d_tp'
        ############################################################################################
        # (2) Add model column here define variables
        ############################################################################################
        #Add column in the string below:
        column_of_each_model = 'instruments.score_arima_7d, instruments.score_ma10, instruments.score_ma20, instruments.score_ma30, instruments.score_ma40, instruments.score_ma50, '+\
        'instruments.score_ma10ctt, instruments.score_arima_7dr, instruments.score_3dtrend, instruments.score_5dtrend, instruments.score_7dtrend, '+\
        'instruments.score_price_action_20d, instruments.score_price_action_10d, instruments.score_price_action_10dr'
        #-------------------------------------------------------------------------------------------
        #Add a variable named based on the model column:
        score_arima_7d = 0
        score_ma10 = 0
        score_ma20 = 0
        score_ma30 = 0
        score_ma40 = 0
        score_ma50 = 0
        score_ma10ctt = 0
        score_arima_7dr = 0
        score_3dtrend = 0
        score_5dtrend = 0
        score_7dtrend = 0
        score_price_action_20d = 0
        score_price_action_10d = 0
        score_price_action_10dr = 0
        #------------------------------------------------------------------------------------------

        import pymysql.cursors
        connection = pymysql.connect(host=db_srv,
                                     user=db_usr,
                                     password=db_pwd,
                                     db=db_name,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        cr = connection.cursor(pymysql.cursors.SSCursor)
        sql = "SELECT symbol from symbol_list WHERE uid = " + str(uid)
        cr.execute(sql)
        rs = cr.fetchall()
        for row in rs: symbol = row[0]

        sql = "SELECT asset_class, pip, " + str(column_of_each_model) + " FROM instruments JOIN symbol_list ON symbol_list.symbol = instruments.symbol WHERE symbol_list.uid = " + str(uid)
        cr.execute(sql)
        rs = cr.fetchall()
        for row in rs:
            asset_class = row[0]
            pip = row[1]
            ##########################################################################################
            # (3) Add model column as per column_of_each_model variable and append row index
            ##########################################################################################
            score_arima_7d = row[2]
            score_ma10 = row[3]
            score_ma20 = row[4]
            score_ma30 = row[5]
            score_ma40 = row[6]
            score_ma50 = row[7]
            score_ma10ctt = row[8]
            score_arima_7dr = row[9]
            score_3dtrend = row[10]
            score_5dtrend = row[11]
            score_7dtrend = row[12]
            score_price_action_20d = row[13]
            score_price_action_10d = row[14]
            score_price_action_10dr = row[15]
            #----------------------------------------------------------------------------------------
        

        #############################################################################################
        # (4) Add model to the model_list
        #############################################################################################
        model_list = (score_arima_7d, score_ma10, score_ma20, score_ma30, score_ma40, score_ma50, score_ma10ctt, score_arima_7dr, score_3dtrend, score_5dtrend, score_7dtrend, score_price_action_20d, score_price_action_10d, score_price_action_10dr)
        #--------------------------------------------------------------------------------------------
        selected_model_id = model_list.index( max(model_list) )

        ##############################################################################################
        # (5) Add model column in here for index and increment id.
        ##############################################################################################
        if selected_model_id == 0: selected_model_column = 'price_instruments_data.arima_7d_tp'
        if selected_model_id == 1: selected_model_column = 'price_instruments_data.ma10_tp'
        if selected_model_id == 2: selected_model_column = 'price_instruments_data.ma20_tp'
        if selected_model_id == 3: selected_model_column = 'price_instruments_data.ma30_tp'
        if selected_model_id == 4: selected_model_column = 'price_instruments_data.ma40_tp'
        if selected_model_id == 5: selected_model_column = 'price_instruments_data.ma50_tp'
        if selected_model_id == 6: selected_model_column = 'price_instruments_data.ma10ctt_tp'
        if selected_model_id == 7: selected_model_column = 'price_instruments_data.arima_7dr_tp'
        if selected_model_id == 8: selected_model_column = 'price_instruments_data.3dtrend_tp'
        if selected_model_id == 9: selected_model_column = 'price_instruments_data.5dtrend_tp'
        if selected_model_id == 10: selected_model_column = 'price_instruments_data.7dtrend_tp'
        if selected_model_id == 11: selected_model_column = 'price_instruments_data.price_action_20d_tp'
        if selected_model_id == 12: selected_model_column = 'price_instruments_data.price_action_10d_tp'
        if selected_model_id == 13: selected_model_column = 'price_instruments_data.price_action_10dr_tp'
        #---------------------------------------------------------------------------------------------

        sql = "SELECT id FROM price_instruments_data WHERE symbol ='"+ symbol +"' ORDER BY date DESC LIMIT 1"
        cr.execute(sql)
        rs = cr.fetchall()
        price_id = 0
        for row in rs: price_id = row[0]

        if force_full_update:
            sql = "UPDATE price_instruments_data SET price_instruments_data.target_price = CAST("+ selected_model_column +" AS DECIMAL(20,"+ str( get_instr_decimal_places(symbol) ) +")) WHERE price_instruments_data.symbol = '"+ symbol + "'"
            print('### ::: ' + sql)
            cr.execute(sql); connection.commit()
            clear_chart_data(symbol)
            clear_trades(symbol)
            get_forecast_pnl(symbol,uid,nd,force_full_update)

            get_trades(symbol,uid,nd,False)
            gen_chart(symbol,uid)
            get_instr_sum(symbol,uid,asset_class,dn,pip)
        else:
            sql = "UPDATE price_instruments_data SET price_instruments_data.target_price = CAST("+ selected_model_column +" AS DECIMAL(20,"+ str( get_instr_decimal_places(symbol) ) +")) WHERE price_instruments_data.id = " + str(price_id)
            cr.execute(sql); connection.commit()


        cr.close()
        connection.close()
        

    except Exception as e: print("compute_target_price() " + str(e) )


def set_all_prediction_model_target_price_n_score(uid,force_full_update):
    try:
        #Set the score and return model_tp
        ##############################################################################################
        # (6) Call function for each model.
        ##############################################################################################
        set_model_arima_7d(uid,force_full_update)
        set_model_ma10(uid,force_full_update)
        set_model_ma20(uid,force_full_update)
        set_model_ma30(uid,force_full_update)
        set_model_ma40(uid,force_full_update)
        set_model_ma50(uid,force_full_update)
        set_model_ma10ctt(uid,force_full_update)
        set_model_arima_7dr(uid,force_full_update)
        set_model_3d_trend(uid,force_full_update)
        set_model_5d_trend(uid,force_full_update)
        set_model_7d_trend(uid,force_full_update)
        set_model_price_action_20d(uid,force_full_update)
        set_model_price_action_10d(uid,force_full_update)
        set_model_price_action_10dr(uid,force_full_update)
        #--------------------------------------------------------------------------------------------

        #target_price get the value of highest score model
        compute_target_price(uid,force_full_update)

    except Exception as e: print("set_all_prediction_model_target_price_n_score() " + str(e) )


def output_prediction(force_full_update,uid):
    try:

        import pymysql.cursors
        connection = pymysql.connect(host=db_srv,
                                     user=db_usr,
                                     password=db_pwd,
                                     db=db_name,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        cr = connection.cursor(pymysql.cursors.SSCursor)

        if uid == 0:
            sql = "SELECT uid FROM symbol_list WHERE symbol NOT LIKE '%"+ get_portf_suffix() +"%' AND disabled=0 ORDER BY symbol"
        else:
            sql = "SELECT uid FROM symbol_list WHERE uid = " + str(uid)

        cr.execute(sql)
        rs = cr.fetchall()
        for row in rs:
            uid = row[0]
            set_all_prediction_model_target_price_n_score(uid,force_full_update)
            

        cr.close()
        connection.close()

    except Exception as e: print("output_prediction() " + str(e) )
