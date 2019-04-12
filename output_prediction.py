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
from model_arima_7d import *
from model_ma10 import *

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



def get_forecast_pnl(s,uid,nd):


    import pymysql.cursors
    connection = pymysql.connect(host=db_srv,
                                 user=db_usr,
                                 password=db_pwd,
                                 db=db_name,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    td = datetime.datetime.now()
    i = 0
    wdb = 7

    while i <= nd:

        j = nd - i
        k = (nd - i) + wdb
        pd = datetime.datetime.now() - timedelta(days=k)
        sd = datetime.datetime.now() - timedelta(days=j)
        pd_str = pd.strftime("%Y%m%d")
        sd_str = sd.strftime("%Y%m%d")

        signal = ''
        p_price_close = 0
        p_target_price = 0
        pnl = 0
        pnl_long = 999
        pnl_short = 999

        print(s +": "+ sd_str +": "+ os.path.basename(__file__) )

        cr = connection.cursor(pymysql.cursors.SSCursor)
        sql = "SELECT price_close, target_price FROM price_instruments_data WHERE symbol ='"+s+"' AND date = "+ pd_str
        cr.execute(sql)
        rs = cr.fetchall()
        for row in rs:
            p_price_close = row[0]
            p_target_price = row[1]
        cr.close()

        if (p_price_close > 0 and p_target_price > 0 ):

            if p_price_close < p_target_price:
                signal = "b"
            else:
                signal = "s"

            id = 0
            s_pnl = 0
            s_pnl_long = 0
            s_pnl_short = 0
            s_price_close = 0
            cr = connection.cursor(pymysql.cursors.SSCursor)
            sql = "SELECT id, price_close, pnl, pnl_long, pnl_short FROM price_instruments_data WHERE symbol ='"+s+"' AND date = "+ sd_str
            print(sql)
            cr.execute(sql)
            rs = cr.fetchall()
            for row in rs:
                id = row[0]
                s_price_close = row[1]
                s_pnl = row[2]
                s_pnl_long = row[3]
                s_pnl_short = row[4]
            cr.close()

            if s_pnl == 0 or s_pnl_long == 0 or s_pnl_short == 0:
                if signal == "b":
                    pnl = s_price_close - p_price_close
                    pnl_long = pnl
                if signal == "s":
                    pnl = p_price_close - s_price_close
                    pnl_short = pnl
                cr = connection.cursor(pymysql.cursors.SSCursor)
                sql = "UPDATE price_instruments_data SET pnl = " + str(pnl) + ", pnl_long = " + str(pnl_long) + ", pnl_short = " + str(pnl_short) + " WHERE id = " + str(id)
                print(sql)
                try:
                    cr.execute(sql)
                    connection.commit()
                    cr.close()
                except Exception as e: print(e)
        i += 1
    connection.close()

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

    except Exception as e: print(e)
    return r

def compute_target_price(uid,force_full_update):
    try:
        nd = 200
        #name column of each model in the same order...
        selected_model_column = 'price_instruments_data.arima7d_tp'
        ############################################################################################
        # (1) Add model column here define variables
        ############################################################################################
        column_of_each_model = 'instruments.score_arima7d, instruments.score_ma10'
        score_arima7d = 0 #[0]
        score_ma10 = 0 #[1]
        ############################################################################################

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

        sql = "SELECT " + str(column_of_each_model) + " FROM instruments JOIN symbol_list ON symbol_list.symbol = instruments.symbol WHERE symbol_list.uid = " + str(uid)
        cr.execute(sql)
        rs = cr.fetchall()
        for row in rs:
            ##########################################################################################
            # (2) Add model column as per column_of_each_model variable
            ##########################################################################################
            score_arima7d = row[0]
            score_ma10 = row[1]
            ##########################################################################################

        model_list = (score_arima7d, score_ma10)
        selected_model_id = model_list.index( max(model_list) )

        ##############################################################################################
        # (3) Add model column in here for index
        ##############################################################################################
        if selected_model_id == 0: selected_model_column = 'price_instruments_data.arima7d_tp'
        if selected_model_id == 1: selected_model_column = 'price_instruments_data.ma10_tp'
        ##############################################################################################

        sql = "SELECT id FROM price_instruments_data WHERE symbol ='"+ symbol +"' ORDER BY date DESC LIMIT 1"
        cr.execute(sql)
        rs = cr.fetchall()
        price_id = 0
        for row in rs: price_id = row[0]

        if force_full_update:
            sql = "UPDATE price_instruments_data SET price_instruments_data.target_price = FORMAT(" + selected_model_column + ","+ str( get_instr_decimal_places(symbol) ) +") WHERE price_instruments_data.symbol = '"+ symbol
            cr.execute(sql); connection.commit()
            get_forecast_pnl(symbol,uid,nd)
        else:
            sql = "UPDATE price_instruments_data SET price_instruments_data.target_price = FORMAT(" + selected_model_column + ","+ str( get_instr_decimal_places(symbol) ) +") WHERE price_instruments_data.id = " + str(price_id)
            cr.execute(sql); connection.commit()


        cr.close()
        connection.close()


    except Exception as e: print(e)


def set_all_prediction_model_target_price_n_score(uid,force_full_update):
    try:
        #Set the score and return model_tp
        arima_7d_tp = set_model_arima_7d(uid,force_full_update)
        ma10_tp = set_model_ma10(uid,force_full_update)
        #target_price get the value of highest score model
        compute_target_price(uid,force_full_update)

    except Exception as e: print(e)


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
            sql = "SELECT uid FROM symbol_list WHERE symbol NOT LIKE '%"+ get_portf_suffix() +"%' ORDER BY symbol"
        else:
            sql = "SELECT uid FROM symbol_list WHERE uid = " + str(uid)

        cr.execute(sql)
        rs = cr.fetchall()
        for row in rs:
            uid = row[0]
            set_all_prediction_model_target_price_n_score(uid,force_full_update)

        cr.close()
        connection.close()

    except Exception as e: print(e)
