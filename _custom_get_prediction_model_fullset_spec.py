# Copyright (c) 2018-present, Taatu Ltd.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
import sys
import os
from pathlib import Path
from output_prediction import *

pdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.abspath(pdir) )
from settings import *
sett = sa_path()

sys.path.append(os.path.abspath( sett.get_path_pwd() ))
from sa_access import *
access_obj = sa_db_access()


def recalc_prediction_model(symbol):
    try:

        import pymysql.cursors
        connection = pymysql.connect(host=db_srv,
                                     user=db_usr,
                                     password=db_pwd,
                                     db=db_name,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        uid = 0
        cr = connection.cursor(pymysql.cursors.SSCursor)
        sql = "SELECT uid FROM symbol_list WHERE symbol='"+ str(symbol) +"'"
        cr.execute(sql)
        rs = cr.fetchall()
        for row in rs: uid = row[0]

        set_of_data_uid = uid
        calc_all_data = True
        order = 'desc'
        output_prediction(calc_all_data,set_of_data_uid,order)

    except Exception as e: print(e)

print("###############################################################################")
print("Recalculate prediction model: all histdata for specific symbol")
print("--------------------------------------------------------------")
print("IMPORTANT: BACKUP THE DATABASE PRIOR TO RUN THIS SCRIPT")
print("recalc_prediction_model(symbol)")
print(" ")
print("provide the following parameters:")
print("(1) symbol")
print("--------------------------------------")
print("Affected tables/column:")
print("-----------------------")
print("1. price_instruments_data.*")
print("###############################################################################")

recalc_prediction_model('EURHUF')
recalc_prediction_model('GBPHUF')
recalc_prediction_model('CHFHUF')
recalc_prediction_model('USDPLN')
recalc_prediction_model('NASDAQ:AAXJ')
recalc_prediction_model('NASDAQ:ACWI')
recalc_prediction_model('NYSEARCA:AGG')
recalc_prediction_model('NYSEARCA:AMLP')
recalc_prediction_model('NYSEARCA:AOA')
recalc_prediction_model('NYSEARCA:AOK')
recalc_prediction_model('NYSEARCA:AOR')
recalc_prediction_model('NYSEARCA:ASHR')
recalc_prediction_model('NYSEARCA:BIL')
recalc_prediction_model('NYSEARCA:BKLN')
recalc_prediction_model('NASDAQ:BND')
recalc_prediction_model('NYSEARCA:BOND')
recalc_prediction_model('NYSEARCA:BSV')
recalc_prediction_model('NYSEARCA:CORN')
recalc_prediction_model('NYSEARCA:DBA')
recalc_prediction_model('NYSEARCA:DGRO')
recalc_prediction_model('NYSEARCA:DIA')
recalc_prediction_model('NYSEARCA:EEM')
recalc_prediction_model('NYSEARCA:EFA')
recalc_prediction_model('NASDAQ:EMB')
recalc_prediction_model('NYSEARCA:EWG')
recalc_prediction_model('NYSEARCA:EWH')
recalc_prediction_model('NYSEARCA:EWJ')
recalc_prediction_model('NYSEARCA:EWT')
recalc_prediction_model('NYSEARCA:EWW')
recalc_prediction_model('NYSEARCA:EWY')
recalc_prediction_model('NYSEARCA:EWZ')
