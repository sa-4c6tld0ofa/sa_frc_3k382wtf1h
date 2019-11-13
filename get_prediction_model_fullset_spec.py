""" Recalculate prediction model: all historical data for specific instrument """
import sys
import os
from pathlib import Path
from output_prediction import *

pdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.abspath(pdir) )
from settings import *
sett = SmartAlphaPath()

sys.path.append(os.path.abspath( sett.get_path_pwd() ))
from sa_access import *
access_obj = sa_db_access()


def recalc_prediction_model(symbol):
    """
    Recalculate prediction model for a given instrument
    Args:
        String: Symbol
    Returns:
        None
    """
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
