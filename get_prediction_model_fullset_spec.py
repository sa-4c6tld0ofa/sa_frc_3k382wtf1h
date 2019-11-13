""" Recalculate prediction model: all historical data for specific instrument """
import sys
import os
import pymysql.cursors
from output_prediction import output_prediction

PDIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.abspath(PDIR))
from settings import SmartAlphaPath
SETT = SmartAlphaPath()
sys.path.append(os.path.abspath(SETT.get_path_pwd()))
from sa_access import sa_db_access
ACCESS_OBJ = sa_db_access()
DB_USR = ACCESS_OBJ.username()
DB_PWD = ACCESS_OBJ.password()
DB_NAME = ACCESS_OBJ.db_name()
DB_SRV = ACCESS_OBJ.db_server()

def recalc_prediction_model(symbol):
    """
    Recalculate prediction model for a given instrument
    Args:
        String: Symbol
    Returns:
        None
    """
    connection = pymysql.connect(host=DB_SRV,
                                 user=DB_USR,
                                 password=DB_PWD,
                                 db=DB_NAME,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    uid = 0
    cursor = connection.cursor(pymysql.cursors.SSCursor)
    sql = "SELECT uid FROM symbol_list WHERE symbol='"+ str(symbol) +"'"
    cursor.execute(sql)
    res = cursor.fetchall()
    for row in res:
        uid = row[0]
    cursor.close()
    connection.close()

    set_of_data_uid = uid
    calc_all_data = True
    order = 'desc'
    output_prediction(calc_all_data, set_of_data_uid, order)


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
