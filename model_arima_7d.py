""" Model Arima 7d """
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

################################################################################
# Notes to add additional model to the system:
# Add a column in table "instruments" named score_modelXX
# Add a column in table "price_instruments_data" named modelXX_tp
# Follow instruction in the following py file as well as for output_prediction.py
################################################################################

def get_model_price_arima_7d(uid):
    """
    Get model price prediction
    Args:
        Integer: Instrument unique id
    Returns:
        Double: price prediction
    """
    ################################################
    # (1) Logic according to model
    # Logic as per specific to the model
    ################################################
    ret = 0
    forc_src = SETT.get_path_src()
    file_str = forc_src+str(uid)+'f.csv'
    filepath = Path(file_str)
    if filepath.exists():
        with open(file_str) as csvfile:
            csv_file = csv.reader(csvfile, delimiter=',')
            i = 1
            for row in csv_file:
                if i == 8:
                    point_forecast = row[1]
                i += 1
    ret = point_forecast
#---------------------------------------------------------------------------
    return ret

########################################################################
# (2) Set the name of the model function
########################################################################
def set_model_arima_7d(uid, force_full_update, connection):
    """ xxx """
    ret = 0
    ########################################################################
    # (2.1) Define names of column in use by the model
    ########################################################################
    model_tp_column = 'price_instruments_data.arima_7d_tp'
    model_score_column = 'instruments.score_arima_7d'
    #-----------------------------------------------------------------------

    day_to_process = 370
    score = 0

    if force_full_update:
        sql_selection = "SELECT price_instruments_data.symbol, "+\
        "price_instruments_data.date, price_instruments_data.price_close, " +\
        str(model_tp_column) + " FROM price_instruments_data JOIN "+\
        "symbol_list ON symbol_list.symbol = price_instruments_data.symbol "+\
        "WHERE symbol_list.uid = "+ str(uid) +" ORDER BY date DESC LIMIT "+\
        str(day_to_process)
    else:
        sql_selection = "SELECT price_instruments_data.symbol, "+\
        "price_instruments_data.date, price_instruments_data.price_close, " +\
        str(model_tp_column) + " FROM price_instruments_data JOIN "+\
        "symbol_list ON symbol_list.symbol = price_instruments_data.symbol "+\
        "WHERE symbol_list.uid = "+ str(uid) +\
        " AND price_instruments_data.is_ta_calc = 0 ORDER BY date DESC LIMIT "+ str(day_to_process)

    cursor = connection.cursor(pymysql.cursors.SSCursor)
    sql = sql_selection
    cursor.execute(sql)
    res = cursor.fetchall()
    symbol = ''
    for row in res:
        symbol = row[0]
        last_date = row[1].strftime('%Y%m%d')
        last_price = row[2]
        model_tp = row[3]

        cr_c = connection.cursor(pymysql.cursors.SSCursor)
        sql_c = "SELECT " + str(model_tp_column) +\
        ", price_instruments_data.price_close FROM price_instruments_data "+\
        "JOIN symbol_list ON symbol_list.symbol = price_instruments_data.symbol "+\
        "WHERE symbol_list.uid = "+ str(uid) +\
        " AND date = DATE_SUB("+ str(last_date) +", INTERVAL 7 DAY)"
        cr_c.execute(sql_c)
        rs_c = cr_c.fetchall()
        model_prediction_tp = 0
        previous_price = 0
        for row in rs_c:
            model_prediction_tp = row[0]
            previous_price = row[1]
        cr_c.close()


        if model_prediction_tp != 0 and previous_price != 0:
            type_of_trade = ''
            if previous_price <= model_prediction_tp:
                type_of_trade = 'b'
            if previous_price > model_prediction_tp:
                type_of_trade = 's'
            if (previous_price >= last_price) and (type_of_trade == 'b'):
                if score > 0:
                    score = score - 0.01
            if (previous_price >= last_price) and (type_of_trade == 's'):
                score = score + 0.01
            if (previous_price < last_price) and (type_of_trade == 'b'):
                score = score + 0.01
            if (previous_price < last_price) and (type_of_trade == 's'):
                if score > 0:
                    score = score - 0.01
            debug("### score calc "+ str(model_score_column) +\
                  ": current score = " + str(score))

        ##########################################################
        # check specific model function to get target price
        # if force_full_update == False: applies only to arima_7d
        ##########################################################
        if model_tp == 0:
            ########################################################################
            # (3) Define function that calc the model target price
            ########################################################################
            last_model_tp = get_model_price_arima_7d(uid)
            cr_u = connection.cursor(pymysql.cursors.SSCursor)
            sql_u = "UPDATE price_instruments_data SET " +\
            str(model_tp_column) + " = " + str(last_model_tp) +\
            " WHERE symbol = '"+ str(symbol) +"' AND date = " + str(last_date)
            cr_u.execute(sql_u)
            connection.commit()
            ret = last_model_tp
        gc.collect()
    model_score = 0
    if not force_full_update:
        sql = "SELECT "+ str(model_score_column) +\
        " FROM instruments WHERE symbol = '"+ str(symbol) +"'"
        cursor.execute(sql)
        res = cursor.fetchall()
        for row in res:
            model_score = row[0]
    debug("### Total score calc "+ str(model_score_column) +": " +\
          str(model_score) + " + " + str(score))
    model_score = round(model_score + score, 2)
    debug("### Total score "+ str(model_score_column) +": " + str(model_score))


    sql = "UPDATE instruments SET " + str(model_score_column) + " = " +\
    str(model_score) + " WHERE symbol = '"+ str(symbol) +"'"
    cursor.execute(sql)
    connection.commit()
    cursor.close()
    gc.collect()
    return ret
