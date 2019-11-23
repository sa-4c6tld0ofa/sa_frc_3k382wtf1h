""" Model trend 5-day """
import sys
import os
import gc
import pymysql.cursors
from model_trend_calc import TrendData
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


def get_model_5d_trend(uid, date_str, connection):
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

    target_price = 0
    price_close = 0
    ta_5dtrend = ''

    cursor = connection.cursor(pymysql.cursors.SSCursor)
    sql = "SELECT instruments.symbol, instruments.stdev_st FROM instruments "+\
    "JOIN symbol_list ON symbol_list.symbol = instruments.symbol "+\
    "WHERE symbol_list.uid = " + str(uid)
    cursor.execute(sql)
    res = cursor.fetchall()
    symbol = ''
    for row in res:
        symbol = row[0]
        stdev_st = row[1]

    sql = "SELECT price_close, 5dtrend FROM price_instruments_data "+\
    "WHERE symbol = '"+ str(symbol) +"' AND date = " + str(date_str)
    cursor.execute(sql)
    res = cursor.fetchall()
    for row in res:
        price_close = row[0]
        ta_5dtrend = row[1]

    if ta_5dtrend == 'u':
        target_price = float(price_close) + float(stdev_st)
    if ta_5dtrend == 'd':
        target_price = float(price_close) - float(stdev_st)
    if ta_5dtrend == '':
        target_price = price_close
    ret = target_price

    cursor.close()
    #---------------------------------------------------------------------------
    return ret

########################################################################
# (2) Set the name of the model function
########################################################################
def set_model_5d_trend(uid, force_full_update, connection):
    """ xxx """
    #-------------------------------------------------------------------
    ret = 0
    ########################################################################
    # (2.1) Define names of column in use by the model
    ########################################################################
    model_tp_column = 'price_instruments_data.5dtrend_tp'
    model_score_column = 'instruments.score_5dtrend'
    #-----------------------------------------------------------------------

    day_to_process = 370
    score = 0

    if force_full_update:
        sql_selection = "SELECT price_instruments_data.symbol, "+\
        "price_instruments_data.date, price_instruments_data.price_close, " +\
        str(model_tp_column) + " FROM price_instruments_data "+\
        "JOIN symbol_list ON symbol_list.symbol = price_instruments_data.symbol "+\
        "WHERE symbol_list.uid = "+ str(uid) +\
        " ORDER BY date DESC LIMIT "+ str(day_to_process)
    else:
        sql_selection = "SELECT price_instruments_data.symbol, "+\
        "price_instruments_data.date, price_instruments_data.price_close, " +\
        str(model_tp_column) + " FROM price_instruments_data "+\
        "JOIN symbol_list ON symbol_list.symbol = price_instruments_data.symbol "+\
        "WHERE symbol_list.uid = "+ str(uid) +\
        " AND price_instruments_data.is_ta_calc = 0 ORDER BY date DESC"

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

        if model_tp == 0:
            trend = TrendData(symbol, last_date)
            trend_3d_value = trend.get_3d_trend()
            trend_5d_value = trend.get_5d_trend()
            trend_7d_value = trend.get_7d_trend()
            cr_u = connection.cursor(pymysql.cursors.SSCursor)
            sql_u = "UPDATE price_instruments_data SET 3dtrend = '"+\
            str(trend_3d_value) +"', 5dtrend = '"+ str(trend_5d_value) +\
            "', 7dtrend = '"+ str(trend_7d_value) +"'  WHERE symbol = '"+\
            str(symbol) +"' AND date = " + str(last_date)
            cr_u.execute(sql_u)
            connection.commit()

            ########################################################################
            # (3) Define function that calc the model target price
            ########################################################################
            last_model_tp = get_model_5d_trend(uid, last_date, connection)
            #-----------------------------------------------------------------------
            sql_u = "UPDATE price_instruments_data SET " +\
            str(model_tp_column) + " = " + str(last_model_tp) +\
            " WHERE symbol = '"+ str(symbol) +"' AND date = " + str(last_date)
            cr_u.execute(sql_u)
            connection.commit()
            ret = last_model_tp
            cr_u.close()
        gc.collect()
    model_score = 0
    if not force_full_update:
        sql = "SELECT "+ str(model_score_column) +" FROM instruments "+\
        "WHERE symbol = '"+ str(symbol) +"'"
        cursor.execute(sql)
        res = cursor.fetchall()
        for row in res:
            model_score = row[0]
    debug("### Total score calc "+ str(model_score_column) +\
          ": " + str(model_score) + " + " + str(score))
    model_score = round(model_score + score, 2)
    debug("### Total score "+ str(model_score_column) +": " + str(model_score))

    sql = "UPDATE instruments SET " + str(model_score_column) +\
    " = " + str(model_score) + " WHERE symbol = '"+ str(symbol) +"'"
    cursor.execute(sql)
    connection.commit()
    cursor.close()
    gc.collect()
    return ret
