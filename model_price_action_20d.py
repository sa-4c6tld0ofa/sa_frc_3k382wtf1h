""" Model price action 20-day """
import sys
import os
import gc
from datetime import timedelta
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

def get_model_price_action_20d(uid, date_str, connection):
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
    column_data_name = 'price_action_20d'
    stdev_st = 0
    symbol = ''
    price_close = 0
    model_data = 0
    model_tp = 0

    cursor = connection.cursor(pymysql.cursors.SSCursor)
    sql = "SELECT instruments.stdev_st, instruments.symbol FROM instruments "+\
    "JOIN symbol_list ON symbol_list.symbol = instruments.symbol "+\
    "WHERE symbol_list.uid = " + str(uid)
    cursor.execute(sql)
    res = cursor.fetchall()
    symbol = ''
    for row in res:
        stdev_st = row[0]
        symbol = row[1]

    sql = "SELECT price_close, "+ column_data_name +\
    " FROM price_instruments_data WHERE symbol = '"+\
    str(symbol) +"' AND date = " + str(date_str)
    cursor.execute(sql)
    res = cursor.fetchall()
    for row in res:
        price_close = row[0]
        model_data = row[1]

    # Model Logic
    #-----------------------------------------------------------------------
    if model_data < 1:
        model_tp = price_close - stdev_st
    if model_data >= 1:
        model_tp = price_close + stdev_st
    #-----------------------------------------------------------------------
    ret = model_tp
    cursor.close()
    debug(str(symbol) + ' ::: '+ str(ret) +' = ' + str(date_str) +\
          ' ::: ' + str(price_close) + ' ::: stdev=' + str(stdev_st))
    return ret

########################################################################
# (2) Set the name of the model function
########################################################################
def set_model_price_action_20d(uid, force_full_update, connection):
    """ xxx """
    #-------------------------------------------------------------------
    ret = 0
    ########################################################################
    # (2.1) Define names of column in use by the model
    ########################################################################
    model_column = 'price_instruments_data.price_action_20d'
    model_tp_column = 'price_instruments_data.price_action_20d_tp'
    model_score_column = 'instruments.score_price_action_20d'
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
        selected_date = row[1]
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
            ########################################################################
            # (2.2) Create a function to calculate the value of the model data
            # function should contains specific parameters: symbol, date
            ########################################################################
            model_data = get_price_action_model_data(symbol, selected_date, connection)
            #-----------------------------------------------------------------------
            cr_u = connection.cursor(pymysql.cursors.SSCursor)
            sql_u = "UPDATE price_instruments_data SET "+\
            model_column +" = "+ str(model_data) +\
            " WHERE symbol = '"+ str(symbol) +"' AND date = " + str(last_date)
            cr_u.execute(sql_u)
            connection.commit()
            cr_u.close()
            ########################################################################
            # (3) Define function that calc the model target price
            ########################################################################
            model_tp = get_model_price_action_20d(uid, last_date, connection)
            debug(str(model_tp) + ' ::: ' + str(last_date) +\
                  ' ::: ' + str(selected_date))
            #-----------------------------------------------------------------------
            cr_v = connection.cursor(pymysql.cursors.SSCursor)
            sql_v = "UPDATE price_instruments_data SET " +\
            str(model_tp_column) + " = " + str(model_tp) +\
            " WHERE symbol = '"+ str(symbol) +"' AND date = " + str(last_date)
            cr_v.execute(sql_v)
            connection.commit()
            cr_v.close()
            ret = model_tp
        gc.collect()
    model_score = 0
    if not force_full_update:
        sql = "SELECT "+ str(model_score_column) +\
        " FROM instruments WHERE symbol = '"+ str(symbol) +"'"
        cursor.execute(sql)
        res = cursor.fetchall()
        for row in res:
            model_score = row[0]
    debug("### Total score calc "+ str(model_score_column) +\
          ": " + str(model_score) + " + " + str(score))
    model_score = round(model_score + score, 2)
    debug("### Total score "+ str(model_score_column) +\
          ": " + str(model_score))

    sql = "UPDATE instruments SET " + str(model_score_column) +\
    " = " + str(model_score) + " WHERE symbol = '"+ str(symbol) +"'"
    cursor.execute(sql)
    connection.commit()
    cursor.close()
    gc.collect()
    return ret



def get_data_day(what, symbol, date_start, date_end, connection):
    """ xxx """
    ret = 0

    cursor = connection.cursor(pymysql.cursors.SSCursor)
    if what == 'u':
        sql = 'SELECT COUNT(*) FROM price_instruments_data WHERE symbol="'+\
        str(symbol) +'" AND change_1d >0 AND date >=' +\
        str(date_start) + ' AND date <=' + str(date_end)
    if what == 'd':
        sql = 'SELECT COUNT(*) FROM price_instruments_data WHERE symbol="'+\
        str(symbol) +'" AND change_1d <0 AND date >=' +\
        str(date_start) + ' AND date <=' + str(date_end)
    if what == 'avgu':
        sql = 'SELECT AVG(change_1d) FROM price_instruments_data WHERE symbol="'+\
        str(symbol) +'" AND change_1d >0 AND date >=' +\
        str(date_start) + ' AND date <=' + str(date_end)
    if what == 'avgd':
        sql = 'SELECT AVG(change_1d) FROM price_instruments_data WHERE symbol="'+\
        str(symbol) +'" AND change_1d <0 AND date >=' +\
        str(date_start) + ' AND date <=' + str(date_end)
    if what == 's':
        sql = 'SELECT sentiment_1d FROM price_instruments_data WHERE symbol="'+\
        str(symbol) +'" AND date >=' + str(date_start) +\
        ' AND date <=' + str(date_end)
    debug(sql)
    cursor.execute(sql)
    res = cursor.fetchall()
    for row in res:
        ret = row[0]
    cursor.close()
    return ret

def get_price_action_model_data(symbol, selected_date, connection):
    """ xxx """
    ret = 0
    date_end = selected_date
    date_start = selected_date - timedelta(days=20)
    date_start_str = date_start.strftime("%Y%m%d")
    date_end_str = date_end.strftime("%Y%m%d")

    #1. count number of days up in num_period
    day_up = get_data_day('u', symbol, date_start_str, date_end_str, connection)
    #2. count number of days down in num_period
    day_down = get_data_day('d', symbol, date_start_str, date_end_str, connection)
    #3. average volatility percentage of days up in num_period
    day_avg_vol_up = get_data_day('avgu', symbol, date_start_str, date_end_str, connection)
    #4. average volatility percentage of days down in num_period
    day_avg_vol_down = get_data_day('avgd', symbol, date_start_str, date_end_str, connection)
    #5. sentiment_1d
    ccc = get_data_day('s', symbol, date_start_str, date_end_str, connection)
    aaa = 0
    bbb = 0

    # a = days_up / days_down
    if day_down != 0 and day_down is not None and day_up is not None:
        aaa = day_up / day_down
    # b = vol_days_up / vol_days_down
    if (day_avg_vol_down != 0 and day_avg_vol_down is not None and
            day_avg_vol_up is not None):
        bbb = day_avg_vol_up / day_avg_vol_down

    # (a + b) / 2
    if ccc != 0:
        ret = (aaa + bbb + ccc) / 3
    else:
        ret = (aaa + bbb) / 2
    return ret
