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

def get_model_price_action_20d(uid,date_str):
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
    r = 0
    column_data_name = 'price_action_20d'
    stdev_st = 0
    symbol = ''
    price_close = 0
    model_data = 0
    model_tp = 0

    connection = pymysql.connect(host=DB_SRV,
                                 user=DB_USR,
                                 password=DB_PWD,
                                 db=DB_NAME,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    cr = connection.cursor(pymysql.cursors.SSCursor)
    sql = "SELECT instruments.stdev_st, instruments.symbol FROM instruments JOIN symbol_list ON symbol_list.symbol = instruments.symbol WHERE symbol_list.uid = " + str(uid)
    cr.execute(sql)
    rs = cr.fetchall()
    for row in rs:
        stdev_st = row[0]
        symbol = row[1]

    sql = "SELECT price_close, "+ column_data_name +" FROM price_instruments_data WHERE symbol = '"+ str(symbol) +"' AND date = " + str(date_str)
    cr.execute(sql)
    rs = cr.fetchall()
    for row in rs:
        price_close = row[0]
        model_data = row[1]

    # Model Logic
    #-----------------------------------------------------------------------
    if model_data < 1: model_tp = price_close - stdev_st
    if model_data >= 1: model_tp = price_close + stdev_st
    #-----------------------------------------------------------------------
    r = model_tp
    cr.close()
    connection.close()

    debug(str(symbol) + ' ::: '+ str(r) +' = ' + str(date_str) + ' ::: ' + str(price_close) + ' ::: stdev=' + str(stdev_st) )
    return r

########################################################################
# (2) Set the name of the model function
########################################################################
def set_model_price_action_20d(uid,force_full_update):
    """
    Desc
    Args:
        None
    Returns:
        None
    """
    #-------------------------------------------------------------------
    r = 0
    ########################################################################
    # (2.1) Define names of column in use by the model
    ########################################################################
    model_column = 'price_instruments_data.price_action_20d'
    model_tp_column = 'price_instruments_data.price_action_20d_tp'
    model_score_column = 'instruments.score_price_action_20d'
    #-----------------------------------------------------------------------

    day_to_process = 370
    score = 0

    connection = pymysql.connect(host=DB_SRV,
                                 user=DB_USR,
                                 password=DB_PWD,
                                 db=DB_NAME,
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
        selected_date = row[1]
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
            debug("### score calc "+ str(model_score_column) +": current score = " + str(score) )

        if model_tp == 0:
            ########################################################################
            # (2.2) Create a function to calculate the value of the model data
            # function should contains specific parameters: symbol, date
            ########################################################################
            model_data = get_price_action_model_data(symbol, selected_date)
            #-----------------------------------------------------------------------
            cr_u = connection.cursor(pymysql.cursors.SSCursor)
            sql_u = "UPDATE price_instruments_data SET "+ model_column +" = "+ str(model_data) +" WHERE symbol = '"+ str(symbol) +"' AND date = " + str(last_date)
            cr_u.execute(sql_u)
            connection.commit()
            ########################################################################
            # (3) Define function that calc the model target price
            ########################################################################
            model_tp = get_model_price_action_20d(uid,last_date)
            debug( str(model_tp) + ' ::: ' + str(last_date) + ' ::: ' + str(selected_date) )
            #-----------------------------------------------------------------------
            cr_u = connection.cursor(pymysql.cursors.SSCursor)
            sql_u = "UPDATE price_instruments_data SET " + str(model_tp_column) + " = " + str( model_tp ) + " WHERE symbol = '"+ str(symbol) +"' AND date = " + str(last_date)
            cr_u.execute(sql_u)
            connection.commit()
            r = model_tp
            cr_u.close()
        gc.collect()
    model_score = 0
    if force_full_update == False:
        sql = "SELECT "+ str(model_score_column) +" FROM instruments WHERE symbol = '"+ str(symbol) +"'"
        cr.execute(sql)
        rs = cr.fetchall()
        for row in rs: model_score = row[0]
    debug("### Total score calc "+ str(model_score_column) +": " + str(model_score) + " + " + str(score) )
    model_score = round(model_score + score,2)
    debug("### Total score "+ str(model_score_column) +": " + str(model_score) )

    sql = "UPDATE instruments SET " + str(model_score_column) + " = " + str(model_score) + " WHERE symbol = '"+ str(symbol) +"'"
    cr.execute(sql)
    connection.commit()

    cr_c.close()
    cr.close()
    connection.close()
    gc.collect()
    return r



def get_data_day(w,symbol,date_start,date_end):
    """
    Desc
    Args:
        None
    Returns:
        None
    """
    r = 0
    connection = pymysql.connect(host=DB_SRV,
                                 user=DB_USR,
                                 password=DB_PWD,
                                 db=DB_NAME,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    cr = connection.cursor(pymysql.cursors.SSCursor)
    if w == 'u': sql = 'SELECT COUNT(*) FROM price_instruments_data WHERE symbol="'+ str(symbol) +'" AND change_1d >0 AND date >=' + str(date_start) + ' AND date <=' + str(date_end)
    if w == 'd': sql = 'SELECT COUNT(*) FROM price_instruments_data WHERE symbol="'+ str(symbol) +'" AND change_1d <0 AND date >=' + str(date_start) + ' AND date <=' + str(date_end)
    if w == 'avgu': sql = 'SELECT AVG(change_1d) FROM price_instruments_data WHERE symbol="'+ str(symbol) +'" AND change_1d >0 AND date >=' + str(date_start) + ' AND date <=' + str(date_end)
    if w == 'avgd': sql = 'SELECT AVG(change_1d) FROM price_instruments_data WHERE symbol="'+ str(symbol) +'" AND change_1d <0 AND date >=' + str(date_start) + ' AND date <=' + str(date_end)
    if w == 's': sql = 'SELECT sentiment_1d FROM price_instruments_data WHERE symbol="'+ str(symbol) +'" AND date >=' + str(date_start) + ' AND date <=' + str(date_end)
    debug(sql)
    cr.execute(sql)
    rs = cr.fetchall()
    for row in rs: r = row[0]
    cr.close()
    connection.close()
    return r

def get_price_action_model_data(symbol, selected_date):
    """
    Desc
    Args:
        None
    Returns:
        None
    """
    r = 0
    date_end = selected_date
    date_start = selected_date - timedelta(days=20)
    date_start_str = date_start.strftime("%Y%m%d")
    date_end_str = date_end.strftime("%Y%m%d")

    #1. count number of days up in num_period
    day_up = get_data_day('u',symbol,date_start_str,date_end_str)
    #2. count number of days down in num_period
    day_down = get_data_day('d',symbol,date_start_str,date_end_str)
    #3. average volatility percentage of days up in num_period
    day_avg_vol_up = get_data_day('avgu',symbol,date_start_str,date_end_str)
    #4. average volatility percentage of days down in num_period
    day_avg_vol_down = get_data_day('avgd',symbol,date_start_str,date_end_str)
    #5. sentiment_1d
    c = get_data_day('s',symbol,date_start_str,date_end_str)
    # a = days_up / days_down
    a = day_up / day_down
    # b = vol_days_up / vol_days_down
    b = day_avg_vol_up / day_avg_vol_down
    # (a + b) / 2
    if c != 0:
        r = (a + b + c) / 3
    else:
        r = (a + b) / 2
    return r
