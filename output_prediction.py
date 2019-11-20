""" Main module to calculate prediction model """
import sys
import os
import datetime
from datetime import timedelta
import gc
import pymysql.cursors
PDIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.abspath(PDIR))
from settings import SmartAlphaPath, debug, get_portf_suffix
SETT = SmartAlphaPath()
sys.path.append(os.path.abspath(SETT.get_path_core()))
from ta_instr_sum import get_instr_sum
from ta_gen_chart_data import gen_chart
from get_frc_pnl import get_forecast_pnl
from get_trades import get_trades
from logging import log_this
sys.path.append(os.path.abspath(SETT.get_path_pwd()))
from sa_access import sa_db_access
ACCESS_OBJ = sa_db_access()
DB_USR = ACCESS_OBJ.username()
DB_PWD = ACCESS_OBJ.password()
DB_NAME = ACCESS_OBJ.db_name()
DB_SRV = ACCESS_OBJ.db_server()

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
from model_arima_7d import set_model_arima_7d
from model_ma10 import set_model_ma10
from model_ma20 import set_model_ma20
from model_ma30 import set_model_ma30
from model_ma40 import set_model_ma40
from model_ma50 import set_model_ma50
from model_ma10ctt import set_model_ma10ctt
from model_arima_7dr import set_model_arima_7dr
from model_trend_3d import set_model_3d_trend
from model_trend_5d import set_model_5d_trend
from model_trend_7d import set_model_7d_trend
from model_price_action_20d import set_model_price_action_20d
from model_price_action_10d import set_model_price_action_10d
from model_price_action_10dr import set_model_price_action_10dr


def clear_chart_data(symbol):
    """
    Clear chart_data table for a given instrument
    Args:
        String: Instrument  symbol
    Returns:
        None
    """
    connection = pymysql.connect(host=DB_SRV,
                                 user=DB_USR,
                                 password=DB_PWD,
                                 db=DB_NAME,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    cursor = connection.cursor(pymysql.cursors.SSCursor)
    sql = "DELETE FROM chart_data WHERE symbol = '"+ str(symbol) +"'"
    cursor.execute(sql)
    connection.commit()
    cursor.close()
    connection.close()
    gc.collect()

def clear_trades(symbol):
    """
    Clear trade data for a given instrument
    Args:
        String: Instrument symbol
    Returns:
        None
    """
    connection = pymysql.connect(host=DB_SRV,
                                 user=DB_USR,
                                 password=DB_PWD,
                                 db=DB_NAME,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    cursor = connection.cursor(pymysql.cursors.SSCursor)
    sql = "DELETE FROM trades WHERE symbol = '"+ str(symbol) +"'"
    cursor.execute(sql)
    connection.commit()

    cursor.close()
    connection.close()


def get_instr_decimal_places(symbol):
    """
    Get specified instrument decimal places
    Args:
        String: Instrument symbol
    Returns:
        Integer: Decimal place
    """
    ret = 5
    connection = pymysql.connect(host=DB_SRV,
                                 user=DB_USR,
                                 password=DB_PWD,
                                 db=DB_NAME,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    cursor = connection.cursor(pymysql.cursors.SSCursor)
    sql = "SELECT decimal_places FROM instruments WHERE symbol = '"+\
    str(symbol) +"'"
    cursor.execute(sql)
    res = cursor.fetchall()
    for row in res:
        ret = row[0]
    cursor.close()
    connection.close()
    gc.collect()
    return ret

def compute_target_price(uid, force_full_update):
    """
    Calculate the target price according to various model score
    Args:
        Integer: Instrument unique id
        Boolean: If force full update, update regardless of flag is_ta_calc
    Returns:
        None
    """
    number_day_scan = 370
    date_minus_ten = datetime.datetime.now() - timedelta(days=10)
    date_minus_ten = date_minus_ten.strftime("%Y%m%d")


    #name column of each model in the same order...
    selected_model_column = 'price_instruments_data.arima_7d_tp'
    ############################################################################################
    # (2) Add model column here define variables
    ############################################################################################
    #Add column in the string below:
    column_of_each_model = '''
    instruments.score_arima_7d,
    instruments.score_ma10,
    instruments.score_ma20,
    instruments.score_ma30,
    instruments.score_ma40,
    instruments.score_ma50,
    instruments.score_ma10ctt,
    instruments.score_arima_7dr,
    instruments.score_3dtrend,
    instruments.score_5dtrend,
    instruments.score_7dtrend,
    instruments.score_price_action_20d,
    instruments.score_price_action_10d,
    instruments.score_price_action_10dr
    '''
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

    connection = pymysql.connect(host=DB_SRV,
                                 user=DB_USR,
                                 password=DB_PWD,
                                 db=DB_NAME,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    cursor = connection.cursor(pymysql.cursors.SSCursor)
    sql = "SELECT symbol from symbol_list WHERE uid = " + str(uid)
    cursor.execute(sql)
    res = cursor.fetchall()
    for row in res:
        symbol = row[0]

    sql = "SELECT asset_class, pip, " + str(column_of_each_model) +\
    " FROM instruments JOIN symbol_list ON "+\
    "symbol_list.symbol = instruments.symbol WHERE symbol_list.uid = " + str(uid)
    cursor.execute(sql)
    res = cursor.fetchall()
    for row in res:
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
    model_list = (score_arima_7d,
                  score_ma10,
                  score_ma20,
                  score_ma30,
                  score_ma40,
                  score_ma50,
                  score_ma10ctt,
                  score_arima_7dr,
                  score_3dtrend,
                  score_5dtrend,
                  score_7dtrend,
                  score_price_action_20d,
                  score_price_action_10d,
                  score_price_action_10dr
                  )
    #--------------------------------------------------------------------------------------------
    selected_model_id = model_list.index(max(model_list))

    ##############################################################################################
    # (5) Add model column in here for index and increment id.
    ##############################################################################################
    if selected_model_id == 0:
        selected_model_column = 'price_instruments_data.arima_7d_tp'
    if selected_model_id == 1:
        selected_model_column = 'price_instruments_data.ma10_tp'
    if selected_model_id == 2:
        selected_model_column = 'price_instruments_data.ma20_tp'
    if selected_model_id == 3:
        selected_model_column = 'price_instruments_data.ma30_tp'
    if selected_model_id == 4:
        selected_model_column = 'price_instruments_data.ma40_tp'
    if selected_model_id == 5:
        selected_model_column = 'price_instruments_data.ma50_tp'
    if selected_model_id == 6:
        selected_model_column = 'price_instruments_data.ma10ctt_tp'
    if selected_model_id == 7:
        selected_model_column = 'price_instruments_data.arima_7dr_tp'
    if selected_model_id == 8:
        selected_model_column = 'price_instruments_data.3dtrend_tp'
    if selected_model_id == 9:
        selected_model_column = 'price_instruments_data.5dtrend_tp'
    if selected_model_id == 10:
        selected_model_column = 'price_instruments_data.7dtrend_tp'
    if selected_model_id == 11:
        selected_model_column = 'price_instruments_data.price_action_20d_tp'
    if selected_model_id == 12:
        selected_model_column = 'price_instruments_data.price_action_10d_tp'
    if selected_model_id == 13:
        selected_model_column = 'price_instruments_data.price_action_10dr_tp'
    #---------------------------------------------------------------------------------------------

    sql = "SELECT id FROM price_instruments_data WHERE symbol ='"+\
    symbol +"' ORDER BY date DESC LIMIT 1"
    cursor.execute(sql)
    res = cursor.fetchall()
    price_id = 0
    for row in res:
        price_id = row[0]

    if force_full_update:
        sql = "UPDATE price_instruments_data SET "+\
        "price_instruments_data.target_price = CAST("+\
        selected_model_column +" AS DECIMAL(20,"+\
        str(get_instr_decimal_places(symbol)) +\
        ")) WHERE price_instruments_data.symbol = '"+ symbol + "'"
        debug('### ::: ' + sql)
        cursor.execute(sql)
        connection.commit()
        clear_chart_data(symbol)
        clear_trades(symbol)
        get_forecast_pnl(symbol, uid, number_day_scan, force_full_update)

        get_trades(symbol, uid, number_day_scan, False)
        gen_chart(symbol, uid)
        get_instr_sum(symbol, uid, asset_class, date_minus_ten, pip)
    else:
        sql = "UPDATE price_instruments_data SET "+\
        "price_instruments_data.target_price = CAST("+\
        selected_model_column +" AS DECIMAL(20,"+\
        str(get_instr_decimal_places(symbol)) +\
        ")) WHERE price_instruments_data.id = " + str(price_id)
        cursor.execute(sql)
        connection.commit()


    cursor.close()
    connection.close()
    gc.collect()


def set_all_prediction_model_target_price_n_score(uid, force_full_update):
    """
    Set all prediction model target price and calculate score
    Args:
        Integer: Instrument unique id
        Booelan: Force full update regardless of the flag is_ta_calc if True
    Returns:
        None
    """
    log_this('2. set_all_prediction_model_target_price_n_score (uid'+ str(uid) +')', 0)
    #Set the score and return model_tp
    ##############################################################################################
    # (6) Call function for each model.
    ##############################################################################################
    set_model_arima_7d(uid, force_full_update)
    set_model_ma10(uid, force_full_update)
    set_model_ma20(uid, force_full_update)
    set_model_ma30(uid, force_full_update)
    set_model_ma40(uid, force_full_update)
    set_model_ma50(uid, force_full_update)
    set_model_ma10ctt(uid, force_full_update)
    set_model_arima_7dr(uid, force_full_update)
    set_model_3d_trend(uid, force_full_update)
    set_model_5d_trend(uid, force_full_update)
    set_model_7d_trend(uid, force_full_update)
    set_model_price_action_20d(uid, force_full_update)
    set_model_price_action_10d(uid, force_full_update)
    set_model_price_action_10dr(uid, force_full_update)
    #--------------------------------------------------------------------------------------------

    #target_price get the value of highest score model
    compute_target_price(uid, force_full_update)
    log_this('2. set_all_prediction_model_target_price_n_score (uid'+ str(uid) +')', 1)


def output_prediction(force_full_update, uid, order):
    """
    Main function to get prediction calculated and update tables in database
    Args:
        Boolean: if True, update tables regardless of flag is_ta_calc
        Integer: Instrument unique id
        String: order of update desc, asc. by symbol.
    Returns:
        None
    """
    log_this('2. output_prediction' , 0)
    connection = pymysql.connect(host=DB_SRV,
                                 user=DB_USR,
                                 password=DB_PWD,
                                 db=DB_NAME,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    cursor = connection.cursor(pymysql.cursors.SSCursor)

    if uid == 0:
        sql = "SELECT uid FROM symbol_list WHERE symbol NOT LIKE '%"+\
        get_portf_suffix() +"%' AND disabled=0 ORDER BY symbol "+ order
    else:
        sql = "SELECT uid FROM symbol_list WHERE uid = " + str(uid)

    cursor.execute(sql)
    res = cursor.fetchall()
    for row in res:
        uid = row[0]
        set_all_prediction_model_target_price_n_score(uid, force_full_update)

    cursor.close()
    connection.close()
    gc.collect()
    log_this('2. output_prediction' , 1)
