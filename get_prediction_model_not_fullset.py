""" Calculate prediction model for flagged is_ta_calc=0 """
from output_prediction import output_prediction

SET_OF_DATA_UID = 0
CALC_ALL_DATA = False
ORDER = 'asc'

output_prediction(CALC_ALL_DATA, SET_OF_DATA_UID, ORDER)
