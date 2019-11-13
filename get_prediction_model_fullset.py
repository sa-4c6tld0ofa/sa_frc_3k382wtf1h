""" Calculate prediction model fullset """
from output_prediction import output_prediction

SET_OF_DATA_UID = 0
CALC_ALL_DATA = True
ORDER = 'asc'

for i in range(2):
    output_prediction(CALC_ALL_DATA, SET_OF_DATA_UID, ORDER)
