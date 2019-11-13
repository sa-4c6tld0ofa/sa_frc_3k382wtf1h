""" Calculate prediction model fullset """
from output_prediction import *

set_of_data_uid = 0
calc_all_data = True
order = 'asc'

for i in range(2):
    output_prediction(calc_all_data,set_of_data_uid,order)
