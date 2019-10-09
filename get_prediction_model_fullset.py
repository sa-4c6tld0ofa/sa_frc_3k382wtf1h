# Copyright (c) 2018-present, Taatu Ltd.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
from output_prediction import *

set_of_data_uid = 0
calc_all_data = True
order = 'asc'

for i in range(2):
    output_prediction(calc_all_data,set_of_data_uid,order)
