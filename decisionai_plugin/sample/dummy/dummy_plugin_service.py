import os
import json
from flask import jsonify, make_response
import uuid
import time
import datetime

from common.plugin_service import PluginService
from common.util.constant import STATUS_SUCCESS, STATUS_FAIL
from common.util.timeutil import dt_to_str, str_to_dt, dt_to_str_file_name

class DummyPluginService(PluginService):

    def __init__(self):
        super().__init__(False)

    def need_retrain(self, current_series_set, current_params, new_series_set, new_params, context):
        return False

    def do_train(self, model_dir, parameters, context):
        sub_dir = os.path.join(model_dir, 'test')
        os.makedirs(sub_dir, exist_ok=True)
        with open(os.path.join(sub_dir, 'test_model.txt'), 'w') as text_file:
            text_file.write('test')
        
        time.sleep(2)
        return STATUS_SUCCESS, ''

    def do_inference(self, model_dir, parameters, context):
        start = time.time()

        start_time = str_to_dt(parameters['startTime'])
        start_time = start_time + datetime.timedelta(days=-3)
        end_time = str_to_dt(parameters['endTime'])
        factor_def = parameters['seriesSets']

        factors_data = self.tsanaclient.get_timeseries(parameters['apiEndpoint'], parameters['apiKey'], factor_def, start_time, end_time, 40000)
        print("Data item number: {}".format(len(factors_data)))
        total_time = time.time() - start

        sub_dir = os.path.join('temp', 'test_aidice_data')
        os.makedirs(sub_dir, exist_ok=True)

        with open(os.path.join(sub_dir, 'aidice_data_{}_rows_{}_duration_{}s.txt'.format(dt_to_str_file_name(datetime.datetime.utcnow()), len(factors_data), total_time)), 'a') as text_file:
            for series in factors_data:
                data_str = json.dumps(series.__dict__) + '\n'
                text_file.write(data_str)
        return STATUS_SUCCESS, ''