import os
import json
from flask import jsonify, make_response
import uuid
import time
from common.plugin_service import PluginService
from common.util.constant import STATUS_SUCCESS, STATUS_FAIL

class DummyPluginService(PluginService):

    def __init__(self):
        super().__init__()

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
        raise Exception('error')