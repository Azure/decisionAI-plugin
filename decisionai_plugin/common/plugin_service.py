import asyncio
import atexit
import json
import os
import shutil
import time
import traceback
import uuid
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from os import environ

import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from flask import jsonify, make_response

from .tsanaclient import TSANAClient
from .util.constant import InferenceState
from .util.constant import ModelState
from .util.constant import STATUS_SUCCESS, STATUS_FAIL
from .util.context import Context
from .util.meta import insert_meta, get_meta, update_state, get_model_list, clear_state_when_necessary
from .util.model import upload_model, download_model
from .util.monitor import init_monitor, run_monitor, stop_monitor
from .util.timeutil import str_to_dt

#async infras
#executor = ProcessPoolExecutor()
#ThreadPool easy for debug
executor = ThreadPoolExecutor()
loop = asyncio.new_event_loop()

#monitor infras
sched = BackgroundScheduler()

from telemetry import log

def load_config(path):
    try:
        with open(path, 'r') as config_file:
            config_yaml = yaml.safe_load(config_file)
            Config = namedtuple('Config', sorted(config_yaml))
            config = Config(**config_yaml)
        return config
    except Exception:
        return None


class PluginService():
    def __init__(self, trainable=True):
        self.trainable = trainable

        config_file = environ.get('SERVICE_CONFIG_FILE')
        config = load_config(config_file)
        if config is None:
            log.error("No configuration '%s', or the configuration is not in JSON format. " % (config_file))
            exit()
        self.config = config
        self.tsanaclient = TSANAClient(config.series_limit)

        if self.trainable:
            init_monitor(config)
            sched.add_job(func=lambda: run_monitor(config), trigger="interval", seconds=10)
            sched.start()
            atexit.register(lambda: stop_monitor(config))
            atexit.register(lambda: sched.shutdown())

    def do_verify(self, parameters, context:Context):
        return STATUS_SUCCESS, ''

    def need_retrain(self, current_series_set, current_params, new_series_set, new_params, context:Context):
        return True

    def do_train(self, model_dir, parameters, series, context:Context):
        return STATUS_SUCCESS, ''

    def do_inference(self, model_dir, parameters, series, context:Context):
        return STATUS_SUCCESS, [], ''

    def do_delete(self, subscription, model_id):
        return STATUS_SUCCESS, ''

    def get_data_time_range(self, parameters, is_training=False):
        return str_to_dt(parameters['startTime']), str_to_dt(parameters['endTime'])

    def train_wrapper(self, subscription, model_id, parameters, callback):
        start = time.time()

        log.info("Start train wrapper for model %s by %s " % (model_id, subscription))
        try:
            model_dir = os.path.join(self.config.model_dir, subscription + '_' + model_id + '_' + str(time.time()))
            os.makedirs(model_dir, exist_ok=True)

            if 'dataRetrieving' in parameters and parameters['dataRetrieving']:
                start_time, end_time = self.get_data_time_range(parameters, True)
                series = self.tsanaclient.get_timeseries(parameters['apiEndpoint'], parameters['apiKey'], parameters['seriesSets'], start_time, end_time)
                result, message = self.do_train(model_dir, parameters, series, Context(subscription, model_id))
            else:
                result, message = self.do_train(model_dir, parameters, None, Context(subscription, model_id))
            
            if result == STATUS_SUCCESS:
                if callback is not None:
                    callback(subscription, model_id, model_dir, parameters, ModelState.Ready, message)
            else:
                raise Exception(message)
        except Exception as e:
            error_message = str(e) + '\n' + traceback.format_exc()
            if callback is not None:
                callback(subscription, model_id, None, parameters, ModelState.Failed, error_message)

            result = STATUS_FAIL
        finally:
            shutil.rmtree(model_dir, ignore_errors=True)

        total_time = (time.time() - start)
        log.duration("training_task_duration", total_time, model_id=model_id, result=result, endpoint=parameters['apiEndpoint'], group_id=parameters['groupId'], group_name=parameters['groupName'].replace(' ', '_'), instance_id=parameters['instance']['instanceId'], instance_name=parameters['instance']['instanceName'].replace(' ', '_'))
        log.count("training_task_count", 1,  model_id=model_id, result=result, endpoint=parameters['apiEndpoint'], group_id=parameters['groupId'], group_name=parameters['groupName'].replace(' ', '_'), instance_id=parameters['instance']['instanceId'], instance_name=parameters['instance']['instanceName'].replace(' ', '_'))

        return STATUS_SUCCESS, ''

    # inference_window: 30
    # endTime: endtime
    def inference_wrapper(self, subscription, model_id, parameters, callback):
        start = time.time()

        log.info("Start inference wrapper %s by %s " % (model_id, subscription))
        try:
            result, message = self.do_verify(parameters, Context(subscription, model_id))
            if result != STATUS_SUCCESS:
                raise Exception('Verify failed! ' + message)

            model_dir = os.path.join(self.config.model_dir, subscription + '_' + model_id + '_' + str(time.time()))
            os.makedirs(model_dir, exist_ok=True)

            if self.trainable:
                download_model(self.config, subscription, model_id, model_dir)
            
            if 'dataRetrieving' in parameters and parameters['dataRetrieving']:
                start_time, end_time = self.get_data_time_range(parameters)
                series = self.tsanaclient.get_timeseries(parameters['apiEndpoint'], parameters['apiKey'], parameters['seriesSets'], start_time, end_time)
                result, values, message = self.do_inference(model_dir, parameters, series, Context(subscription, model_id))
            else:
                result, values, message = self.do_inference(model_dir, parameters, None, Context(subscription, model_id))

            if callback is not None:
                callback(subscription, model_id, parameters, result, values, message)
        except Exception as e:
            error_message = str(e) + '\n' + traceback.format_exc()
            results = [{'timestamp': parameters['endTime'], 'status': InferenceState.Failed.name, 'result': dict(errorMessage=error_message)}]
            self.tsanaclient.save_inference_result(parameters, results)

            if callback is not None:
                callback(subscription, model_id, parameters, STATUS_FAIL, None, error_message)
            
            result = STATUS_FAIL
        finally:
            shutil.rmtree(model_dir, ignore_errors=True)

        total_time = (time.time() - start)
        log.duration("inference_task_duration", total_time, model_id=model_id, result=result, endpoint=parameters['apiEndpoint'], group_id=parameters['groupId'], group_name=parameters['groupName'].replace(' ', '_'), instance_id=parameters['instance']['instanceId'], instance_name=parameters['instance']['instanceName'].replace(' ', '_'))
        log.count("inference_task_count", 1,  model_id=model_id, result=result, endpoint=parameters['apiEndpoint'], group_id=parameters['groupId'], group_name=parameters['groupName'].replace(' ', '_'), instance_id=parameters['instance']['instanceId'], instance_name=parameters['instance']['instanceName'].replace(' ', '_'))

        return STATUS_SUCCESS, ''

    def train_callback(self, subscription, model_id, model_dir, parameters, model_state, last_error=None):
        log.info("Training callback %s by %s , state = %s, last_error = %s" % (model_id, subscription, model_state, last_error if last_error is not None else ''))
        meta = get_meta(self.config, subscription, model_id)
        if meta is None or meta['state'] == ModelState.Deleted.name:
            return STATUS_FAIL, 'Model is not found! '

        if model_state == ModelState.Ready:
            result, message = upload_model(self.config, subscription, model_id, model_dir)
            if result != STATUS_SUCCESS:
                model_state = ModelState.Failed
                last_error = 'Model storage failed! ' + message

        update_state(self.config, subscription, model_id, model_state, None, last_error)
        return self.tsanaclient.save_training_result(parameters, model_id, model_state.name, last_error)

    def inference_callback(self, subscription, model_id, parameters, result, values, last_error=None):
        if result == STATUS_SUCCESS:
            for value in values:
                result, last_error = self.tsanaclient.save_data_points(parameters, value['metricId'], value['dimension'], value['timestamps'], value['values'], value['fields'], value['fieldValues'])
                if result != STATUS_SUCCESS:
                    raise Exception(last_error)
        log.info ("Inference callback %s by %s , result = %s, last_error = %s" % (model_id, subscription, result, last_error if last_error is not None else ''))

    def train(self, request):
        request_body = json.loads(request.data)
        instance_id = request_body['instance']['instanceId']
        if not self.trainable:
            return make_response(jsonify(dict(instanceId=instance_id, modelId='', result=STATUS_SUCCESS, message='Model is not trainable', modelState=ModelState.Ready.name)), 200)

        subscription = request.headers.get('apim-subscription-id', 'Official')
        result, message = self.do_verify(request_body, Context(subscription, ''))
        if result != STATUS_SUCCESS:
            return make_response(jsonify(dict(instanceId=instance_id, modelId='', result=STATUS_FAIL, message='Verify failed! ' + message, modelState=ModelState.Deleted.name)), 400)

        models_in_train = []
        for model in get_model_list(self.config, subscription):
            if 'instanceId' in model and model['instanceId'] == request_body['instance']['instanceId'] and model['state'] == ModelState.Training.name:
                models_in_train.append(model['modelId'])

        if len(models_in_train) >= self.config.models_in_training_limit_per_instance:
            return make_response(jsonify(dict(instanceId=instance_id, modelId='', result=STATUS_FAIL, message='Models in training limit reached! Abort training this time.', modelState=ModelState.Deleted.name)), 400)

        log.info('Create training task')
        try:
            if 'modelId' in request_body and request_body['modelId']:
                model_id = request_body['modelId']
            else:
                model_id = str(uuid.uuid1())
            insert_meta(self.config, subscription, model_id, request_body)
            meta = get_meta(self.config, subscription, model_id)
            asyncio.ensure_future(loop.run_in_executor(executor, self.train_wrapper, subscription, model_id, request_body, self.train_callback))
            return make_response(jsonify(dict(instanceId=instance_id, modelId=model_id, result=STATUS_SUCCESS, message='Training task created', modelState=ModelState.Training.name)), 201)
        except Exception as e: 
            meta = get_meta(self.config, subscription, model_id)
            error_message = str(e) + '\n' + traceback.format_exc()
            if meta is not None: 
                update_state(self.config, subscription, model_id, ModelState.Failed, None, error_message)
            return make_response(jsonify(dict(instanceId=instance_id, modelId=model_id, result=STATUS_FAIL, message='Fail to create new task ' + error_message, modelState=ModelState.Failed.name)), 400)

    def inference(self, request, model_id):
        request_body = json.loads(request.data)
        instance_id = request_body['instance']['instanceId']
        subscription = request.headers.get('apim-subscription-id', 'Official')
        
        if self.trainable:
            meta = get_meta(self.config, subscription, model_id)
            if meta is None:
                return make_response(jsonify(dict(instanceId=instance_id, modelId=model_id, result=STATUS_FAIL, message='Model is not found!', modelState=ModelState.Deleted.name)), 400)
                
            if meta['state'] != ModelState.Ready.name:
                return make_response(jsonify(dict(instanceId=instance_id, modelId=model_id, result=STATUS_FAIL, message='Cannot do inference right now, status is ' + meta['state'], modelState=meta['state'])), 400)

            current_set = json.dumps(json.loads(meta['series_set']), sort_keys=True)
            current_params = json.dumps(json.loads(meta['para']), sort_keys=True)

            new_set = json.dumps(request_body['seriesSets'], sort_keys=True)
            new_params = json.dumps(request_body['instance']['params'], sort_keys=True)

            if current_set != new_set or current_params != new_params:
                if self.need_retrain(json.loads(meta['series_set']), json.loads(meta['para']), request_body['seriesSets'], request_body['instance']['params'], Context(subscription, model_id)):
                    return make_response(jsonify(dict(instanceId=instance_id, modelId=model_id, result=STATUS_FAIL, message='Inconsistent series sets or params!', modelState=meta['state'])), 400)

        log.info('Create inference task')
        asyncio.ensure_future(loop.run_in_executor(executor, self.inference_wrapper, subscription, model_id, request_body, self.inference_callback))
        return make_response(jsonify(dict(instanceId=instance_id, modelId=model_id, result=STATUS_SUCCESS, message='Inference task created', modelState=ModelState.Ready.name)), 201)

    def state(self, request, model_id):
        if not self.trainable:
            return make_response(jsonify(dict(instanceId='', modelId=model_id, result=STATUS_SUCCESS, message='Model is not trainable', modelState=ModelState.Ready.name)), 200)

        try:
            subscription = request.headers.get('apim-subscription-id', 'Official')
            meta = get_meta(self.config, subscription, model_id)
            if meta == None:
                return make_response(jsonify(dict(instanceId='', modelId=model_id, result=STATUS_FAIL, message='Model is not found!', modelState=ModelState.Deleted.name)), 400)

            meta = clear_state_when_necessary(self.config, subscription, model_id, meta)
            return make_response(jsonify(dict(instanceId='', modelId=model_id, result=STATUS_SUCCESS, message=meta['last_error'] if 'last_error' in meta else '', modelState=meta['state'])), 200)
        except Exception as e:
            error_message = str(e) + '\n' + traceback.format_exc()
            return make_response(jsonify(dict(instanceId='', modelId=model_id, result=STATUS_FAIL, message=error_message, modelState=ModelState.Failed.name)), 400)
        
    def list_models(self, request):
        subscription = request.headers.get('apim-subscription-id', 'Official')
        return make_response(jsonify(get_model_list(self.config, subscription)), 200)

    def delete(self, request, model_id):
        if not self.trainable:
            return make_response(jsonify(dict(instanceId='', modelId=model_id, result=STATUS_SUCCESS, message='Model is not trainable')), 200)

        try:
            subscription = request.headers.get('apim-subscription-id', 'Official')
            result, message = self.do_delete(subscription, model_id)
            if result == STATUS_SUCCESS:
                update_state(self.config, subscription, model_id, ModelState.Deleted)
                return make_response(jsonify(dict(instanceId='', modelId=model_id, result=STATUS_SUCCESS, message='Model {} has been deleted'.format(model_id), modelState=ModelState.Deleted.name)), 200)
            else:
                raise Exception(message)
        except Exception as e:
            error_message = str(e) + '\n' + traceback.format_exc()
            return make_response(jsonify(dict(instanceId='', modelId=model_id, result=STATUS_FAIL, message=error_message, modelState=ModelState.Failed.name)), 400)

    def verify(self, request):
        request_body = json.loads(request.data)
        instance_id = request_body['instance']['instanceId']
        subscription = request.headers.get('apim-subscription-id', 'Official')
        result, message = self.do_verify(request_body, Context(subscription, ''))
        if result != STATUS_SUCCESS:
            return make_response(jsonify(dict(instanceId=instance_id, modelId='', result=STATUS_FAIL, message='Verify failed! ' + message, modelState=ModelState.Deleted.name)), 400)
        else:
            return make_response(jsonify(dict(instanceId=instance_id, modelId='', result=STATUS_SUCCESS, message='Verify successfully! ' + message, modelState=ModelState.Deleted.name)), 200)