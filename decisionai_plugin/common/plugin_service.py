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
from .util.constant import INSTANCE_ID_KEY
from .util.context import Context
from .util.meta import insert_meta, get_meta, update_state, get_model_list, clear_state_when_necessary
from .util.model import upload_model, download_model
from .util.monitor import init_monitor, run_monitor, stop_monitor
from .util.timeutil import str_to_dt

from .util.kafka_operator import send_message, consume_loop
from .util.job_record import JobRecord

import zlib
import base64
import gc

#async infras
#executor = ProcessPoolExecutor()
#ThreadPool easy for debug
executor = ThreadPoolExecutor(max_workers=3)

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
        config_file = environ.get('SERVICE_CONFIG_FILE')
        config = load_config(config_file)
        if config is None:
            log.error("No configuration '%s', or the configuration is not in JSON format. " % (config_file))
            exit()
        self.config = config
        self.tsanaclient = TSANAClient()

        self.trainable = trainable
        if self.trainable:
            init_monitor(config)
            sched.add_job(func=lambda: run_monitor(config), trigger="interval", seconds=10)
            sched.start()
            atexit.register(lambda: stop_monitor(config))
            atexit.register(lambda: sched.shutdown())

            self.training_topic = self.__class__.__name__ + '-training'
            executor.submit(consume_loop, self.train_wrapper, self.training_topic)

        self.inference_topic = self.__class__.__name__ + '-inference'
        executor.submit(consume_loop, self.inference_wrapper, self.inference_topic)

    # verify parameters
    # Parameters:
    #   parameters: a dict object which should includes
    #       apiEndpoint: api endpoint for specific user
    #       apiKey: api key for specific user
    #       groupId: groupId in TSANA, which is copied from inference request, or from the entity
    #       series_sets: Array of series set
    #   context: request context include subscription and model_id
    # Return: 
    #   STATUS_SUCCESS/STATUS_FAIL, error_message
    def do_verify(self, parameters, context:Context):
        return STATUS_SUCCESS, ''

    # check if need to retrain model this time
    # Parameters:
    #   current_series_set: series set used in instance now
    #   current_params: params used in instance now
    #   new_series_set: series set used in this request
    #   new_params: params used in this request
    #   context: request context include subscription and model_id
    # Return:
    #   True/False
    def need_retrain(self, current_series_set, current_params, new_series_set, new_params, context:Context):
        return True

    # train model
    # Parameters:
    #   model_dir: output dir for model training result, framework will handle model storage
    #   parameters: training request body which include
    #     apiEndpoint: api endpoint for specific user
    #     apiKey: api key for specific user
    #     groupId: groupId in TSANA
    #     seriesSets: Array of series set
    #     startTime: start timestamp
    #     endTime: end timestamp
    #     instance: an info dict for this instance which includes
    #       instanceId: UUID for this instance
    #       params: training parameters for this request
    #   series: an array of Series object or None if config.auto_data_retrieving is False
    #     Series include
    #       series_id: UUID
    #       dim: dimension dict for this series
    #       fields: 1-d string array, ['time', '__VAL__', '__FIELD__.ExpectedValue', '__FIELD__.IsAnomaly', '__FIELD__.PredictionValue', '__FIELD__.PredictionModelScore', '__FIELD__.IsSuppress', '__FIELD__.Period', '__FIELD__.CostPoint', '__FIELD__.Mean', '__FIELD__.STD', '__FIELD__.TrendChangeAnnotate', '__FIELD__.TrendChang...tateIgnore', '__FIELD__.AnomalyAnnotate', ...]
    #       value: 2-d array, [['2020-10-12T17:55:00Z', 1.0, None, None, None, None, None, None, None, None, None, None, None, None, ...]]
    #   context: request context include subscription and model_id
    # Return:
    #   STATUS_SUCCESS/STATUS_FAIL, error_message
    def do_train(self, model_dir, parameters, series, context:Context):
        return STATUS_SUCCESS, ''

    # inference model
    # Parameters:
    #   model_dir: input dir for model inference, model has been download and unpacked to this dir
    #   parameters: inference request body which include
    #     apiEndpoint: api endpoint for specific user
    #     apiKey: api key for specific user
    #     groupId: groupId in TSANA
    #     seriesSets: Array of series set
    #     startTime: start timestamp
    #     endTime: end timestamp
    #     instance: an info dict for this instance which includes
    #       instanceId: UUID for this instance
    #       params: inference parameters for this request
    #       target: a dict for inference result which include
    #         dimensions: dimension name list for target, defined when register plugin
    #         metrics: metric name list for target, defined when register plugin
    #         granularityName: granularity name for target, defined when register plugin
    #         hookIds: hook id list, defined when register plugin
    #   series: an array of Series object or None if config.auto_data_retrieving is False
    #     Series include
    #       series_id: UUID
    #       dim: dimension dict for this series
    #       fields: 1-d string array, ['time', '__VAL__', '__FIELD__.ExpectedValue', '__FIELD__.IsAnomaly', '__FIELD__.PredictionValue', '__FIELD__.PredictionModelScore', '__FIELD__.IsSuppress', '__FIELD__.Period', '__FIELD__.CostPoint', '__FIELD__.Mean', '__FIELD__.STD', '__FIELD__.TrendChangeAnnotate', '__FIELD__.TrendChang...tateIgnore', '__FIELD__.AnomalyAnnotate', ...]
    #       value: 2-d array, [['2020-10-12T17:55:00Z', 1.0, None, None, None, None, None, None, None, None, None, None, None, None, ...]]
    #   context: request context include subscription and model_id
    # Return:
    #   result: STATUS_SUCCESS/STATUS_FAIL
    #   values: a list of value dict or None if you do not need framework to handle inference result storge, this value dict should include
    #     metricId: UUID, comes from metrics segment of target of request body
    #     dimension: dimension dict for this series, dimension names come from target segment of request body
    #     timestamps: string timestamps list
    #     values: double type value list, matching timestamps 
    #     fields: field names list, optional
    #     fieldValues: 2-d array which include a value list for each field, optional
    #   message: error message
    def do_inference(self, model_dir, parameters, series, context:Context):
        return STATUS_SUCCESS, None, ''

    def do_delete(self, parameters, model_id):
        return STATUS_SUCCESS, ''

    def get_data_time_range(self, parameters, is_training=False):
        return str_to_dt(parameters['startTime']), str_to_dt(parameters['endTime'])

    def train_wrapper(self, message):
        start = time.time()        

        subscription = message['subscription']
        model_id = message['model_id']
        task_id = message['job_id']
        parameters = message['params']

        log.info("Start train wrapper for model %s by %s " % (model_id, subscription))
        try:
            self.tsanaclient.save_training_status(task_id, parameters, ModelState.Pending.name)
            
            model_dir = os.path.join(self.config.model_dir, subscription + '_' + model_id + '_' + str(time.time()))
            os.makedirs(model_dir, exist_ok=True)

            series = None
            if self.config.auto_data_retrieving:
                start_time, end_time = self.get_data_time_range(parameters, True)
                series = self.tsanaclient.get_timeseries_gw(parameters, parameters['seriesSets'], start_time, end_time)
            
            update_state(self.config, subscription, model_id, ModelState.Training)
            self.tsanaclient.save_training_status(task_id, parameters, ModelState.Training.name)
            result, message = self.do_train(model_dir, parameters, series, Context(subscription, model_id, task_id))

            if result == STATUS_SUCCESS:
                    self.train_callback(subscription, model_id, task_id, model_dir, parameters, ModelState.Ready, None)
            else:
                raise Exception(message)
        except Exception as e:
            self.train_callback(subscription, model_id, task_id, None, parameters, ModelState.Failed, str(e))

            result = STATUS_FAIL
        finally:
            shutil.rmtree(model_dir, ignore_errors=True)

        total_time = (time.time() - start)
        log.duration("training_task_duration", total_time, model_id=model_id, task_id=task_id, result=result, endpoint=parameters['apiEndpoint'], group_id=parameters['groupId'], group_name=parameters['groupName'].replace(' ', '_'), instance_id=parameters['instance']['instanceId'], instance_name=parameters['instance']['instanceName'].replace(' ', '_'))
        log.count("training_task_count", 1,  model_id=model_id, task_id=task_id, result=result, endpoint=parameters['apiEndpoint'], group_id=parameters['groupId'], group_name=parameters['groupName'].replace(' ', '_'), instance_id=parameters['instance']['instanceId'], instance_name=parameters['instance']['instanceName'].replace(' ', '_'))
        gc.collect()

        return STATUS_SUCCESS, ''

    # inference_window: 30
    # endTime: endtime
    def inference_wrapper(self, message):
        start = time.time()

        subscription = message['subscription']
        model_id = message['model_id']
        task_id = message['job_id']
        parameters = message['params']

        log.info("Start inference wrapper %s by %s " % (model_id, subscription))
        try:
            self.tsanaclient.save_inference_status(task_id, parameters, InferenceState.Pending.name)

            result, message = self.do_verify(parameters, Context(subscription, model_id, task_id))
            if result != STATUS_SUCCESS:
                raise Exception('Verify failed! ' + message)

            model_dir = os.path.join(self.config.model_dir, subscription + '_' + model_id + '_' + str(time.time()))
            os.makedirs(model_dir, exist_ok=True)

            if self.trainable:
                download_model(self.config, subscription, model_id, model_dir)
            
            start_time, end_time = self.get_data_time_range(parameters)
            if self.config.auto_data_retrieving:
                series = self.tsanaclient.get_timeseries_gw(parameters, parameters['seriesSets'], start_time, end_time)              
            else:
                series = None

            self.tsanaclient.save_inference_status(task_id, parameters, InferenceState.Running.name)
            result, values, message = self.do_inference(model_dir, parameters, series, Context(subscription, model_id, task_id))
            self.inference_callback(subscription, model_id, task_id, parameters, result, values, message)
        except Exception as e:
            self.inference_callback(subscription, model_id, task_id, parameters, STATUS_FAIL, None, str(e))
        finally:
            shutil.rmtree(model_dir, ignore_errors=True)

        total_time = (time.time() - start)
        log.duration("inference_task_duration", total_time, model_id=model_id, task_id=task_id, result=result, endpoint=parameters['apiEndpoint'], group_id=parameters['groupId'], group_name=parameters['groupName'].replace(' ', '_'), instance_id=parameters['instance']['instanceId'], instance_name=parameters['instance']['instanceName'].replace(' ', '_'))
        log.count("inference_task_count", 1,  model_id=model_id, task_id=task_id, result=result, endpoint=parameters['apiEndpoint'], group_id=parameters['groupId'], group_name=parameters['groupName'].replace(' ', '_'), instance_id=parameters['instance']['instanceId'], instance_name=parameters['instance']['instanceName'].replace(' ', '_'))
        gc.collect()
        
        return STATUS_SUCCESS, ''

    def train_callback(self, subscription, model_id, task_id, model_dir, parameters, model_state, last_error=None):
        try:
            meta = get_meta(self.config, subscription, model_id)
            if meta is None or meta['state'] == ModelState.Deleted.name:
                return STATUS_FAIL, 'Model is not found! '

            if model_state == ModelState.Ready:
                result, message = upload_model(self.config, subscription, model_id, model_dir)
                if result != STATUS_SUCCESS:
                    model_state = ModelState.Failed
                    last_error = 'Model storage failed! ' + message
        except Exception as e:
            model_state = ModelState.Failed
            last_error = str(e)
            raise e
        finally:
            update_state(self.config, subscription, model_id, model_state, None, last_error)
            self.tsanaclient.save_training_status(task_id, parameters, model_state.name, last_error)
            self.tsanaclient.save_training_result(parameters, model_id, model_state.name, last_error)
            error_message = last_error + '\n' + traceback.format_exc() if model_state != ModelState.Ready else None
            log.info("Training callback by %s, model_id = %s, task_id = %s, state = %s, last_error = %s" % (subscription, model_id, task_id, model_state, error_message if error_message is not None else ''))

    def inference_callback(self, subscription, model_id, task_id, parameters, result, values, last_error=None):
        try:
            if result == STATUS_SUCCESS and values != None:
                for value in values:
                    result, last_error = self.tsanaclient.save_data_points(parameters, value['metricId'], value['dimension'], value['timestamps'], value['values'], 
                                                                            value['fields'] if 'fields' in value else None, value['fieldValues'] if 'fieldValues' in value else None)
                    if result != STATUS_SUCCESS:
                        break
        except Exception as e:
            result = STATUS_FAIL
            last_error = str(e)
            raise e
        finally:
            if result == STATUS_SUCCESS:
                self.tsanaclient.save_inference_status(task_id, parameters, InferenceState.Ready.name)
            else:
                self.tsanaclient.save_inference_status(task_id, parameters, InferenceState.Failed.name, last_error)
            error_message = last_error + '\n' + traceback.format_exc() if result != STATUS_SUCCESS else None
            log.info("Inference callback by %s, model_id = %s, task_id = %s, result = %s, last_error = %s" % (subscription, model_id, task_id, result, error_message if error_message is not None else ''))

    def train(self, request):
        request_body = json.loads(request.data)
        instance_id = request_body['instance']['instanceId']
        if not self.trainable:
            return make_response(jsonify(dict(instanceId=instance_id, modelId='', taskId='', result=STATUS_SUCCESS, message='Model is not trainable', modelState=ModelState.Ready.name)), 200)

        subscription = request.headers.get('apim-subscription-id', 'Official')
        request_body[INSTANCE_ID_KEY] = subscription

        result, message = self.do_verify(request_body, Context(subscription, '', ''))
        if result != STATUS_SUCCESS:
            return make_response(jsonify(dict(instanceId=instance_id, modelId='', taskId='', result=STATUS_FAIL, message='Verify failed! ' + message, modelState=ModelState.Deleted.name)), 400)

        models_in_train = []
        for model in get_model_list(self.config, subscription):
            if 'instanceId' in model and model['instanceId'] == request_body['instance']['instanceId'] and (model['state'] == ModelState.Training.name or model['state'] == ModelState.Pending.name):
                models_in_train.append(model['modelId'])

        if len(models_in_train) >= self.config.models_in_training_limit_per_instance:
            return make_response(jsonify(dict(instanceId=instance_id, modelId='', taskId='', result=STATUS_FAIL, message='Models in training limit reached! Abort training this time.', modelState=ModelState.Deleted.name)), 400)

        log.info('Create training task')
        try:
            task_id = str(uuid.uuid1())
            if 'modelId' in request_body and request_body['modelId']:
                model_id = request_body['modelId']
            else:
                model_id = str(uuid.uuid1())
            insert_meta(self.config, subscription, model_id, request_body)
            meta = get_meta(self.config, subscription, model_id)

            job = JobRecord(task_id, JobRecord.MODE_TRAINING, self.__class__.__name__, model_id, subscription, request_body)
            send_message(self.training_topic, dict(job))
            log.count("training_task_throughput_in", 1, topic_name=self.training_topic, model_id=model_id, endpoint=request_body['apiEndpoint'], group_id=request_body['groupId'], group_name=request_body['groupName'].replace(' ', '_'), instance_id=request_body['instance']['instanceId'], instance_name=request_body['instance']['instanceName'].replace(' ', '_'))
            return make_response(jsonify(dict(instanceId=instance_id, modelId=model_id, taskId=task_id, result=STATUS_SUCCESS, message='Training task created', modelState=ModelState.Training.name)), 201)
        except Exception as e:
            meta = get_meta(self.config, subscription, model_id)
            error_message = str(e)
            if meta is not None: 
                update_state(self.config, subscription, model_id, ModelState.Failed, None, error_message)
            log.error("Create training task failed! subscription = %s, model_id = %s, task_id = %s, last_error = %s" % (subscription, model_id, task_id, error_message + '\n' + traceback.format_exc()))
            return make_response(jsonify(dict(instanceId=instance_id, modelId=model_id, taskId=task_id, result=STATUS_FAIL, message='Fail to create new task ' + error_message, modelState=ModelState.Failed.name)), 400)

    def inference(self, request, model_id):
        request_body = json.loads(request.data)
        instance_id = request_body['instance']['instanceId']
        subscription = request.headers.get('apim-subscription-id', 'Official')
        
        request_body[INSTANCE_ID_KEY] = subscription

        if self.trainable:
            meta = get_meta(self.config, subscription, model_id)
            if meta is None:
                return make_response(jsonify(dict(instanceId=instance_id, modelId=model_id, taskId='', result=STATUS_FAIL, message='Model is not found!', modelState=ModelState.Deleted.name)), 400)

            if meta['state'] != ModelState.Ready.name:
                return make_response(jsonify(dict(instanceId=instance_id, modelId=model_id, taskId='', result=STATUS_FAIL, message='Cannot do inference right now, status is ' + meta['state'], modelState=meta['state'])), 400)

            try:
                series_set = json.loads(meta['series_set'])
            except:
                series_set = json.loads(zlib.decompress(base64.b64decode(meta['series_set'].encode("ascii"))).decode('utf-8'))

            para = json.loads(meta['para'])

            current_set = json.dumps(series_set, sort_keys=True)
            current_params = json.dumps(para, sort_keys=True)

            new_set = json.dumps(request_body['seriesSets'], sort_keys=True)
            new_params = json.dumps(request_body['instance']['params'], sort_keys=True)

            if current_set != new_set or current_params != new_params:
                if self.need_retrain(series_set, para, request_body['seriesSets'], request_body['instance']['params'], Context(subscription, model_id, '')):
                    return make_response(jsonify(dict(instanceId=instance_id, modelId=model_id, taskId='', result=STATUS_FAIL, message='Inconsistent series sets or params!', modelState=meta['state'])), 400)

        log.info('Create inference task')
        task_id = str(uuid.uuid1())
        job = JobRecord(task_id, JobRecord.MODE_INFERENCE, self.__class__.__name__, model_id, subscription, request_body)
        send_message(self.inference_topic, dict(job))
        log.count("inference_task_throughput_in", 1, topic_name=self.inference_topic, model_id=model_id, endpoint=request_body['apiEndpoint'], group_id=request_body['groupId'], group_name=request_body['groupName'].replace(' ', '_'), instance_id=request_body['instance']['instanceId'], instance_name=request_body['instance']['instanceName'].replace(' ', '_'))

        return make_response(jsonify(dict(instanceId=instance_id, modelId=model_id, taskId=task_id, result=STATUS_SUCCESS, message='Inference task created', modelState=ModelState.Ready.name)), 201)

    def state(self, request, model_id):
        if not self.trainable:
            return make_response(jsonify(dict(instanceId='', modelId=model_id, taskId='', result=STATUS_SUCCESS, message='Model is not trainable', modelState=ModelState.Ready.name)), 200)

        try:
            subscription = request.headers.get('apim-subscription-id', 'Official')
            request_body = json.loads(request.data)
            request_body[INSTANCE_ID_KEY] = subscription
                
            meta = get_meta(self.config, subscription, model_id)
            if meta == None:
                return make_response(jsonify(dict(instanceId='', modelId=model_id, taskId='', result=STATUS_FAIL, message='Model is not found!', modelState=ModelState.Deleted.name)), 400)

            meta = clear_state_when_necessary(self.config, subscription, model_id, meta)
            return make_response(jsonify(dict(instanceId='', modelId=model_id, taskId='', result=STATUS_SUCCESS, message=meta['last_error'] if 'last_error' in meta else '', modelState=meta['state'])), 200)
        except Exception as e:
            error_message = str(e)
            log.error("Get model state failed! subscription = %s, model_id = %s, last_error = %s" % (subscription, model_id, error_message + '\n' + traceback.format_exc()))
            return make_response(jsonify(dict(instanceId='', modelId=model_id, taskId='', result=STATUS_FAIL, message=error_message, modelState=ModelState.Failed.name)), 400)
        
    def list_models(self, request):
        subscription = request.headers.get('apim-subscription-id', 'Official')
        return make_response(jsonify(get_model_list(self.config, subscription)), 200)

    def delete(self, request, model_id):
        if not self.trainable:
            return make_response(jsonify(dict(instanceId='', modelId=model_id, taskId='', result=STATUS_SUCCESS, message='Model is not trainable')), 200)

        try:
            subscription = request.headers.get('apim-subscription-id', 'Official')
            request_body = json.loads(request.data)
            request_body[INSTANCE_ID_KEY] = subscription
            instance_id = request_body['instance']['instanceId']
            result, message = self.do_delete(request_body, model_id)
            if result == STATUS_SUCCESS:
                update_state(self.config, subscription, model_id, ModelState.Deleted)
                return make_response(jsonify(dict(instanceId=instance_id, modelId=model_id, taskId='', result=STATUS_SUCCESS, message='Model {} has been deleted'.format(model_id), modelState=ModelState.Deleted.name)), 200)
            else:
                raise Exception(message)
        except Exception as e:
            error_message = str(e)
            log.error("Delete model failed! subscription = %s, model_id = %s, last_error = %s" % (subscription, model_id, error_message + '\n' + traceback.format_exc()))
            return make_response(jsonify(dict(instanceId='', modelId=model_id, taskId='', result=STATUS_FAIL, message=error_message, modelState=ModelState.Failed.name)), 400)

    def verify(self, request):
        request_body = json.loads(request.data)
        instance_id = request_body['instance']['instanceId']
        subscription = request.headers.get('apim-subscription-id', 'Official')
        request_body[INSTANCE_ID_KEY] = subscription

        try:
            result, message = self.do_verify(request_body, Context(subscription, '', ''))
            if result != STATUS_SUCCESS:
                return make_response(jsonify(dict(instanceId=instance_id, modelId='', taskId='', result=STATUS_FAIL, message='Verify failed! ' + message, modelState=ModelState.Deleted.name)), 400)
            else:
                return make_response(jsonify(dict(instanceId=instance_id, modelId='', taskId='', result=STATUS_SUCCESS, message='Verify successfully! ' + message, modelState=ModelState.Deleted.name)), 200)
        except Exception as e:
            error_message = str(e)
            log.error("Verify parameters failed! subscription = %s, instance_id = %s, last_error = %s" % (subscription, instance_id, error_message + '\n' + traceback.format_exc()))
            return make_response(jsonify(dict(instanceId=instance_id, modelId='', taskId='', result=STATUS_FAIL, message='Verify failed! ' + error_message, modelState=ModelState.Deleted.name)), 400)