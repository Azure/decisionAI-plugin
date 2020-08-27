import time
import traceback
from telemetry import log

from flask import Flask, request, g, jsonify, make_response
from flask_restful import Resource, Api

from .plugin_service import PluginService
from .util.constant import STATUS_SUCCESS, STATUS_FAIL

import logging

app = Flask(__name__)
api = Api(app)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def try_except(fn):
    def wrapped(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            error_message = str(e) + '\n' + traceback.format_exc()
            return make_response(jsonify(dict(result=STATUS_FAIL, message='Unknown error, please check your request. ' + error_message)), 502)
    return wrapped

@app.route('/', methods=['GET'])
def index():
    return "Welcome to TSANA Computing Platform"


@app.before_request
def before_request():
    g.start = time.time()

@app.after_request
def after_request(response):
    # TODO log here
    request_log = '\nRequest begin-----------------------------'
    request_log += '\n'
    request_log += '  url: ' + str(request.url)
    request_log += '\n'
    request_log += '  body: ' + str(request.data)
    request_log += '\n'
    request_log += '  response status: ' + str(response.status)
    request_log += '\n'
    request_log += '  response data: ' + str(response.data)
    request_log += '\n'
    request_log += 'Request end-----------------------------'
    log.info(request_log)

    total_time = (time.time() - g.start)
    url_rule = str(request.url_rule)
    url = str(request.path)
    status = str(response.status_code)
    log.duration("tsg_plugin_api_request_duration", total_time, url_rule=url_rule, url=url, response_code=status)
    log.count("tsg_plugin_api_request_count", 1, url_rule=url_rule, url=url, response_code=status)
    return response

class PluginModelIndexAPI(Resource):
    def put(self):
        return "Welcome to TSANA Computing Platform"

api.add_resource(PluginModelIndexAPI, '/')

class PluginModelAPI(Resource):  # The API class that handles a single user
    def __init__(self, plugin_service: PluginService):
        self.__plugin_service = plugin_service

    @try_except
    def get(self, model_id):
        return self.__plugin_service.state(request, model_id)

    @try_except
    def post(self, model_id):
        pass

    @try_except
    def put(self, model_id):
        pass

    @try_except
    def delete(self, model_id):
        return self.__plugin_service.delete(request, model_id)


class PluginModelTrainAPI(Resource):
    def __init__(self, plugin_service: PluginService):
        self.__plugin_service = plugin_service

    @try_except
    def post(self):
        return self.__plugin_service.train(request)


class PluginModelInferenceAPI(Resource):
    def __init__(self, plugin_service: PluginService):
        self.__plugin_service = plugin_service

    @try_except
    def post(self, model_id):
        return self.__plugin_service.inference(request, model_id)


class PluginModelParameterAPI(Resource):
    def __init__(self, plugin_service: PluginService):
        self.__plugin_service = plugin_service

    @try_except
    def post(self):
        return self.__plugin_service.verify(request)


class PluginModelListAPI(Resource):
    def __init__(self, plugin_service: PluginService):
        self.__plugin_service = plugin_service

    @try_except
    def get(self):
        return self.__plugin_service.list_models(request)

def api_init(plugin_name, plugin_service:PluginService):
    api.add_resource(PluginModelListAPI, '/{}/models'.format(plugin_name), resource_class_kwargs={'plugin_service': plugin_service})
    api.add_resource(PluginModelAPI, '/{}/models/<model_id>'.format(plugin_name), resource_class_kwargs={'plugin_service': plugin_service})
    api.add_resource(PluginModelTrainAPI, '/{}/models/train'.format(plugin_name), resource_class_kwargs={'plugin_service': plugin_service})
    api.add_resource(PluginModelInferenceAPI, '/{}/models/<model_id>/inference'.format(plugin_name), resource_class_kwargs={'plugin_service': plugin_service})
    api.add_resource(PluginModelParameterAPI, '/{}/parameters'.format(plugin_name), resource_class_kwargs={'plugin_service': plugin_service})