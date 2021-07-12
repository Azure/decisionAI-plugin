import json
import math
import datetime
import os

from .util.constant import STATUS_SUCCESS, STATUS_FAIL
from .util.constant import USER_ADDR
from .util.constant import INGESTION_API, META_API, TSG_API, STORAGE_GW_API
from .util.constant import INSTANCE_ID_KEY
from .util.constant import INSTANCE_ID_PLACEHOLDER
from .util.constant import META_ENDPOINT, TSG_ENDPOINT, INGESTION_ENDPOINT, STORAGE_GW_MT_ENDPOINT_PATTERN, STORAGE_GW_ST_ENDPOINT_PATTERN
from .util.constant import IS_INTERNAL, IS_MT
from .util.retryrequests import RetryRequests
from .util.series import Series
from .util.timeutil import get_time_offset, str_to_dt, dt_to_str, get_time_list

from telemetry import log

import pandas as pd

REQUEST_TIMEOUT_SECONDS = 120

def get_field_idx(fields, target):
    for idx, field in enumerate(fields):
        if field == '__FIELD__.' + target:
            return idx

    raise Exception("Not found field {} in {}".format(target, ','.join(fields)))


class TSANAClient(object):
    def __init__(self, username=None, password=None, retrycount=3, retryinterval=1000):
        self.username = username
        self.password = password
        self.retryrequests = RetryRequests(retrycount, retryinterval)
        self.crt = os.environ['MA_CERT_CRT_PATH'] if 'MA_CERT_CRT_PATH' in os.environ else ''
        self.key = os.environ['MA_CERT_KEY_PATH'] if 'MA_CERT_KEY_PATH' in os.environ else ''

    def post(self, api_endpoint, instance_id, api_key, user, path, data):
        if not api_endpoint.startswith('http'):
            api_endpoint = "https://" + api_endpoint

        url = api_endpoint.rstrip('/') + path

        headers = {
            "x-api-key": api_key,
            "x-user": user,
            "X-Base-Address": api_endpoint,
            "Instance-Id": instance_id,
            "X-Action-Source": "Plugin",
            "Content-Type": "application/json"
        }

        if self.username and self.password:
            auth = (self.username, self.password)
        else:
            auth = None

        if self.crt and self.key:
            cert = (self.crt, self.key)
        else:
            cert = None

        try:
            r = self.retryrequests.post(url=url, headers=headers, auth=auth, data=json.dumps(data),
                                        timeout=REQUEST_TIMEOUT_SECONDS, cert=cert, verify=False)
            if r.status_code != 204:
                try:
                    return r.json()
                except ValueError as e:
                    return "ValueError: " + str(e) + " Content: " + r.content.decode('UTF-8')
        except Exception as e:
            raise Exception('TSANA service api "{}" failed, request:{}, {}'.format(url, json.dumps(data), str(e)))

    def put(self, api_endpoint, instance_id, api_key, user, path, data):
        if not api_endpoint.startswith('http'):
            api_endpoint = "https://" + api_endpoint

        url = api_endpoint.rstrip('/') + path

        headers = {
            "x-api-key": api_key,
            "x-user": user,
            "X-Base-Address": api_endpoint,
            "Instance-Id": instance_id,
            "X-Action-Source": "Plugin",
            "Content-Type": "application/json"
        }

        if self.username and self.password:
            auth = (self.username, self.password)
        else:
            auth = None

        if self.crt and self.key:
            cert = (self.crt, self.key)
        else:
            cert = None

        try:
            r = self.retryrequests.put(url=url, headers=headers, auth=auth, data=json.dumps(data),
                                        timeout=REQUEST_TIMEOUT_SECONDS, cert=cert, verify=False)
            if r.status_code != 204:
                try:
                    return r.json()
                except ValueError:
                    return r.content
        except Exception as e:
            raise Exception('TSANA service api "{}" failed, request:{}, {}'.format(url, json.dumps(data), str(e)))

    def get(self, api_endpoint, instance_id, api_key, user, path):
        if not api_endpoint.startswith('http'):
            api_endpoint = "https://" + api_endpoint

        url = api_endpoint.rstrip('/') + path

        headers = {
            "x-api-key": api_key,
            "x-user": user,
            "X-Base-Address": api_endpoint,
            "Instance-Id": instance_id,
            "X-Action-Source": "Plugin",
            "Content-Type": "application/json"
        }

        if self.username and self.password:
            auth = (self.username, self.password)
        else:
            auth = None

        if self.crt and self.key:
            cert = (self.crt, self.key)
        else:
            cert = None

        try:
            r = self.retryrequests.get(url=url, headers=headers, auth=auth, timeout=REQUEST_TIMEOUT_SECONDS,
                                       cert=cert, verify=False)
            try:
                return r.json()
            except ValueError:
                return r.content
        except Exception as e:
            raise Exception('TSANA service api "{}" failed, {}'.format(url, str(e)))

    ################ GATEWAY API ################

    # Query time series from TSANA
    # Parameters:
    #   parameters: a dict object which should includes
    #       apiEndpoint: api endpoint for specific user
    #       apiKey: api key for specific user
    #       groupId: groupId in TSANA, which is copied from inference request, or from the entity
    #   series_sets: Array of series set
    #   start_time: inclusive, the first timestamp to be query
    #   end_time: exclusive
    #   top: top sereis number per series set
    # Return: 
    #   An array of Series object
    #     Series include
    #       series_id: UUID
    #       dim: dimension dict for this series
    #       fields: 1-d string array, ['time', '__VAL__', '__FIELD__.ExpectedValue', '__FIELD__.IsAnomaly', '__FIELD__.PredictionValue', '__FIELD__.PredictionModelScore', '__FIELD__.IsSuppress', '__FIELD__.Period', '__FIELD__.CostPoint', '__FIELD__.Mean', '__FIELD__.STD', '__FIELD__.TrendChangeAnnotate', '__FIELD__.TrendChang...tateIgnore', '__FIELD__.AnomalyAnnotate', ...]
    #       value: 2-d array, [['2020-10-12T17:55:00Z', 1.0, None, None, None, None, None, None, None, None, None, None, None, None, ...]]
    def get_timeseries_gw(self, parameters, series_sets, start_time, end_time, top=20):

        if start_time > end_time:
            raise Exception('start_time should be less than or equal to end_time')
        
        instance_id = parameters[INSTANCE_ID_KEY]
        if IS_INTERNAL:
            if IS_MT:
                storage_gw_endpoint = STORAGE_GW_MT_ENDPOINT_PATTERN
                storage_gw_endpoint = storage_gw_endpoint.replace(INSTANCE_ID_PLACEHOLDER, instance_id)
            else:
                storage_gw_endpoint = STORAGE_GW_ST_ENDPOINT_PATTERN
            meta_endpoint = META_ENDPOINT
        else:
            storage_gw_endpoint = parameters['apiEndpointV2'] + STORAGE_GW_API
            meta_endpoint = parameters['apiEndpointV2'] + META_API

        end_str = dt_to_str(end_time)
        start_str = dt_to_str(start_time)

        multi_series_data = []
        total_point_num = 0
        loop = 0

        # Query each series's tag
        for data in series_sets:
            dim = {}
            if 'dimensionFilter' not in data:
                data['dimensionFilter'] = data['filters']

            for dimkey in data['dimensionFilter']:
                dim[dimkey] = [data['dimensionFilter'][dimkey]]

            skip = 0
            count = 0
            para = dict(metricId=data['metricId'], dimensionFilter=dim, activeSince=start_str)
            gran_info = (data['metricMeta']['granularityName'], data['metricMeta']['granularityAmount'])
            data_point_num_per_series = len(get_time_list(start_time, end_time, gran_info))
            series_limit_per_call = min(max(100000 // data_point_num_per_series, 1), 1000)

            while True:
                # Max data points per call is 100000
                ret = self.post(meta_endpoint, instance_id, parameters['apiKey'], parameters['groupId'] + USER_ADDR, '/metrics/' + data['metricId'] + '/series/query?$skip={}&$maxpagesize={}'.format(skip, series_limit_per_call), data=para)
                if len(ret['value']) == 0:
                    break

                series_list = []
                for s in ret['value']:
                    series = {}
                    series['metricsName'] = s['metricId']
                    series['begin'] = start_str
                    series['end'] = end_str
                    series['tagSet'] = s['dimension']
                    series['returnSeriesId'] = True
                    series_list.append(series)

                if len(series_list) > 0:                    
                    ret_data = self.post(storage_gw_endpoint, instance_id, parameters['apiKey'], parameters['groupId'] + USER_ADDR, '/api/query_series', data=series_list)
                    sub_multi_series_data = []
                    for factor in ret_data:
                        if len(factor['values']) <= 0:
                            continue

                        sub_multi_series_data.append(Series(factor['name'], factor['seriesId'], factor['tags'], factor['columns'], factor['values']))
                        total_point_num += len(factor['values'])
                        log.count("get_data_series_num", 1, endpoint=parameters['apiEndpoint'], group_id=parameters['groupId'], group_name=parameters['groupName'].replace(' ', '_'), instance_id=parameters['instance']['instanceId'], instance_name=parameters['instance']['instanceName'].replace(' ', '_'))
                        log.count("get_data_point_num", len(factor['values']), endpoint=parameters['apiEndpoint'], group_id=parameters['groupId'], group_name=parameters['groupName'].replace(' ', '_'), instance_id=parameters['instance']['instanceId'], instance_name=parameters['instance']['instanceName'].replace(' ', '_'))
                    
                    multi_series_data.extend(sub_multi_series_data)
                    count += len(sub_multi_series_data)

                    if count >= top:
                        log.warning(f"Current series num {count} for series set {json.dumps(data)} has reached limit {top}!")
                        break
                
                skip = skip + len(series_list)
                loop = loop + 1
                if loop % 10 == 0:
                    log.info(f"Loop times: {loop}, total series num: {len(multi_series_data)}, total points num {total_point_num}.")

            # Max data points limit is 4000000, about 400Mb
            if total_point_num >= 4000000:
                log.info("Reach total point number limit 4000000.")
                break

        if not len(multi_series_data):
            raise Exception("Series is empty")
        
        log.info(f"Get time series completed! Total series num: {len(multi_series_data)}, total points num {total_point_num}.")
        return multi_series_data

    ################ META API ################

    # To get the meta of a specific datafeed from TSANA
    # Parameters:
    #   parameters: a dict object which should includes
    #       apiEndpoint: api endpoint for specific user
    #       apiKey: api key for specific user
    #       groupId: groupId in TSANA, which is copied from inference request, or from the entity
    #   datafeed_id: an UUID string
    # Return:
    #   the meta of the specified datafeed, or None if there is something wrong. 
    def get_datafeed_meta(self, parameters, datafeed_id):
        return self.get(META_ENDPOINT if IS_INTERNAL else parameters['apiEndpointV2'] + META_API, parameters[INSTANCE_ID_KEY], parameters['apiKey'], parameters['groupId'] + USER_ADDR, '/datafeeds/' + datafeed_id)
        
    # To get the meta of a specific metric from TSANA
    # Parameters:
    #   parameters: a dict object which should includes
    #       apiEndpoint: api endpoint for specific user
    #       apiKey: api key for specific user
    #       groupId: groupId in TSANA, which is copied from inference request, or from the entity
    #   metric_id: an UUID string
    # Return:
    #   the meta of the specified metric, or None if there is something wrong. 
    def get_metric_meta(self, parameters, metric_id):
        return self.get(META_ENDPOINT if IS_INTERNAL else parameters['apiEndpointV2'] + META_API, parameters[INSTANCE_ID_KEY], parameters['apiKey'], parameters['groupId'] + USER_ADDR, '/metrics/' + metric_id + '/meta')

    # To get the dimension values of a specific dimension of a metric from TSANA
    # Parameters:
    #   parameters: a dict object which should includes
    #       apiEndpoint: api endpoint for specific user
    #       apiKey: api key for specific user
    #       groupId: groupId in TSANA, which is copied from inference request, or from the entity
    #   metric_id: an UUID string
    #   dimension_name: dimension name for specific metric with metric_id
    # Return:
    #   the dimension values of a specific dimension of a metric, or None if there is something wrong. 
    def get_dimesion_values(self, parameters, metric_id, dimension_name):
        dims = self.get(META_ENDPOINT if IS_INTERNAL else parameters['apiEndpointV2'] + META_API, parameters[INSTANCE_ID_KEY], parameters['apiKey'], parameters['groupId'] + USER_ADDR, '/metrics/' + metric_id + '/dimensions')
        if 'dimensions' in dims and dimension_name in dims['dimensions']:
            return dims['dimensions'][dimension_name]
        else:
            return None

    # Query time series from TSANA
    # Parameters:
    #   parameters: a dict object which should includes
    #       apiEndpoint: api endpoint for specific user
    #       apiKey: api key for specific user
    #       groupId: groupId in TSANA, which is copied from inference request, or from the entity
    #   series_sets: Array of series set
    #   start_time: inclusive, the first timestamp to be query
    #   end_time: exclusive
    #   top: top sereis number per series set
    # Return: 
    #   An array of Series object
    def get_timeseries(self, parameters, series_sets, start_time, end_time, top=20):

        if start_time > end_time:
            raise Exception('start_time should be less than or equal to end_time')

        end_str = dt_to_str(end_time)
        start_str = dt_to_str(start_time)

        multi_series_data = []
        total_point_num = 0
        loop = 0

        # Query each series's tag
        for data in series_sets:
            dim = {}
            if 'dimensionFilter' not in data:
                data['dimensionFilter'] = data['filters']

            for dimkey in data['dimensionFilter']:
                dim[dimkey] = [data['dimensionFilter'][dimkey]]

            skip = 0
            count = 0
            para = dict(metricId=data['metricId'], dimensionFilter=dim, activeSince=start_str)
            gran_info = (data['metricMeta']['granularityName'], data['metricMeta']['granularityAmount'])
            data_point_num_per_series = len(get_time_list(start_time, end_time, gran_info))
            series_limit_per_call = min(max(100000 // data_point_num_per_series, 1), 1000)

            while True:
                # Max data points per call is 100000
                ret = self.post(META_ENDPOINT if IS_INTERNAL else parameters['apiEndpointV2'] + META_API, parameters[INSTANCE_ID_KEY], parameters['apiKey'], parameters['groupId'] + USER_ADDR, '/metrics/' + data['metricId'] + '/series/query?$skip={}&$maxpagesize={}'.format(skip, series_limit_per_call), data=para)
                if len(ret['value']) == 0:
                    break

                series_list = []
                for s in ret['value']:
                    s['startTime'] = start_str
                    s['endTime'] = end_str
                    s['returnSeriesId'] = True
                    series_list.append(s)
                
                if len(series_list) > 0:                    
                    ret_data = self.post(META_ENDPOINT if IS_INTERNAL else parameters['apiEndpointV2'] + META_API, parameters[INSTANCE_ID_KEY], parameters['apiKey'], parameters['groupId'] + USER_ADDR, '/metrics/series/data', data=dict(value=series_list))
                    sub_multi_series_data = []
                    for factor in ret_data['value']:
                        if len(factor['values']) <= 0:
                            continue

                        sub_multi_series_data.append(Series(factor['id']['metricId'], factor['id']['seriesId'], factor['id']['dimension'], factor['fields'], factor['values']))
                        total_point_num += len(factor['values'])
                    
                    multi_series_data.extend(sub_multi_series_data)
                    count += len(sub_multi_series_data)

                    if count >= top:
                        log.warning(f"Current series num {count} for series set {json.dumps(data)} has reached limit {top}!")
                        break
                
                skip = skip + len(series_list)
                loop = loop + 1
                if loop % 10 == 0:
                    log.info(f"Loop times: {loop}, total series num: {len(multi_series_data)}, total points num {total_point_num}.")
                
            # Max data points limit is 4000000, about 400Mb
            if total_point_num >= 4000000:
                log.info("Reach total point number limit 4000000.")
                break

        if not len(multi_series_data):
            raise Exception("Series is empty")
        
        log.info(f"Get time series completed! Total series num: {len(multi_series_data)}, total points num {total_point_num}.")
        return multi_series_data

    # Get ranked dimensions
    # Parameters:
    #   parameters: a dict object which should includes
    #       apiEndpoint: api endpoint for specific user
    #       apiKey: api key for specific user
    #       groupId: groupId in TSANA, which is copied from inference request, or from the entity
    #   metric_id: uuid for metric
    #   dimensions: included dimensions
    #   start_time: inclusive, the first timestamp to be query
    #   top: max count for returned results
    #   skip: offset
    # Return:
    #   ranked series dimensions
    def rank_series(self, parameters, metric_id, dimensions, start_time, top=10, skip=0):
        url = f'/metrics/{metric_id}/rank-series'
        para = dict(dimensions=dimensions, count=top, startTime=start_time, skip=skip)
        return self.post(META_ENDPOINT if IS_INTERNAL else parameters['apiEndpointV2'] + META_API, parameters[INSTANCE_ID_KEY], parameters['apiKey'], parameters['groupId'] + USER_ADDR, url, data=para)

    ################ INGESTION API ################

    # Query data from original data source for datafeed which does not need ingestion
    # Parameters:
    #   parameters: a dict object which should includes
    #       apiEndpoint: api endpoint for specific user
    #       apiKey: api key for specific user
    #       groupId: groupId in TSANA, which is copied from inference request, or from the entity
    #   datafeed_id: datafeed id for source data
    #   start_time: inclusive, the first timestamp to be query
    #   end_time: exclusive
    # Return: 
    #   LoadSchemaResponse:
    #       SeriesNumber: long number of rows
    #       Schemas:columns, dict<string, string> which includes <column_name, column_type>
    #       PreviewData: list<dict<String, Object>> which includes <column_name, column_value>
    #       Err: error code
    def get_source_data(self, parameters, datafeed_id, start_time, end_time):
            
        if start_time > end_time:
            raise Exception('start_time should be less than or equal to end_time')

        body = {
                "datafeedId": datafeed_id,
                "startTime": dt_to_str(start_time),
                "endTime": dt_to_str(end_time)
            }

        ret_data = self.post(INGESTION_ENDPOINT if IS_INTERNAL else parameters['apiEndpointV2'] + INGESTION_API, parameters[INSTANCE_ID_KEY], parameters['apiKey'], parameters['groupId'] + USER_ADDR, '/loadSourceData', body)
        return ret_data

    # Save a inference result back to TSANA
    # Parameters: 
    #   parameters: a dict object which should includes
    #       apiEndpoint: api endpoint for specific user
    #       apiKey: api key for specific user
    #       groupId: groupId in TSANA, which is copied from inference request, or from the entity
    #   metric_id: an UUID string
    #   dimensions: a dict includes dimension name and value
    #   timestamps: an array of timestamps
    #   values: an array of inference result values
    #   fields: an array of field names
    #   field_values: an 2-d array of field values corresponding with fields
    #   push_data_type: 'DatabaseOnly'/'AnomalyDetection', default is 'AnomalyDetection'
    # Return:
    #   result: STATE_SUCCESS / STATE_FAIL
    #   message: description for the result
    def save_data_points(self, parameters, metric_id, dimensions, timestamps, values, fields=None, field_values=None, push_data_type='AnomalyDetection'):
        try: 
            if len(values) <= 0: 
                raise Exception('empty values')

            body = {
                "metricId": metric_id,
                "dimensions": dimensions,
                "timestamps": timestamps, 
                "values": values,
                "pushDataType": push_data_type
            }

            if fields and len(fields) > 0 and field_values and len(field_values) > 0:
                body['fields'] = fields
                body['fieldValues'] = field_values

            self.post(INGESTION_ENDPOINT if IS_INTERNAL else parameters['apiEndpointV2'] + INGESTION_API, parameters[INSTANCE_ID_KEY], parameters['apiKey'], parameters['groupId'] + USER_ADDR, '/pushData', body)
            return STATUS_SUCCESS, ''
        except Exception as e:
            return STATUS_FAIL, str(e)

    ################ TSG API ################

    # To get the detailed info of a specific group from TSANA
    # Parameters:
    #   parameters: a dict object which should includes
    #       apiEndpoint: api endpoint for specific user
    #       apiKey: api key for specific user
    #       groupId: groupId in TSANA, which is copied from inference request, or from the entity
    # Return:
    #   detailed info of the specified group 
    def get_group_detail(self, parameters):
        return self.get(TSG_ENDPOINT if IS_INTERNAL else parameters['apiEndpointV2'] + TSG_API, parameters[INSTANCE_ID_KEY], parameters['apiKey'], parameters['groupId'] + USER_ADDR, '/timeSeriesGroups/' + parameters['groupId'])

    # Save a training result back to TSANA
    # Parameters: 
    #   parameters: a dict object which should includes
    #       apiEndpoint: api endpoint for specific user
    #       apiKey: api key for specific user
    #       groupId: groupId in TSANA, which is copied from inference request, or from the entity
    #       instance: instance object, which is copied from the inference request, or from the entity
    #   model_id: model id
    #   model_state: model state(TRAINING,READY,FAILED,DELETED)
    #   message: detail message
    # Return:
    #   result: STATE_SUCCESS / STATE_FAIL
    #   message: description for the result 
    def save_training_result(self, parameters, model_id, model_state:str, message:str):
        try:
            body = {
                'modelId': model_id, 
                'state': model_state, 
                'message': message
            }

            self.put(TSG_ENDPOINT if IS_INTERNAL else parameters['apiEndpointV2'] + TSG_API, parameters[INSTANCE_ID_KEY], parameters['apiKey'], parameters['groupId'] + USER_ADDR, '/timeSeriesGroups/' + parameters['groupId'] + '/appInstances/' + parameters['instance']['instanceId'] + '/modelKey', body)
            return STATUS_SUCCESS, ''
        except Exception as e:
            return STATUS_FAIL, str(e)

    # Save a inference result back to TSANA
    # Parameters: 
    #   parameters: a dict object which should includes
    #       apiEndpoint: api endpoint for specific user
    #       apiKey: api key for specific user
    #       groupId: groupId in TSANA, which is copied from inference request, or from the entity
    #       instance: instance object, which is copied from the inference request, or from the entity
    #   result: an array of inference result. 
    # Return:
    #   result: STATE_SUCCESS / STATE_FAIL
    #   message: description for the result
    def save_inference_result(self, parameters, result, batch_size=1000):
        try:
            if len(result) <= 0: 
                return STATUS_SUCCESS, ''

            for batch_index in range(math.ceil(len(result) / batch_size)):
                body = {
                'groupId': parameters['groupId'], 
                'instanceId': parameters['instance']['instanceId'], 
                'results': []
                }
                batch_start = batch_index * batch_size
                for step in range(min(batch_size, len(result) - batch_start)):
                    item = result[batch_start + step]
                    item['timestamp'] = dt_to_str(str_to_dt(item['timestamp']))
                    body['results'].append({
                        'params': parameters['instance']['params'],
                        'timestamp': item['timestamp'],
                        'result': item['value'],
                        'status': item['status']
                    })
                self.post(TSG_ENDPOINT if IS_INTERNAL else parameters['apiEndpointV2'] + TSG_API, parameters[INSTANCE_ID_KEY], parameters['apiKey'], parameters['groupId'] + USER_ADDR, '/timeSeriesGroups/' + parameters['groupId'] + '/appInstances/' + parameters['instance']['instanceId'] + '/saveResult', body)
            return STATUS_SUCCESS, ''
        except Exception as e:
            return STATUS_FAIL, str(e)

    # Save training task status back to TSANA
    # Parameters: 
    #   parameters: a dict object which should includes
    #       apiEndpoint: api endpoint for specific user
    #       apiKey: api key for specific user
    #       groupId: groupId in TSANA, which is copied from inference request, or from the entity
    #       instance: instance object, which is copied from the inference request, or from the entity
    #   status: ModelState:Pending, Training, Ready, Deleted, Failed
    #   last_error: last error message
    # Return:
    #   result: STATE_SUCCESS / STATE_FAIL
    #   message: description for the result
    def save_training_status(self, task_id, parameters, status, last_error=None):
        try: 
            context = {}
            context['groupId'] = parameters['groupId']
            context['groupName'] = parameters['groupName']
            context['instanceId'] = parameters['instance']['instanceId']
            context['instanceName'] = parameters['instance']['instanceName']
            context['startTime'] = parameters['startTime']
            context['endTime'] = parameters['endTime']

            body = {
                'taskId': task_id,
                'operation': 'Train',
                'context': context,
                'status': status,
                'lastError': last_error if last_error is not None else ''
                }
               
            self.post(TSG_ENDPOINT if IS_INTERNAL else parameters['apiEndpointV2'] + TSG_API, parameters[INSTANCE_ID_KEY], parameters['apiKey'], parameters['groupId'] + USER_ADDR, '/timeSeriesGroups/' + parameters['groupId'] + '/appInstances/' + parameters['instance']['instanceId'] + '/ops', body)
            return STATUS_SUCCESS, ''
        except Exception as e:
            log.warning(f"Save training status failed. taskId: {task_id}, error: {str(e)}")
            return STATUS_FAIL, str(e)

    # Save inference task status back to TSANA
    # Parameters: 
    #   parameters: a dict object which should includes
    #       apiEndpoint: api endpoint for specific user
    #       apiKey: api key for specific user
    #       groupId: groupId in TSANA, which is copied from inference request, or from the entity
    #       instance: instance object, which is copied from the inference request, or from the entity
    #   status: InferenceState:Pending, Running, Ready, Failed
    #   last_error: last error message
    # Return:
    #   result: STATE_SUCCESS / STATE_FAIL
    #   message: description for the result
    def save_inference_status(self, task_id, parameters, status, last_error=None):
        try: 
            if not parameters['manually']:
                return STATUS_SUCCESS, ''
                
            context = {}
            context['groupId'] = parameters['groupId']
            context['groupName'] = parameters['groupName']
            context['instanceId'] = parameters['instance']['instanceId']
            context['instanceName'] = parameters['instance']['instanceName']
            context['startTime'] = parameters['startTime']
            context['endTime'] = parameters['endTime']

            body = {
                'taskId': task_id,
                'operation': 'Inference',
                'context': context,
                'status': status,
                'lastError': last_error if last_error is not None else ''
                }
               
            self.post(TSG_ENDPOINT if IS_INTERNAL else parameters['apiEndpointV2'] + TSG_API, parameters[INSTANCE_ID_KEY], parameters['apiKey'], parameters['groupId'] + USER_ADDR, '/timeSeriesGroups/' + parameters['groupId'] + '/appInstances/' + parameters['instance']['instanceId'] + '/ops', body)
            return STATUS_SUCCESS, ''
        except Exception as e:
            log.warning(f"Save inference status failed. taskId: {task_id}, error: {str(e)}")
            return STATUS_FAIL, str(e)

    # Get inference result from TSANA
    # Parameters: 
    #   parameters: a dict object which should includes
    #       apiEndpoint: api endpoint for specific user
    #       apiKey: api key for specific user
    #       groupId: groupId in TSANA, which is copied from inference request, or from the entity
    #       instance: instance object, which is copied from the inference request, or from the entity
    # Return:
    #   result: STATE_SUCCESS / STATE_FAIL
    #   message: description for the result
    #   value: inference result array
    def get_inference_result(self, parameters):
        try: 
            ret = self.get(TSG_ENDPOINT if IS_INTERNAL else parameters['apiEndpointV2'] + TSG_API, parameters[INSTANCE_ID_KEY], parameters['apiKey'], parameters['groupId'] + USER_ADDR, '/timeSeriesGroups/' 
                                + parameters['groupId'] 
                                + '/appInstances/' 
                                + parameters['instance']['instanceId'] 
                                + '/history?startTime=' 
                                + parameters['startTime']
                                + '&endTime=' 
                                + parameters['endTime'])
            
            return STATUS_SUCCESS, '', ret
        except Exception as e:
            return STATUS_FAIL, str(e), None

    # Push alert in general
    # Parameters:
    #   parameters: a dict object which should includes
    #       apiEndpoint: api endpoint for specific user
    #       apiKey: api key for specific user
    #       groupId: groupId in TSANA, which is copied from inference request, or from the entity
    #   alert_type: alert type
    #   message: alert message
    # Return:
    #   result: STATE_SUCCESS / STATE_FAIL 
    #   message: description for the result
    def push_alert(self, parameters, alert_type, message):
        try:
            url = '/timeSeriesGroups/alert'
            para = dict(alertType=alert_type, message=message)
            self.post(TSG_ENDPOINT if IS_INTERNAL else parameters['apiEndpointV2'] + TSG_API, parameters[INSTANCE_ID_KEY], parameters['apiKey'], parameters['groupId'] + USER_ADDR, url, data=para)
            return STATUS_SUCCESS, ''
        except Exception as e:
            return STATUS_FAIL, repr(e)


