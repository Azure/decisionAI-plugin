import os
import sys
from os import environ
import time
import json

environ['SERVICE_CONFIG_FILE'] = 'sample/lr/config/service_config.yaml'

from sample.lr.lr_plugin_service import LrPluginService
from sample.util.request_generator import generate_request
from common.plugin_model_api import api_init, app
from common.util.timeutil import str_to_dt, dt_to_str
from common.util.constant import STATUS_SUCCESS, STATUS_FAIL, InferenceState

if __name__ == '__main__':
    
    lr = LrPluginService()
    api_init('lr', lr)
    app.testing = True
    client = app.test_client()
    response = client.get('/')

    request_json = '''
                    {
                        "seriesSets": [{
                                "seriesSetName": "TCP-Connection_Count",
                                "seriesSetId": "e24d1ec6-86c9-4db8-a5da-6a97f09fab6c",
                                "metricId": "074cb13a-ac9d-4aa6-9a76-117304dcb18f",
                                "dimensionFilter": {
                                    "CapacityUnit": "AM0P107CU002",
                                    "PerformanceCounterName": "TCPv4 Connections Established",
                                    "location": "AM0"
                                },
                                "enrichmentConfigs": [{
                                        "enrichmentName": "AnomalyDetection",
                                        "enrichmentConfigId": "f759cbec-4592-445c-b3e9-ea93aabc55b5"
                                    }
                                ],
                                "metricMeta": {
                                    "granularityName": "Custom",
                                    "granularityAmount": 600,
                                    "datafeedId": "dbf277ec-ae4d-4a8d-af30-9cd5c4c0c504",
                                    "metricName": "Count",
                                    "datafeedName": "TCP-Connection",
                                    "dataStartFrom": "2020-08-01T00:00:00Z"
                                }
                            }
                        ],
                        "instance": {
                            "instanceName": "LinearRegression-TSDB_Instance_1602507635734",
                            "instanceId": "f0707011-fd5d-4560-94e4-b3c205d6b111",
                            "status": "Active",
                            "appId": "1e068f40-ecaa-4f6f-ba7c-32b146dbd6f3",
                            "appName": "LinearRegression-TSDB",
                            "appDisplayName": "LinearRegression-TSDB",
                            "remoteModelKey": "",
                            "remoteCandidateModelKey": "",
                            "params": {
                                "metricDeficiency": 0,
                                "tracebackWindow": 20
                            },
                            "target": {
                                "actionLinkTemplate": "",
                                "admins": ["chuwan@microsoft.com", "d9ae9583-d7e5-440a-bf38-d3bcce117d04@metricsadvisor.ai"],
                                "authenticationType": "Basic",
                                "createdTime": 1602507638000,
                                "creator": "chuwan@microsoft.com",
                                "dataSourceType": "API",
                                "dataStartFrom": 1602547200000,
                                "datafeedDescription": "",
                                "datafeedId": "33a1d98b-adb2-478b-b273-5c50e5a9ca04",
                                "datafeedName": "Data Feed for LinearRegression-TSDB_Instance_1602507635734",
                                "dimensions": [{
                                        "dimensionDisplayName": "seriesId",
                                        "dimensionName": "seriesId"
                                    }
                                ],
                                "fillMissingPointForAd": "PreviousValue",
                                "fillMissingPointForAdValue": 0.0,
                                "granularityName": "Daily",
                                "ingestionType": "Single",
                                "isAdmin": true,
                                "maxConcurrency": -1,
                                "maxQueryPerMinute": 30.0,
                                "metrics": [{
                                        "derivedScript": "",
                                        "metricDescription": "",
                                        "metricDisplayName": "LrValue",
                                        "metricId": "428f5f7c-ebd7-400a-a83c-c6e9ab5291cd",
                                        "metricName": "LrValue",
                                        "metricType": "NORMAL"
                                    }
                                ],
                                "migrationType": 0,
                                "minRetryIntervalInSeconds": -1,
                                "needRollup": "RollupByUser",
                                "parameterList": [],
                                "rollUpColumns": "",
                                "rollUpMethod": "None",
                                "startOffsetInSeconds": 0,
                                "status": "Active",
                                "stopRetryAfterInSeconds": -1,
                                "target": "TSDB",
                                "timestampColumn": "timestamp",
                                "viewMode": "Private",
                                "viewers": []
                            },
                            "hookIds": []
                        },
                        "groupId": "d9ae9583-d7e5-440a-bf38-d3bcce117d04",
                        "groupName": "lr-tsdb-test",
                        "startTime": "2020-10-09T00:00:00Z",
                        "endTime": "2020-10-10T00:00:00Z",
                        "apiKey": "fd96d9a4-d21b-4533-b695-47400630ca60",
                        "apiEndpoint": "https://aidice-t2-api.azurewebsites.net/",
                        "manually": true
                    }
                    '''

    request_body = json.loads(request_json)

    # generate request sample
    request_sample = generate_request(lr, request_body['apiEndpoint'], request_body['apiKey'], request_body['groupId'], request_body['instance']['instanceId'], request_body['startTime'], request_body['endTime'])

    #do inference
    response = client.post('/lr/models/0000/inference', data=request_json)
    time.sleep(10)

    #get inference Json result
    while True:
        ready = True
        result, message, value = lr.tsanaclient.get_inference_result(request_sample)
        if result != STATUS_SUCCESS:
            ready = False
            print("Get inference result failed, message: " + message)
            break
        
        for item in value['value']:
            if item['status'] == 'Running':
                ready = False
                break

        if not ready:
            print("Not ready, wait for 5 seconds...")
            time.sleep(5)
        else:
            break
    
    if ready:
        print("Inference ready now, result is:")
        print(value)

    #get inference TSDB result
    start_time, end_time, gran = lr.get_inference_time_range(request_sample)
    series_set = []
    dedup = {}
    for data in request_sample['seriesSets']:
            dim = {}
            if 'dimensionFilter' not in data:
                data['dimensionFilter'] = data['filters']

            for dimkey in data['dimensionFilter']:
                dim[dimkey] = [data['dimensionFilter'][dimkey]]

            ret = lr.tsanaclient.rank_series(request_sample['apiEndpoint'], request_sample['apiKey'], data['metricId'], dim, dt_to_str(start_time), top=10)        
            for s in ret['value']:
                if s['seriesId'] not in dedup:
                    s['metricMeta'] = data['metricMeta']
                    series_set.append(s)
                    dedup[s['seriesId']] = True

    for series in series_set:
        series_def = dict(metricId=request_sample['instance']['target']['metrics'][0]['metricId'], dimensionFilter=dict(seriesId=series['seriesId']), metricMeta=series['metricMeta'])
        factors_data = lr.tsanaclient.get_timeseries(request_sample['apiEndpoint'], request_sample['apiKey'], [series_def],
                                                       start_time, end_time, offset=0,
                                                       top=100)
        for factor in factors_data:
            print(json.dumps(factor.__dict__))