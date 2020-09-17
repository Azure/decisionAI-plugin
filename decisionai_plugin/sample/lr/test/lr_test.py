import os
import sys
from os import environ
import time
import json

environ['SERVICE_CONFIG_FILE'] = 'sample/lr/config/service_config.yaml'

from sample.lr.lr_plugin_service import LrPluginService
from common.plugin_model_api import api_init, app
from common.util.timeutil import str_to_dt
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
                                "seriesSetId": "681cd8a4-4bc9-491b-b2dd-1a588aae534e",
                                "metricId": "7cf6ad65-d127-4ca7-9921-3b5c6985fd2f",
                                "dimensionFilter": {
                                    "CapacityUnit": "PS2PR01CU001",
                                    "PerformanceCounterName": "TCPv4 Connections Established",
                                    "location": "PS2"
                                },
                                "enrichmentConfigs": [{
                                        "enrichmentName": "AnomalyDetection",
                                        "enrichmentConfigId": "9c2196ec-7955-4e88-a964-eb789d444fa3"
                                    }
                                ],
                                "metricMeta": {
                                    "granularityName": "Custom",
                                    "granularityAmount": 600,
                                    "datafeedId": "e1dffb01-3c9b-4e9e-bc34-8ce52b3c5b9a",
                                    "metricName": "Count",
                                    "datafeedName": "TCP-Connection",
                                    "dataStartFrom": "2020-08-01T00:00:00Z"
                                }
                            }, {
                                "seriesSetName": "TCP-Connection_Count",
                                "seriesSetId": "5bdeecca-552a-4950-80f5-dfd28e0fb7b1",
                                "metricId": "7cf6ad65-d127-4ca7-9921-3b5c6985fd2f",
                                "dimensionFilter": {
                                    "CapacityUnit": "CY4PR13CU002",
                                    "PerformanceCounterName": "TCPv4 Connections Established",
                                    "location": "CY4"
                                },
                                "enrichmentConfigs": [{
                                        "enrichmentName": "AnomalyDetection",
                                        "enrichmentConfigId": "9c2196ec-7955-4e88-a964-eb789d444fa3"
                                    }
                                ],
                                "metricMeta": {
                                    "granularityName": "Custom",
                                    "granularityAmount": 600,
                                    "datafeedId": "e1dffb01-3c9b-4e9e-bc34-8ce52b3c5b9a",
                                    "metricName": "Count",
                                    "datafeedName": "TCP-Connection",
                                    "dataStartFrom": "2020-08-01T00:00:00Z"
                                }
                            }
                        ],
                        "instance": {
                            "instanceName": "LinearRegression_Instance_1600262682811",
                            "instanceId": "9a9f325c-529d-4c47-a9bf-24d02d70f1ea",
                            "status": "Active",
                            "appId": "b7bd9c1e-3604-4483-9743-7a0798c7f5c8",
                            "appName": "LinearRegression",
                            "appDisplayName": "LinearRegression",
                            "remoteModelKey": "",
                            "remoteCandidateModelKey": "",
                            "params": {
                                "metricDeficiency": 0,
                                "tracebackWindow": 10
                            },
                            "target": {
                                "target": "JSON"
                            },
                            "hookIds": []
                        },
                        "groupId": "d6f24f36-ae78-4337-bbd8-7cc964a38747",
                        "groupName": "lr-test",
                        "startTime": "2020-09-15T00:00:00Z",
                        "endTime": "2020-09-16T00:00:00Z",
                        "apiKey": "d60e17bc-2a9e-4ee5-a937-9542b40124ef",
                        "apiEndpoint": "https://aidice-app-api.azurewebsites.net/",
                        "manually": true
                    }
                    '''
    response = client.post('/lr/models/0000/inference', data=request_json)

    time.sleep(10)

    while True:
        ready = True
        result, message, value = lr.tsanaclient.get_inference_result(json.loads(request_json))
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