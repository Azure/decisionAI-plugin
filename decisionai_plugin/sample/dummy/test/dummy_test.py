import os
import sys
from os import environ
import time
import json

#sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir))

environ['SERVICE_CONFIG_FILE'] = 'sample/dummy/config/service_config.yaml'

from sample.dummy.dummy_plugin_service import DummyPluginService
from common.plugin_model_api import api_init, app
from common.util.timeutil import str_to_dt

if __name__ == '__main__':
    
    dummy = DummyPluginService()
    api_init('dummy', dummy)
    app.testing = True
    client = app.test_client()
    response = client.get('/')

    '''
    {
        "groupId": "66f75e07-7c9b-4a52-947a-1a3880b418cb",
        "apiEndpoint": "https://AIDice-T2-api.azurewebsites.net/",
        "apiKey": "2812e4ea-ce22-4283-b24d-73333893b240",
        "name":"my-test","seriesSets":[{"seriesSetName":"localCSV_test2_Totalvalue","seriesSetId":"7e7954a5-7716-414b-ba13-2cc0ca644f03","metricId":"9e5bf248-f016-43ea-835e-fa20bcf67a95","dimensionFilter":{},"enrichmentConfigs":[{"enrichmentName":"AnomalyDetection","enrichmentConfigId":"d10a392f-c2c0-4204-94ee-ece960072bf1"}],"metricMeta":{"granularityName":"Daily","granularityAmount":0,"datafeedId":"b6245c1f-b4e4-4294-a707-8f30697978a2","metricName":"Totalvalue","datafeedName":"localCSV_test2","dataStartFrom":"2019-03-17T00:00:00+00:00"}}
        ],
        "instance": {
            "instanceName": "Inconsistency Detection_Instance_1601398919427",
            "instanceId": "88e39eee-02c3-480e-927a-4f32dd16e570",
            "status": "Active",
            "appId": "6ea3b8a4-80b3-4fb7-adcd-25e32db6b798",
            "appName": "Inconsistency",
            "appDisplayName": "Inconsistency Detection",
            "appType": "Internal",
            "remoteModelKey": "",
            "remoteCandidateModelKey": "",
            "params": {
                "detectionWindow": 100,
                "shift": false,
                "metricDeficiency": 0,
                "varNaRatioThreshold": 0.3,
                "maxHistoryInDays": 180,
                "tracebackWindow": 0
            },
            "target": {
                "target": "JSON"
            },
            "hookIds": []
        },
        "startTime": "2019-03-17T00:00:00Z",
        "endTime": "2019-06-10T00:00:00Z",
        "manually": true
    }
    '''
    request_json = '''
                    {
                        "groupId": "66f75e07-7c9b-4a52-947a-1a3880b418cb",
                        "apiEndpoint": "https://AIDice-T2-api.azurewebsites.net/",
                        "apiKey": "2812e4ea-ce22-4283-b24d-73333893b240",
                        "name":"my-test",
                        "seriesSets":[{
                            "seriesSetName":"Gandalf_test_multi_v3_TotalIOsGt30s",
                            "seriesSetId":"6b0044a1-e0f7-41cc-9bbf-e142af010981",
                            "metricId":"96542384-5aac-4d1b-9189-f985cbfb8e84",
                            "dimensionFilter":{
                            },
                            "enrichmentConfigs":[{
                                "enrichmentName":"AnomalyDetection",
                                "enrichmentConfigId":"29ac460e-6b03-49e1-a593-4d04766a987a"
                            }],
                            "metricMeta":{
                                "granularityName":"Custom",
                                "granularityAmount":300,
                                "datafeedId":"3e9f2e86-a5ef-44d3-bcf1-55c973e2db65",
                                "metricName":"TotalIOsGt30s",
                                "datafeedName":"Gandalf_test_multi_v3",
                                "dataStartFrom":"2020-08-23T00:00:00+00:00"
                            }
                        }],
                        "instance": {
                            "instanceName": "Inconsistency Detection_Instance_1601398919427",
                            "instanceId": "88e39eee-02c3-480e-927a-4f32dd16e570",
                            "status": "Active",
                            "appId": "6ea3b8a4-80b3-4fb7-adcd-25e32db6b798",
                            "appName": "Inconsistency",
                            "appDisplayName": "Inconsistency Detection",
                            "appType": "Internal",
                            "remoteModelKey": "",
                            "remoteCandidateModelKey": "",
                            "params": {
                                "detectionWindow": 100,
                                "shift": false,
                                "metricDeficiency": 0,
                                "varNaRatioThreshold": 0.3,
                                "maxHistoryInDays": 180,
                                "tracebackWindow": 0
                            },
                            "target": {
                                "target": "JSON"
                            },
                            "hookIds": []
                        },
                        "startTime": "2020-10-12T00:00:00Z",
                        "endTime": "2020-10-12T21:15:00Z",
                        "manually": true
                    }
                    '''

    #do inference
    for i in range(30):
        response = client.post('/dummy/models/0000/inference', data=request_json)
        time.sleep(120)
    
    #response = client.post('/dummy/models/train', data=request_json)
    #time.sleep(10)
    #response = client.post('/dummy/models/train', data=request_json)
    #response = client.post('/dummy/models/7cbb3a50-dc7a-11ea-a0bb-000d3af88183/inference', data=request_json)
    #response = client.get('/dummy/models/b06f99c6-d186-11ea-a12e-000d3af88183')
    #response = client.get('/dummy/models')
    #time.sleep(1000)


    
    
