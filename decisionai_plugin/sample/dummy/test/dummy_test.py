import os
import sys
from os import environ
import time
import json

#sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir))

environ['SERVICE_CONFIG_FILE'] = 'sample/dummy/config/service_config.yaml'

#environ['TELEMETRY_TYPE'] = 'mon3'
environ['MON3_SERVER'] = 'ks2-log-dev.westus2.cloudapp.azure.com:5201'
environ['KENSHO2_PROFILE'] = 'Plugin-Service-Prod'
environ['MON3_APP'] = 'plugin-maga'
environ['MON3_SERVICE'] = 'plugin-service'

from sample.dummy.dummy_plugin_service import DummyPluginService
from decisionai_plugin.common.plugin_model_api import api_init, app
from decisionai_plugin.common.util.timeutil import str_to_dt

if __name__ == '__main__':
    
    dummy = DummyPluginService()
    api_init(dummy)
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
                        "startTime": "2020-10-11T00:00:00Z",
                        "endTime": "2020-11-12T21:15:00Z",
                        "manually": true
                    }
                    '''
    request_json_2 = '''
                    {
                        "apiEndpoint": "https://aidice-t2-api.azurewebsites.net/",
                        "apiKey": "e83abde1-e085-4c2d-9f03-ef61abc98e6e",
                        "endTime": "2020-11-19T00:00:00Z",
                        "groupId": "02c3d5a2-5aa3-441d-a5d8-aeea37add582",
                        "groupName": "aidice-mismatch-test",
                        "instance": {
                            "appDisplayName": "LinearRegression",
                            "appId": "1b7062a5-e624-498a-ae36-49e7ff961d7a",
                            "appName": "LinearRegression",
                            "hookIds": [],
                            "instanceId": "23e69635-5696-437b-bcc3-1d0d9e399e0a",
                            "instanceName": "LinearRegression_Instance_1605634501962",
                            "params": {
                                "metricDeficiency": 0,
                                "tracebackWindow": 20
                            },
                            "remoteCandidateModelKey": "",
                            "remoteModelKey": "",
                            "status": "Active",
                            "target": {
                                "target": "JSON"
                            }
                        },
                        "manually": true,
                        "seriesSets": [{
                                "dimensionFilter": {
                                    "NmAgentBuildInfo": "3.301.5.54",
                                    "NodeHardware_ClusterType": "Compute",
                                    "NodeHardware_Generation": "4.1",
                                    "NodeHardware_HwSkuId": "HP_Gen4.1_C1030H",
                                    "OSHostPlugin": "144.0.10.55.OsHostPlugin-rel_m3bmc_1654.190625-1317.zip",
                                    "OsType": "Windows_IaaS",
                                    "RCALevel1": "FPGA",
                                    "VMSize": "Standard_A4_v2"
                                },
                                "enrichmentConfigs": [{
                                        "enrichmentConfigId": "f1ea4b37-6811-4949-90a9-fe1ad948f8c7",
                                        "enrichmentName": "AnomalyDetection"
                                    }
                                ],
                                "metricId": "39433f50-aec5-400f-89d1-688c05fb5a5a",
                                "metricMeta": {
                                    "dataStartFrom": "2020-09-29T19:00:00Z",
                                    "datafeedId": "355639e3-8ce3-4780-8159-315b2afca316",
                                    "datafeedName": "AirNMAgentUpdateEvents_v0",
                                    "granularityAmount": 0,
                                    "granularityName": "Hourly",
                                    "metricName": "NoOfVMs"
                                },
                                "seriesSetId": "16e64ab6-3723-43ba-bcc3-65d530387da6",
                                "seriesSetName": "AirNMAgentUpdateEvents_v0_NoOfVMs"
                            }, {
                                "dimensionFilter": {
                                    "NmAgentBuildInfo": "3.301.5.54",
                                    "NodeHardware_ClusterType": "Compute",
                                    "NodeHardware_Generation": "4.1",
                                    "NodeHardware_HwSkuId": "HP_Gen4.1_C1030H",
                                    "OSHostPlugin": "144.0.10.55.OsHostPlugin-rel_m3bmc_1654.190625-1317.zip",
                                    "OsType": "Windows_IaaS",
                                    "RCALevel1": "FPGA",
                                    "VMSize": "Standard_A1_v2"
                                },
                                "enrichmentConfigs": [{
                                        "enrichmentConfigId": "f1ea4b37-6811-4949-90a9-fe1ad948f8c7",
                                        "enrichmentName": "AnomalyDetection"
                                    }
                                ],
                                "metricId": "39433f50-aec5-400f-89d1-688c05fb5a5a",
                                "metricMeta": {
                                    "dataStartFrom": "2020-09-29T19:00:00Z",
                                    "datafeedId": "355639e3-8ce3-4780-8159-315b2afca316",
                                    "datafeedName": "AirNMAgentUpdateEvents_v0",
                                    "granularityAmount": 0,
                                    "granularityName": "Hourly",
                                    "metricName": "NoOfVMs"
                                },
                                "seriesSetId": "260c7b90-652d-40ae-b606-3b74cb17303d",
                                "seriesSetName": "AirNMAgentUpdateEvents_v0_NoOfVMs"
                            }, {
                                "dimensionFilter": {
                                    "NmAgentBuildInfo": "3.301.5.54",
                                    "NodeHardware_ClusterType": "Compute",
                                    "NodeHardware_Generation": "4.1",
                                    "NodeHardware_HwSkuId": "HP_Gen4.1_C1030H",
                                    "OSHostPlugin": "144.0.10.55.OsHostPlugin-rel_m3bmc_1654.190625-1317.zip",
                                    "OsType": "Windows_IaaS",
                                    "RCALevel1": "FPGA",
                                    "VMSize": "Standard_A2_v2"
                                },
                                "enrichmentConfigs": [{
                                        "enrichmentConfigId": "f1ea4b37-6811-4949-90a9-fe1ad948f8c7",
                                        "enrichmentName": "AnomalyDetection"
                                    }
                                ],
                                "metricId": "39433f50-aec5-400f-89d1-688c05fb5a5a",
                                "metricMeta": {
                                    "dataStartFrom": "2020-09-29T19:00:00Z",
                                    "datafeedId": "355639e3-8ce3-4780-8159-315b2afca316",
                                    "datafeedName": "AirNMAgentUpdateEvents_v0",
                                    "granularityAmount": 0,
                                    "granularityName": "Hourly",
                                    "metricName": "NoOfVMs"
                                },
                                "seriesSetId": "3121be5e-6d2e-481d-9a05-a0b75648c823",
                                "seriesSetName": "AirNMAgentUpdateEvents_v0_NoOfVMs"
                            }
                        ],
                        "startTime": "2020-11-15T00:00:00Z"
                    }
                    '''

    request_json = '''
    {
        "apiEndpoint": "https://AIDice-T2-api.azurewebsites.net/",
        "apiKey": "2365d22c-a738-41de-8e8e-7be553e826fc",
        "endTime": "2020-12-11T22:55:00Z",
        "groupId": "07514da3-1b93-4cfe-b9dd-177c3e4eb10c",
        "groupName": "e2e_stream_15_gran",
        "instance": {
            "appDisplayName": "AiDice_AKS",
            "appId": "e707b0e4-dad5-45cc-b931-43a29ef16997",
            "appName": "AiDice_AKS",
            "appType": "External",
            "hookIds": ["f07e6f43-248b-4377-a26e-6adcfd01f97a"],
            "instanceId": "95ef1b2e-676f-4065-9dd8-667ddbca7f1d",
            "instanceName": "AiDice_AKS_Instance_1607724790777",
            "params": {
                "Blackout": "",
                "bmsSize": 50,
                "ifEarlyAbort": true,
                "kustoAuthId": "",
                "kustoClientId": "",
                "kustoClientSec": "",
                "kustoCluster": "",
                "kustoDb": "",
                "kustoIngestion": false,
                "kustoValCol": "",
                "lookbackDays": 3,
                "maxOutputChars": 60000,
                "maxOutputRows": 3,
                "maxPatternLength": 4,
                "maxRetrievalRows": 200000,
                "maxSearchResult": 20,
                "maxSearchTime": 180,
                "metricDeficiency": 0,
                "randomProb": 0.5,
                "removeColumns": "[]",
                "runInterval": 900,
                "scheduledRun": true,
                "tabuSize": 50,
                "totalSteps": 5000
            },
            "remoteCandidateModelKey": "",
            "remoteModelKey": "",
            "status": "Active",
            "target": {
                "actionLinkTemplate": "",
                "admins": ["chuwan@microsoft.com", "bennyng@microsoft.com", "giantoni@microsoft.com", "nguyenm@microsoft.com", "conhua@microsoft.com", "mingzhao@microsoft.com", "07514da3-1b93-4cfe-b9dd-177c3e4eb10c@metricsadvisor.ai", "abasok@microsoft.com"],
                "authenticationType": "Basic",
                "createdTime": 1607724804000,
                "creator": "giantoni@microsoft.com",
                "dataSourceType": "API",
                "dataStartFrom": 1607727600000,
                "datafeedDescription": "",
                "datafeedId": "42776dc4-70cf-4c76-b40f-381aad27a622",
                "datafeedName": "Data Feed for AiDice_AKS_Instance_1607724790777",
                "dimensions": [{
                        "dimensionDisplayName": "hashKey",
                        "dimensionName": "hashKey"
                    }
                ],
                "fillMissingPointForAd": "PreviousValue",
                "fillMissingPointForAdValue": 0.0,
                "granularityName": "Hourly",
                "ingestionType": "Single",
                "isAdmin": true,
                "maxConcurrency": -1,
                "maxQueryPerMinute": 30.0,
                "metrics": [{
                        "derivedScript": "",
                        "metricDescription": "",
                        "metricDisplayName": "EvaluationScore",
                        "metricId": "e83ff190-69f1-46b9-a025-50d443c180a7",
                        "metricName": "EvaluationScore",
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
                "viewers": ["bix@microsoft.com"]
            }
        },
        "manually": false,
        "seriesSets": [{
                "dimensionFilter": { },
                "enrichmentConfigs": [{
                        "enrichmentConfigId": "29ac460e-6b03-49e1-a593-4d04766a987a",
                        "enrichmentName": "AnomalyDetection"
                    }
                ],
                "metricId": "96542384-5aac-4d1b-9189-f985cbfb8e84",
                "metricMeta": {
                    "dataStartFrom": "2020-08-23T00:00:00Z",
                    "datafeedId": "3e9f2e86-a5ef-44d3-bcf1-55c973e2db65",
                    "datafeedName": "Gandalf_test_multi_v3",
                    "granularityAmount": 300,
                    "granularityName": "Custom",
                    "metricName": "TotalIOsGt30s"
                },
                "seriesSetId": "bb1cfbf3-236c-4ecc-aad0-2c78f38c3701",
                "seriesSetName": "Gandalf_test_multi_v3_TotalIOsGt30s"
            }
        ],
        "startTime": "2020-12-11T22:55:00Z"
    }
    '''
    request_body = json.loads(request_json)

    data_point_request = '''
    {
        "dimensions": {
            "hashKey": "49ef94b4b9ad0df03ffe7c66e5b31a47"
        },
        "metricId": "e83ff190-69f1-46b9-a025-50d443c180a7",
        "timestamps": ["2020-12-08T22:45:00Z"],
        "values": [1.0]
    }
    '''

    #data_body = json.loads(data_point_request)
    #status, message = dummy.tsanaclient.save_data_points(request_body, request_body['instance']['target']['metrics'][0]['metricId'], data_body['dimensions'], data_body['timestamps'], data_body['values'])
    
    #do inference
    for i in range(30):
        response = client.post('/models/0000/inference', data=request_json)
        time.sleep(1200)
    
    
    #response = client.post('/dummy/models/train', data=request_json)
    #time.sleep(10)
    #response = client.post('/dummy/models/train', data=request_json)
    #response = client.post('/dummy/models/7cbb3a50-dc7a-11ea-a0bb-000d3af88183/inference', data=request_json)
    #response = client.get('/dummy/models/b06f99c6-d186-11ea-a12e-000d3af88183')
    #response = client.get('/dummy/models')
    #time.sleep(1000)


    
    
