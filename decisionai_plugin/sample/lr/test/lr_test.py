import os
import sys
from os import environ
import time
import json

environ['SERVICE_CONFIG_FILE'] = 'sample/lr/config/service_config.yaml'

environ['TELEMETRY_TYPE'] = 'mon3'
environ['MON3_SERVER'] = "ks2-log-dev.westus2.cloudapp.azure.com:5201"
environ['KENSHO2_PROFILE'] = 'Plugin-Service-AIDice'
environ['MON3_APP'] = 'plugin-aidice'
environ['MON3_SERVICE'] = 'plugin-service'

from sample.lr.lr_plugin_service import LrPluginService
from sample.util.request_generator import generate_request
from decisionai_plugin.common.plugin_model_api import api_init, app
from decisionai_plugin.common.util.timeutil import str_to_dt, dt_to_str
from decisionai_plugin.common.util.constant import STATUS_SUCCESS, STATUS_FAIL, InferenceState

if __name__ == '__main__':
    
    lr = LrPluginService()
    api_init(lr)
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
                        "manually": true,
                        "dataRetrieving": true
                    }
                    '''

    request_body = json.loads(request_json)
    #do inference
    response = client.post('/models/0000/inference', data=request_json)
    time.sleep(1000)

    '''
    #save_data_points
    result = {'metricId': '428f5f7c-ebd7-400a-a83c-c6e9ab5291cd', 'dimensions': {'HashKey': '06d1e099bdf6621989f19780ae6ab6aa'}, 'timestamps': ['2019-01-14T05:00:00.0000000Z', '2019-01-15T05:00:00.0000000Z', '2019-01-16T05:00:00.0000000Z', '2019-01-17T05:00:00.0000000Z', '2019-01-18T05:00:00.0000000Z', '2019-01-19T05:00:00.0000000Z', '2019-01-20T05:00:00.0000000Z', '2019-01-21T05:00:00.0000000Z', '2019-01-22T05:00:00.0000000Z', '2019-01-23T05:00:00.0000000Z', '2019-01-24T05:00:00.0000000Z', '2019-01-25T05:00:00.0000000Z', '2019-01-26T05:00:00.0000000Z', '2019-01-27T05:00:00.0000000Z', '2019-01-28T05:00:00.0000000Z', '2019-01-29T05:00:00.0000000Z', '2019-01-30T05:00:00.0000000Z', '2019-01-31T05:00:00.0000000Z', '2019-02-01T05:00:00.0000000Z', '2019-02-02T05:00:00.0000000Z', '2019-02-03T05:00:00.0000000Z', '2019-02-04T05:00:00.0000000Z', '2019-02-05T05:00:00.0000000Z', '2019-02-06T05:00:00.0000000Z', '2019-02-07T05:00:00.0000000Z', '2019-02-08T05:00:00.0000000Z', '2019-02-09T05:00:00.0000000Z', '2019-02-10T05:00:00.0000000Z', '2019-02-11T05:00:00.0000000Z', '2019-02-12T05:00:00.0000000Z', '2019-02-13T05:00:00.0000000Z', '2019-02-14T05:00:00.0000000Z', '2019-02-15T05:00:00.0000000Z', '2019-02-16T05:00:00.0000000Z', '2019-02-17T05:00:00.0000000Z', '2019-02-18T05:00:00.0000000Z', '2019-02-19T05:00:00.0000000Z', '2019-02-20T05:00:00.0000000Z', '2019-02-21T05:00:00.0000000Z', '2019-02-22T05:00:00.0000000Z', '2019-02-23T05:00:00.0000000Z', '2019-02-24T05:00:00.0000000Z', '2019-02-25T05:00:00.0000000Z', '2019-02-26T05:00:00.0000000Z', '2019-02-27T05:00:00.0000000Z', '2019-02-28T05:00:00.0000000Z', '2019-03-01T05:00:00.0000000Z', '2019-03-02T05:00:00.0000000Z', '2019-03-03T05:00:00.0000000Z', '2019-03-04T05:00:00.0000000Z', '2019-03-05T05:00:00.0000000Z', '2019-03-06T05:00:00.0000000Z', '2019-03-07T05:00:00.0000000Z', '2019-03-08T05:00:00.0000000Z', '2019-03-09T05:00:00.0000000Z', '2019-03-10T05:00:00.0000000Z', '2019-03-11T04:00:00.0000000Z', '2019-03-12T04:00:00.0000000Z', '2019-03-13T04:00:00.0000000Z', '2019-03-14T04:00:00.0000000Z', '2019-03-15T04:00:00.0000000Z', '2019-03-16T04:00:00.0000000Z', '2019-03-17T04:00:00.0000000Z', '2019-03-18T04:00:00.0000000Z', '2019-03-19T04:00:00.0000000Z', '2019-03-20T04:00:00.0000000Z', '2019-03-21T04:00:00.0000000Z', '2019-03-22T04:00:00.0000000Z', '2019-03-23T04:00:00.0000000Z', '2019-03-24T04:00:00.0000000Z', '2019-03-25T04:00:00.0000000Z', '2019-03-26T04:00:00.0000000Z', '2019-03-27T04:00:00.0000000Z', '2019-03-28T04:00:00.0000000Z', '2019-03-29T04:00:00.0000000Z', '2019-03-30T04:00:00.0000000Z', '2019-03-31T04:00:00.0000000Z', '2019-04-01T04:00:00.0000000Z', '2019-04-02T04:00:00.0000000Z', '2019-04-03T04:00:00.0000000Z', '2019-04-04T04:00:00.0000000Z', '2019-04-05T04:00:00.0000000Z', '2019-04-06T04:00:00.0000000Z', '2019-04-07T04:00:00.0000000Z', '2019-04-08T04:00:00.0000000Z', '2019-04-09T04:00:00.0000000Z', '2019-04-10T04:00:00.0000000Z', '2019-04-11T04:00:00.0000000Z', '2019-04-12T04:00:00.0000000Z', '2019-04-13T04:00:00.0000000Z', '2019-04-14T04:00:00.0000000Z', '2019-04-15T04:00:00.0000000Z', '2019-04-16T04:00:00.0000000Z', '2019-04-17T04:00:00.0000000Z', '2019-04-18T04:00:00.0000000Z', '2019-04-19T04:00:00.0000000Z', '2019-04-20T04:00:00.0000000Z', '2019-04-21T04:00:00.0000000Z', '2019-04-22T04:00:00.0000000Z', '2019-04-23T04:00:00.0000000Z', '2019-04-24T04:00:00.0000000Z', '2019-04-25T04:00:00.0000000Z', '2019-04-26T04:00:00.0000000Z', '2019-04-27T04:00:00.0000000Z', '2019-04-28T04:00:00.0000000Z', '2019-04-29T04:00:00.0000000Z', '2019-04-30T04:00:00.0000000Z', '2019-05-01T04:00:00.0000000Z', '2019-05-02T04:00:00.0000000Z', '2019-05-03T04:00:00.0000000Z', '2019-05-04T04:00:00.0000000Z', '2019-05-05T04:00:00.0000000Z', '2019-05-06T04:00:00.0000000Z', '2019-05-07T04:00:00.0000000Z', '2019-05-08T04:00:00.0000000Z', '2019-05-09T04:00:00.0000000Z', '2019-05-10T04:00:00.0000000Z', '2019-05-11T04:00:00.0000000Z'], 'values': [1.0, 2.0, 8.0, 7.0, 0.0, 0.0, 1.0, 7.0, 4.0, 5.0, 5.0, 9.0, 0.0, 2.0, 8.0, 8.0, 9.0, 7.0, 5.0, 0.0, 0.0, 4.0, 2.0, 8.0, 13.0, 8.0, 1.0, 1.0, 10.0, 10.0, 10.0, 4.0, 11.0, 6.0, 3.0, 11.0, 7.0, 10.0, 11.0, 7.0, 2.0, 1.0, 17.0, 11.0, 13.0, 18.0, 10.0, 2.0, 0.0, 22.0, 11.0, 16.0, 9.0, 15.0, 3.0, 3.0, 13.0, 23.0, 25.0, 19.0, 29.0, 3.0, 6.0, 25.0, 41.0, 27.0, 29.0, 23.0, 1.0, 6.0, 38.0, 30.0, 44.0, 80.0, 46.0, 6.0, 11.0, 50.0, 61.0, 81.0, 76.0, 46.0, 7.0, 12.0, 61.0, 84.0, 90.0, 94.0, 87.0, 12.0, 7.0, 91.0, 99.0, 124.0, 112.0, 80.0, 12.0, 11.0, 105.0, 140.0, 188.0, 187.0, 181.0, 32.0, 40.0, 216.0, 245.0, 164.0, 287.0, 285.0, 51.0, 55.0, 339.0, 464.0, 471.0, 497.0, 421.0, 59.0]}
    status, message = lr.tsanaclient.save_data_points(request_body, request_body['instance']['target']['metrics'][0]['metricId'], result['dimensions'], result['timestamps'], result['values'])
    print(message)
    
    # generate request sample
    request_sample = generate_request(lr, request_body['apiEndpoint'], request_body['apiKey'], request_body['groupId'], request_body['instance']['instanceId'], request_body['startTime'], request_body['endTime'])
    '''
    
    request_sample = {'groupId': '07514da3-1b93-4cfe-b9dd-177c3e4eb10c', 'apiEndpoint': 'https://AIDice-T2-api.azurewebsites.net/', 'apiKey': '2812e4ea-ce22-4283-b24d-73333893b240', 'seriesSets': [{'seriesSetName': 'Gandalf_test_multi_v3_TotalIOsGt30s', 'seriesSetId': 'bb1cfbf3-236c-4ecc-aad0-2c78f38c3701', 'metricId': '96542384-5aac-4d1b-9189-f985cbfb8e84', 'dimensionFilter': {}, 'enrichmentConfigs': [{'enrichmentName': 'AnomalyDetection', 'enrichmentConfigId': '29ac460e-6b03-49e1-a593-4d04766a987a'}], 'metricMeta': {'granularityName': 'Custom', 'granularityAmount': 300, 'datafeedId': '3e9f2e86-a5ef-44d3-bcf1-55c973e2db65', 'metricName': 'TotalIOsGt30s', 'datafeedName': 'Gandalf_test_multi_v3', 'dataStartFrom': '2020-08-23T00:00:00+00:00'}}], 'instance': {'instanceName': 'AiDice Traceback Window Fix_Instance_1603826870737', 'instanceId': '8734db1c-a59d-43e4-90cb-d2f87a9d66c1', 'status': 'Active', 'appId': 'da2af68f-e0f3-4674-a716-e91fe46f9376', 'appName': 'AiDice_Traceback_Window_Fix', 'appDisplayName': 'AiDice Traceback Window Fix', 'appType': 'External', 'remoteModelKey': '', 'remoteCandidateModelKey': '', 'params': {'removeColumns': '[]', 'totalSteps': 5000, 'scheduledRun': False, 'tracebackWindow': 0, 'ifEarlyAbort': True, 'tabuSize': 50, 'randomProbability': 0.5, 'bmsSize': 50, 'maxPatternLength': 4, 'metricDeficiency': 0, 'maxSearchResult': 20, 'runInterval': 3600, 'maxSearchTime': 180, 'maxOutputRows': 3, 'maxOutputChars': 60000, 'maxRetrievalRows': 1000}, 'target': {'fillMissingPointForAd': 'PreviousValue', 'migrationType': 0, 'maxConcurrency': -1, 'minRetryIntervalInSeconds': -1, 'granularityName': 'Minutely', 'credentialUUID': None, 'dataStartFrom': 1603826940000, 'createdTime': 1603826882000, 'ingestionType': 'Single', 'needRollup': 'RollupByUser', 'allUpIdentification': None, 'detectionStartTime': None, 'datafeedDescription': '', 'rollUpColumns': '', 'creator': 'giantoni@microsoft.com', 'actionLinkTemplate': '', 'extendedDimensions': None, 'stopRetryAfterInSeconds': -1, 'maxQueryPerMinute': 30.0, 'startOffsetInSeconds': 0, 'isAdmin': True, 'viewMode': 'Private', 'target': 'TSDB', 'granularityAmount': None, 'rollUpMethod': 'None', 'viewers': ['bix@microsoft.com'], 'datafeedId': '472b445e-fe64-4a42-8b08-82148d802368', 'parameterList': [], 'fillMissingPointForAdValue': 0.0, 'metrics': [{'metricId': '1f2b7333-dbb8-43e2-a6ab-f752a49ed31f', 'metricName': 'EvaluationScore', 'metricDisplayName': 'EvaluationScore', 'metricDescription': '', 'metricType': 'NORMAL', 'derivedScript': ''}], 'authenticationType': 'Basic', 'datafeedName': 'Data Feed for AiDice Traceback Window Fix_Instance_1603826870737', 'dataSourceType': 'API', 'admins': ['chuwan@microsoft.com', 'bennyng@microsoft.com', 'giantoni@microsoft.com', 'nguyenm@microsoft.com', 'conhua@microsoft.com', 'abasok@microsoft.com', 'mingzhao@microsoft.com', '07514da3-1b93-4cfe-b9dd-177c3e4eb10c@metricsadvisor.ai'], 'dimensions': [{'dimensionName': 'hashKey', 'dimensionDisplayName': 'hashKey'}], 'timestampColumn': 'timestamp', 'status': 'Active'}, 'hookIds': ['f07e6f43-248b-4377-a26e-6adcfd01f97a']}, 'startTime': '2020-11-01T01:59:59Z', 'endTime': '2020-11-01T02:00:00Z', 'manually': True, 'groupName': 'e2e_stream_15_gran'}
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