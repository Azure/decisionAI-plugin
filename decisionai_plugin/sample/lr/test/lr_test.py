import os
import sys
from os import environ
import time
import json

environ['SERVICE_CONFIG_FILE'] = 'sample/lr/config/service_config.yaml'

#environ['TELEMETRY_TYPE'] = 'mon3'
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

    with open("sample/lr/test/request_sample.json", "r") as rs:
        request_json = rs.read()

    request_body = json.loads(request_json)
    #do inference
    response = client.post('/models/0000/inference', data=request_json)
    time.sleep(1000)

    '''
    # save_data_points
    result = {'metricId': '428f5f7c-ebd7-400a-a83c-c6e9ab5291cd', 'dimensions': {'HashKey': '06d1e099bdf6621989f19780ae6ab6aa'}, 'timestamps': ['2019-01-14T05:00:00.0000000Z', '2019-01-15T05:00:00.0000000Z', '2019-01-16T05:00:00.0000000Z', '2019-01-17T05:00:00.0000000Z', '2019-01-18T05:00:00.0000000Z', '2019-01-19T05:00:00.0000000Z', '2019-01-20T05:00:00.0000000Z', '2019-01-21T05:00:00.0000000Z', '2019-01-22T05:00:00.0000000Z', '2019-01-23T05:00:00.0000000Z', '2019-01-24T05:00:00.0000000Z', '2019-01-25T05:00:00.0000000Z', '2019-01-26T05:00:00.0000000Z', '2019-01-27T05:00:00.0000000Z', '2019-01-28T05:00:00.0000000Z', '2019-01-29T05:00:00.0000000Z', '2019-01-30T05:00:00.0000000Z', '2019-01-31T05:00:00.0000000Z', '2019-02-01T05:00:00.0000000Z', '2019-02-02T05:00:00.0000000Z', '2019-02-03T05:00:00.0000000Z', '2019-02-04T05:00:00.0000000Z', '2019-02-05T05:00:00.0000000Z', '2019-02-06T05:00:00.0000000Z', '2019-02-07T05:00:00.0000000Z', '2019-02-08T05:00:00.0000000Z', '2019-02-09T05:00:00.0000000Z', '2019-02-10T05:00:00.0000000Z', '2019-02-11T05:00:00.0000000Z', '2019-02-12T05:00:00.0000000Z', '2019-02-13T05:00:00.0000000Z', '2019-02-14T05:00:00.0000000Z', '2019-02-15T05:00:00.0000000Z', '2019-02-16T05:00:00.0000000Z', '2019-02-17T05:00:00.0000000Z', '2019-02-18T05:00:00.0000000Z', '2019-02-19T05:00:00.0000000Z', '2019-02-20T05:00:00.0000000Z', '2019-02-21T05:00:00.0000000Z', '2019-02-22T05:00:00.0000000Z', '2019-02-23T05:00:00.0000000Z', '2019-02-24T05:00:00.0000000Z', '2019-02-25T05:00:00.0000000Z', '2019-02-26T05:00:00.0000000Z', '2019-02-27T05:00:00.0000000Z', '2019-02-28T05:00:00.0000000Z', '2019-03-01T05:00:00.0000000Z', '2019-03-02T05:00:00.0000000Z', '2019-03-03T05:00:00.0000000Z', '2019-03-04T05:00:00.0000000Z', '2019-03-05T05:00:00.0000000Z', '2019-03-06T05:00:00.0000000Z', '2019-03-07T05:00:00.0000000Z', '2019-03-08T05:00:00.0000000Z', '2019-03-09T05:00:00.0000000Z', '2019-03-10T05:00:00.0000000Z', '2019-03-11T04:00:00.0000000Z', '2019-03-12T04:00:00.0000000Z', '2019-03-13T04:00:00.0000000Z', '2019-03-14T04:00:00.0000000Z', '2019-03-15T04:00:00.0000000Z', '2019-03-16T04:00:00.0000000Z', '2019-03-17T04:00:00.0000000Z', '2019-03-18T04:00:00.0000000Z', '2019-03-19T04:00:00.0000000Z', '2019-03-20T04:00:00.0000000Z', '2019-03-21T04:00:00.0000000Z', '2019-03-22T04:00:00.0000000Z', '2019-03-23T04:00:00.0000000Z', '2019-03-24T04:00:00.0000000Z', '2019-03-25T04:00:00.0000000Z', '2019-03-26T04:00:00.0000000Z', '2019-03-27T04:00:00.0000000Z', '2019-03-28T04:00:00.0000000Z', '2019-03-29T04:00:00.0000000Z', '2019-03-30T04:00:00.0000000Z', '2019-03-31T04:00:00.0000000Z', '2019-04-01T04:00:00.0000000Z', '2019-04-02T04:00:00.0000000Z', '2019-04-03T04:00:00.0000000Z', '2019-04-04T04:00:00.0000000Z', '2019-04-05T04:00:00.0000000Z', '2019-04-06T04:00:00.0000000Z', '2019-04-07T04:00:00.0000000Z', '2019-04-08T04:00:00.0000000Z', '2019-04-09T04:00:00.0000000Z', '2019-04-10T04:00:00.0000000Z', '2019-04-11T04:00:00.0000000Z', '2019-04-12T04:00:00.0000000Z', '2019-04-13T04:00:00.0000000Z', '2019-04-14T04:00:00.0000000Z', '2019-04-15T04:00:00.0000000Z', '2019-04-16T04:00:00.0000000Z', '2019-04-17T04:00:00.0000000Z', '2019-04-18T04:00:00.0000000Z', '2019-04-19T04:00:00.0000000Z', '2019-04-20T04:00:00.0000000Z', '2019-04-21T04:00:00.0000000Z', '2019-04-22T04:00:00.0000000Z', '2019-04-23T04:00:00.0000000Z', '2019-04-24T04:00:00.0000000Z', '2019-04-25T04:00:00.0000000Z', '2019-04-26T04:00:00.0000000Z', '2019-04-27T04:00:00.0000000Z', '2019-04-28T04:00:00.0000000Z', '2019-04-29T04:00:00.0000000Z', '2019-04-30T04:00:00.0000000Z', '2019-05-01T04:00:00.0000000Z', '2019-05-02T04:00:00.0000000Z', '2019-05-03T04:00:00.0000000Z', '2019-05-04T04:00:00.0000000Z', '2019-05-05T04:00:00.0000000Z', '2019-05-06T04:00:00.0000000Z', '2019-05-07T04:00:00.0000000Z', '2019-05-08T04:00:00.0000000Z', '2019-05-09T04:00:00.0000000Z', '2019-05-10T04:00:00.0000000Z', '2019-05-11T04:00:00.0000000Z'], 'values': [1.0, 2.0, 8.0, 7.0, 0.0, 0.0, 1.0, 7.0, 4.0, 5.0, 5.0, 9.0, 0.0, 2.0, 8.0, 8.0, 9.0, 7.0, 5.0, 0.0, 0.0, 4.0, 2.0, 8.0, 13.0, 8.0, 1.0, 1.0, 10.0, 10.0, 10.0, 4.0, 11.0, 6.0, 3.0, 11.0, 7.0, 10.0, 11.0, 7.0, 2.0, 1.0, 17.0, 11.0, 13.0, 18.0, 10.0, 2.0, 0.0, 22.0, 11.0, 16.0, 9.0, 15.0, 3.0, 3.0, 13.0, 23.0, 25.0, 19.0, 29.0, 3.0, 6.0, 25.0, 41.0, 27.0, 29.0, 23.0, 1.0, 6.0, 38.0, 30.0, 44.0, 80.0, 46.0, 6.0, 11.0, 50.0, 61.0, 81.0, 76.0, 46.0, 7.0, 12.0, 61.0, 84.0, 90.0, 94.0, 87.0, 12.0, 7.0, 91.0, 99.0, 124.0, 112.0, 80.0, 12.0, 11.0, 105.0, 140.0, 188.0, 187.0, 181.0, 32.0, 40.0, 216.0, 245.0, 164.0, 287.0, 285.0, 51.0, 55.0, 339.0, 464.0, 471.0, 497.0, 421.0, 59.0]}
    status, message = lr.tsanaclient.save_data_points(request_body, request_body['appInstance']['target']['metrics'][0]['metricId'], result['dimensions'], result['timestamps'], result['values'])
    print(message)
    '''

    # generate request sample
    request_sample = generate_request(lr, request_body['apiEndpoint'], request_body['apiKey'], request_body['groupId'], request_body['appInstance']['appInstanceId'], request_body['startTime'], request_body['endTime'])
    
    #do inference
    response = client.post('/models/0000/inference', data=request_json)
    time.sleep(1000)
    
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
        series_def = dict(metricId=request_sample['appInstance']['target']['metrics'][0]['metricId'], dimensionFilter=dict(seriesId=series['seriesId']), metricMeta=series['metricMeta'])
        factors_data = lr.tsanaclient.get_timeseries(request_sample['apiEndpoint'], request_sample['apiKey'], [series_def],
                                                       start_time, end_time, offset=0,
                                                       top=100)
        for factor in factors_data:
            print(json.dumps(factor.__dict__))