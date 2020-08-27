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

    request_json = '{"groupId":"8e826a5d-1b01-4ff4-a699-38bea97e17de", \
        "seriesSets":[ \
            {"seriesSetId":"b643e346-6883-4764-84a5-e63a3788eec9","metricId":"dc5b66cf-6dd0-4c83-bb8f-d849e68a7660","dimensionFilter":{"ts_code":"600030.SH"},"seriesSetName":"Stock price_high","metricMeta":{"granularityName":"Daily","granularityAmount":0,"datafeedId":"29595b1c-531f-445c-adcf-b75b2ab93c34","metricName":"high","datafeedName":"Stock price","dataStartFrom":1105315200000}}, \
            {"seriesSetId":"0d4cce4d-f4d4-4cef-be87-dbd28062abfc","metricId":"3274f7e6-683b-4d92-b134-0c1186e416a1","dimensionFilter":{"ts_code":"600030.SH"},"seriesSetName":"Stock price_change","metricMeta":{"granularityName":"Daily","granularityAmount":0,"datafeedId":"29595b1c-531f-445c-adcf-b75b2ab93c34","metricName":"change","datafeedName":"Stock price","dataStartFrom":1105315200000}} \
        ], \
        "gran":{"granularityString":"Daily","customInSeconds":0}, \
        "instance":{ \
            "instanceName":"Forecast_Instance_1586447708033","instanceId":"528cbe52-cb6a-44c0-b388-580aba57f2f7","status":"Active","appId":"173276d9-a7ed-494b-9300-6dd1aa09f2c3","appName":"Forecast","appDisplayName":"Forecast","appType":"Internal","remoteModelKey":"", \
            "params":{"missingRatio":0.5,"target":{"seriesSetId":"b643e346-6883-4764-84a5-e63a3788eec9","filters":{"ts_code":"600030.SH"},"metricId":"dc5b66cf-6dd0-4c83-bb8f-d849e68a7660","name":"Stock price_high"},"waitInSeconds":60,"windowSize":28, "step":2},"hookIds":[] \
        }, \
        "startTime":"2020-03-18T00:00:00Z","endTime":"2020-04-18T00:00:00Z","apiKey":"3517cf61-065d-40e9-8ed4-eda58147982d","apiEndpoint":"https://stock-exp2-api.azurewebsites.net/","fieldsFilter":["IsAnomaly"]}'
    
    #response = client.post('/dummy/models/train', data=request_json)
    #time.sleep(10)
    #response = client.post('/dummy/models/train', data=request_json)
    response = client.post('/dummy/models/7cbb3a50-dc7a-11ea-a0bb-000d3af88183/inference', data=request_json)
    #response = client.get('/dummy/models/b06f99c6-d186-11ea-a12e-000d3af88183')
    #response = client.get('/dummy/models')
    time.sleep(1000)


    alert_request_json = '{"seriesSets":[{"seriesSetName":"yongw/5min_value","seriesSetId":"33973891-f2a0-4e7d-9221-c7bce5de50f7","metricId":"f7a8325a-ba58-4c6f-8572-56f5efeb1beb","dimensionFilter":{"seriesId":"3"},"enrichmentConfigs":[{"enrichmentName":"AnomalyDetection","enrichmentConfigId":"8d0c1aab-54d1-4237-a003-9d1a0f88c195"}],"metricMeta":{"granularityName":"Custom","granularityAmount":300,"datafeedId":"603c065b-0cfe-46f4-98ce-f8b72e400fdd","metricName":"value","datafeedName":"yongw/5min","dataStartFrom":"2020-07-01T00:00:00Z"}},{"seriesSetName":"yongw/5min_value","seriesSetId":"700fe222-4d9e-403a-b459-c275f97bd0da","metricId":"f7a8325a-ba58-4c6f-8572-56f5efeb1beb","dimensionFilter":{"seriesId":"2"},"enrichmentConfigs":[{"enrichmentName":"AnomalyDetection","enrichmentConfigId":"8d0c1aab-54d1-4237-a003-9d1a0f88c195"}],"metricMeta":{"granularityName":"Custom","granularityAmount":300,"datafeedId":"603c065b-0cfe-46f4-98ce-f8b72e400fdd","metricName":"value","datafeedName":"yongw/5min","dataStartFrom":"2020-07-01T00:00:00Z"}},{"seriesSetName":"yongw/5min_value","seriesSetId":"10864d2b-c078-45bd-adda-f55ec0e945d0","metricId":"f7a8325a-ba58-4c6f-8572-56f5efeb1beb","dimensionFilter":{"seriesId":"1"},"enrichmentConfigs":[{"enrichmentName":"AnomalyDetection","enrichmentConfigId":"8d0c1aab-54d1-4237-a003-9d1a0f88c195"}],"metricMeta":{"granularityName":"Custom","granularityAmount":300,"datafeedId":"603c065b-0cfe-46f4-98ce-f8b72e400fdd","metricName":"value","datafeedName":"yongw/5min","dataStartFrom":"2020-07-01T00:00:00Z"}},{"seriesSetName":"yongw/5min_value","seriesSetId":"b21522dc-f785-426f-a3d6-65e77b884f66","metricId":"f7a8325a-ba58-4c6f-8572-56f5efeb1beb","dimensionFilter":{"seriesId":"0"},"enrichmentConfigs":[{"enrichmentName":"AnomalyDetection","enrichmentConfigId":"8d0c1aab-54d1-4237-a003-9d1a0f88c195"}],"metricMeta":{"granularityName":"Custom","granularityAmount":300,"datafeedId":"603c065b-0cfe-46f4-98ce-f8b72e400fdd","metricName":"value","datafeedName":"yongw/5min","dataStartFrom":"2020-07-01T00:00:00Z"}}],"instance":{"instanceName":"MAGA-TEST_Instance_1596081953900","instanceId":"47e08c86-088e-4ff2-b801-10f1abcfa97d","status":"Active","appId":"c96fbe27-b5b2-4a22-a27e-881259745bb7","appName":"MAGAplugin","appDisplayName":"MAGA-TEST","appType":"External","remoteModelKey":"e35aec16-d9f8-11ea-9943-e2140f8a2855","remoteCandidateModelKey":"","params":{"alertRatio":-1,"alertWindow":28,"fillMissingMethod":"Linear","fillMissingValue":1,"mergeMode":"Outer","metricDeficiency":0,"sensitivity":92,"snooze":3,"tracebackWindow":388},"hookIds":["e78723ef-3c12-4830-9f79-e9e7073d728a"]},"groupId":"6b733629-465a-4f9b-aeb5-2faa56aeda53","startTime":"2020-08-10T09:20:00Z","endTime":"2020-08-10T09:20:00Z","apiKey":"525f9a7e-d59b-4f6a-bf26-fcb647d097a1","apiEndpoint":"https://stock-exp3-api.azurewebsites.net/","manually":false}'
    alert_result_json = '[{"contributors":[{"variable":"166518f482792966f6bcc1600853f5cf","probability":1.1350116729736328},{"variable":"f1a6b15f82caed2d80b73f91d0d26a33","probability":1.2112369537353516},{"variable":"cd12c5722becb50f88961ef0d766f493","probability":1.220422625541687},{"variable":"8a70009191d7d71315626906fe38fffb","probability":1.223111629486084}],"isAnomaly":true,"score":-0.9746478796005249,"severity":0.0,"timestamp":"2020-07-03T09:20:00Z"},{"contributors":[],"isAnomaly":false,"score":-0.9746478796005249,"severity":0.0,"timestamp":"2020-08-04T09:30:00Z"},{"timestamp":"2020-08-04T09:35:00Z"}]'
    parameters = json.loads(alert_request_json)
    start_time = str_to_dt("2020-08-07T01:40:00Z")
    end_time = str_to_dt("2020-08-07T01:50:00Z")
    gran = ("Custom", 300)
    result = json.loads(alert_result_json)    
    dummy.tsanaclient.trigger_alert(parameters, start_time, end_time, gran, result)


    
    
