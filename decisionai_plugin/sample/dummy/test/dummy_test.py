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
    
    response = client.post('/dummy/models/train', data=request_json)
    #time.sleep(10)
    #response = client.post('/dummy/models/train', data=request_json)
    #response = client.post('/dummy/models/7cbb3a50-dc7a-11ea-a0bb-000d3af88183/inference', data=request_json)
    #response = client.get('/dummy/models/b06f99c6-d186-11ea-a12e-000d3af88183')
    #response = client.get('/dummy/models')
    time.sleep(1000)


    
    
