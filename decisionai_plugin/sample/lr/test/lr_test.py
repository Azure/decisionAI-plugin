import os
import sys
from os import environ
import time
import json

environ['SERVICE_CONFIG_FILE'] = 'sample/lr/config/service_config.yaml'

from sample.lr.lr_plugin_service import LrPluginService
from common.plugin_model_api import api_init, app
from common.util.timeutil import str_to_dt

if __name__ == '__main__':
    
    lr = LrPluginService()
    api_init('lr', lr)
    app.testing = True
    client = app.test_client()
    response = client.get('/')

    request_json = '{"seriesSets":[{"seriesSetName":"yongw/5min_value","seriesSetId":"33973891-f2a0-4e7d-9221-c7bce5de50f7","metricId":"f7a8325a-ba58-4c6f-8572-56f5efeb1beb","dimensionFilter":{"seriesId":"3"},"enrichmentConfigs":[{"enrichmentName":"AnomalyDetection","enrichmentConfigId":"8d0c1aab-54d1-4237-a003-9d1a0f88c195"}],"metricMeta":{"granularityName":"Custom","granularityAmount":300,"datafeedId":"603c065b-0cfe-46f4-98ce-f8b72e400fdd","metricName":"value","datafeedName":"yongw/5min","dataStartFrom":"2020-07-01T00:00:00Z"}},{"seriesSetName":"yongw/5min_value","seriesSetId":"700fe222-4d9e-403a-b459-c275f97bd0da","metricId":"f7a8325a-ba58-4c6f-8572-56f5efeb1beb","dimensionFilter":{"seriesId":"2"},"enrichmentConfigs":[{"enrichmentName":"AnomalyDetection","enrichmentConfigId":"8d0c1aab-54d1-4237-a003-9d1a0f88c195"}],"metricMeta":{"granularityName":"Custom","granularityAmount":300,"datafeedId":"603c065b-0cfe-46f4-98ce-f8b72e400fdd","metricName":"value","datafeedName":"yongw/5min","dataStartFrom":"2020-07-01T00:00:00Z"}},{"seriesSetName":"yongw/5min_value","seriesSetId":"10864d2b-c078-45bd-adda-f55ec0e945d0","metricId":"f7a8325a-ba58-4c6f-8572-56f5efeb1beb","dimensionFilter":{"seriesId":"1"},"enrichmentConfigs":[{"enrichmentName":"AnomalyDetection","enrichmentConfigId":"8d0c1aab-54d1-4237-a003-9d1a0f88c195"}],"metricMeta":{"granularityName":"Custom","granularityAmount":300,"datafeedId":"603c065b-0cfe-46f4-98ce-f8b72e400fdd","metricName":"value","datafeedName":"yongw/5min","dataStartFrom":"2020-07-01T00:00:00Z"}},{"seriesSetName":"yongw/5min_value","seriesSetId":"b21522dc-f785-426f-a3d6-65e77b884f66","metricId":"f7a8325a-ba58-4c6f-8572-56f5efeb1beb","dimensionFilter":{"seriesId":"0"},"enrichmentConfigs":[{"enrichmentName":"AnomalyDetection","enrichmentConfigId":"8d0c1aab-54d1-4237-a003-9d1a0f88c195"}],"metricMeta":{"granularityName":"Custom","granularityAmount":300,"datafeedId":"603c065b-0cfe-46f4-98ce-f8b72e400fdd","metricName":"value","datafeedName":"yongw/5min","dataStartFrom":"2020-07-01T00:00:00Z"}}],"instance":{"instanceName":"MAGA-TEST_Instance_1596081953900","instanceId":"47e08c86-088e-4ff2-b801-10f1abcfa97d","status":"Active","appId":"c96fbe27-b5b2-4a22-a27e-881259745bb7","appName":"MAGAplugin","appDisplayName":"MAGA-TEST","appType":"External","remoteModelKey":"e35aec16-d9f8-11ea-9943-e2140f8a2855","remoteCandidateModelKey":"","params":{"alertRatio":-1,"alertWindow":1,"fillMissingMethod":"Linear","fillMissingValue":1,"mergeMode":"Outer","metricDeficiency":0,"sensitivity":92,"snooze":3,"tracebackWindow":188},"hookIds":["e78723ef-3c12-4830-9f79-e9e7073d728a"]},"groupId":"6b733629-465a-4f9b-aeb5-2faa56aeda53","startTime":"2020-08-10T08:20:00Z","endTime":"2020-08-10T09:20:00Z","apiKey":"525f9a7e-d59b-4f6a-bf26-fcb647d097a1","apiEndpoint":"https://stock-exp3-api.azurewebsites.net/","manually":true}'
    response = client.post('/lr/models/0000/inference', data=request_json)
    time.sleep(1000)