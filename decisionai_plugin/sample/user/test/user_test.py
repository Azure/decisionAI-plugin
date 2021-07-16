import os
import sys
from os import environ
import time
import json

environ['SERVICE_CONFIG_FILE'] = 'sample/user/config/service_config.yaml'

#environ['TELEMETRY_TYPE'] = 'mon3'
environ['MON3_SERVER'] = "ks2-log-dev.westus2.cloudapp.azure.com:5201"
environ['KENSHO2_PROFILE'] = 'Plugin-Service-User'
environ['MON3_APP'] = 'plugin-user'
environ['MON3_SERVICE'] = 'plugin-service'

from decisionai_plugin.common.plugin_model_api import api_init, app
from decisionai_plugin.common.plugin_service import PluginService

if __name__ == '__main__':
    
    user_plugin = PluginService(trainable=False, name="user.model")
    api_init(user_plugin)
    app.testing = True
    client = app.test_client()
    response = client.get('/')

    with open("sample/user/test/request_sample.json", "r") as rs:
        request_json = rs.read()

    request_body = json.loads(request_json)
    #do inference
    response = client.post('/models/0000/inference', data=request_json)
    time.sleep(1000)