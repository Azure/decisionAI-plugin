import os
import sys
from os import environ

environ['SERVICE_CONFIG_FILE'] = 'sample/lr/config/service_config.yaml'

from lr_plugin_service import LrPluginService
from common.plugin_model_api import api_init, app

lr = LrPluginService()
api_init('lr', lr)

if __name__ == '__main__':
    HOST = environ.get('SERVER_HOST', '0.0.0.0')
    PORT = environ.get('SERVER_PORT', 56789)
    app.run(HOST, PORT, threaded=True, use_reloader=False)