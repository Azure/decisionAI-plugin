import os
import sys
from os import environ

#sys.path.append(os.path.join(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir), os.pardir))

environ['SERVICE_CONFIG_FILE'] = 'sample/demo_modeless/config/service_config.yaml'

from sample.demo_modeless.demo_service import DemoService
from common.plugin_model_api import api_init, app

if __name__ == '__main__':
    HOST = environ.get('SERVER_HOST', '0.0.0.0')
    PORT = environ.get('SERVER_PORT', 56789)
    demo = DemoService()
    api_init('demomodeless', demo)
    app.run(HOST, PORT, threaded=True, use_reloader=False)