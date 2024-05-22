import json
import logging
from urllib.parse import urlparse

import requests
from kafka.oauth import AbstractTokenProvider

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def configure(bootstrap_server_list):
    bootstrap_server = bootstrap_server_list[0]
    bootstrap_server = bootstrap_server.replace("\\[|\\]", "")
    logger.info(f"bootstrap_server: {bootstrap_server}")
    url_parser = urlparse("https://" + bootstrap_server)
    server = url_parser.scheme + "://" + url_parser.hostname
    logger.info(f"server: {server}")
    return server


class ManagedIdentityAuthHelper(AbstractTokenProvider):
    __msi_endpoint = "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource={0}"
    __resource_manage_endpoint = {
        "AzureCloud".upper(): "https://management.core.windows.net/",
        "AzureChinaCloud".upper(): "https://management.core.chinacloudapi.cn/",
        "USNat".upper(): "https://management.core.usgovcloudapi.net/",
        "USSec".upper(): "https://management.core.cloudapi.de/"
    }

    def __init__(self, azure_environment, bootstrap_server_list, **config):
        super().__init__(**config)
        self.azure_environment = azure_environment.upper()
        self.token_audience = configure(bootstrap_server_list)

    def get_msi_endpoint(self):
        resource_manager_endpoint = ManagedIdentityAuthHelper.__resource_manage_endpoint.get(self.azure_environment)
        return ManagedIdentityAuthHelper.__msi_endpoint.format(resource_manager_endpoint
                                                               if self.token_audience is None else self.token_audience)

    def token(self):
        msi_endpoint = self.get_msi_endpoint()
        logger.info(f"Acquiring token from {msi_endpoint}")
        r = requests.get(msi_endpoint, headers={'Metadata': 'true'})
        data2 = json.loads(r.text)
        access_token = data2['access_token']
        expires_on = data2['expires_on']
        logger.info(f"Got a new token for {msi_endpoint} will expires on {expires_on}")
        return access_token, float(expires_on)
