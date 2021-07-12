import requests
from requests.adapters import HTTPAdapter
import time
from telemetry import log

# disable certificate warning
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

class CommonException(Exception):
    pass

class RetryRequests(object):
    def __init__(self, count, interval):
        '''
        @param count: int, max retry count
        @param interval: int, retry interval in mille seconds
        '''
        self.count = count
        self.interval = interval
        self.session = requests.Session()
        self.session.mount('https://', HTTPAdapter(pool_maxsize=128))

    def get(self, *args, **kwargs):
        for n in range(self.count - 1, -1, -1):
            try:
                r = self.session.get(*args, **kwargs)
                if not 100 <= r.status_code < 300:
                    raise CommonException('statuscode: {}, message: {}'.format(r.status_code, r.content))
                return r
            except (CommonException, requests.exceptions.RequestException) as e:
                self.session.close()
                if n > 0:
                    time.sleep(self.interval * 0.001)
                else:
                    raise e

    def post(self, *args, **kwargs):
        for n in range(self.count - 1, -1, -1):
            try:
                r = self.session.post(*args, **kwargs)
                if not 100 <= r.status_code < 300:
                    raise CommonException('statuscode: {}, message: {}'.format(r.status_code, r.content))
                return r
            except (CommonException, requests.exceptions.RequestException) as e:
                self.session.close()
                if n > 0:
                    time.sleep(self.interval * 0.001)
                else:
                    raise e

    def put(self, *args, **kwargs):
        for n in range(self.count - 1, -1, -1):
            try:
                r = self.session.put(*args, **kwargs)
                if not 100 <= r.status_code < 300:
                    raise CommonException('statuscode: {}, message: {}'.format(r.status_code, r.content))
                return r
            except (CommonException, requests.exceptions.RequestException) as e:
                self.session.close()
                if n > 0:
                    time.sleep(self.interval * 0.001)
                else:
                    raise e

    def delete(self, *args, **kwargs):
        for n in range(self.count - 1, -1, -1):
            try:
                r = self.session.delete(*args, **kwargs)
                if not 100 <= r.status_code < 300:
                    raise CommonException('statuscode: {}, message: {}'.format(r.status_code, r.content))
                return r
            except (CommonException, requests.exceptions.RequestException) as e:
                self.session.close()
                if n > 0:
                    time.sleep(self.interval * 0.001)
                else:
                    raise e