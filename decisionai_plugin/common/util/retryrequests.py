import requests
import time
from telemetry import log

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

    def get(self, *args, **kwargs):
        for n in range(self.count - 1, -1, -1):
            try:
                session = requests.Session()
                r = session.get(*args, **kwargs)
                if not 100 <= r.status_code < 300:
                    raise CommonException('statuscode: {}, message: {}'.format(r.status_code, r.content))
                return r
            except (CommonException, requests.exceptions.RequestException) as e:
                session.close()
                if n > 0:
                    time.sleep(self.interval * 0.001)
                else:
                    raise e

    def post(self, *args, **kwargs):
        for n in range(self.count - 1, -1, -1):
            try:
                session = requests.Session()
                r = session.post(*args, **kwargs)
                if not 100 <= r.status_code < 300:
                    raise CommonException('statuscode: {}, message: {}'.format(r.status_code, r.content))
                return r
            except (CommonException, requests.exceptions.RequestException) as e:
                session.close()
                if n > 0:
                    time.sleep(self.interval * 0.001)
                else:
                    raise e

    def put(self, *args, **kwargs):
        for n in range(self.count - 1, -1, -1):
            try:
                session = requests.Session()
                r = session.put(*args, **kwargs)
                if not 100 <= r.status_code < 300:
                    raise CommonException('statuscode: {}, message: {}'.format(r.status_code, r.content))
                return r
            except (CommonException, requests.exceptions.RequestException) as e:
                session.close()
                if n > 0:
                    time.sleep(self.interval * 0.001)
                else:
                    raise e

    def delete(self, *args, **kwargs):
        for n in range(self.count - 1, -1, -1):
            try:
                session = requests.Session()
                r = session.delete(*args, **kwargs)
                if not 100 <= r.status_code < 300:
                    raise CommonException('statuscode: {}, message: {}'.format(r.status_code, r.content))
                return r
            except (CommonException, requests.exceptions.RequestException) as e:
                session.close()
                if n > 0:
                    time.sleep(self.interval * 0.001)
                else:
                    raise e