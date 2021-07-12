import datetime
import json

from dateutil import parser

class JobRecord(object):
    STATUS_RUNNING = "Running"
    STATUS_INQUEUE = "InQueue"
    STATUS_SUCCESS = "Success"
    STATUS_FAILED = "Failed"
    STATUS_UNKNOWN = "Unknown"

    MODE_TRAINING = "Training"
    MODE_INFERENCE = "Inference"

    TTL_IN_SECONDS = 7200 * 2

    def __init__(self,
                 job_id: str,
                 mode: str,
                 algorithm_name: str,
                 model_id: str,
                 subscription: str,
                 params: dict,
                 status: str = None,
                 create_time: str = None
                 ):
        self.job_id = job_id
        self.mode = mode
        self.algorithm_name = algorithm_name
        self.model_id = model_id
        self.subscription = subscription
        self.params = params or {}
        self.status = status or JobRecord.STATUS_UNKNOWN
        self.create_time = create_time or str(datetime.datetime.now())

    def __iter__(self):
        obj = {
            "job_id": self.job_id,
            "mode": self.mode,
            "algorithm_name": self.algorithm_name,
            "model_id": self.model_id,
            "subscription": self.subscription,
            "params": self.params,
            "status": self.status,
            "create_time": self.create_time
        }
        yield from obj.items()

    def  exceeded_ttl(self):
        create_time = parser.parse(self.create_time).replace(tzinfo=None)
        now = datetime.datetime.now()
        if now > create_time + datetime.timedelta(seconds=JobRecord.TTL_IN_SECONDS):
            self.change_status(JobRecord.STATUS_FAILED)
            return True

        return False

    def change_status(self, new_status):
        if self.status in [JobRecord.STATUS_SUCCESS, JobRecord.STATUS_FAILED]:
            return

        self.status = new_status

if __name__ == "__main__":
    def create_jobs():
        for i in range(100):
            job_id = "job_%02d" % i
            job = JobRecord(job_id, 'train', 'algo1', 'model1', 'subscription1', params={})
            print(dict(job))