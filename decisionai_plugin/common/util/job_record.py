import datetime
import json

from dateutil import parser

from JobController.configs import constant
from JobController.utils.postgres import PostgresClient


class JobRecord(object):
    """
    table schema:
    CREATE SEQUENCE public.training_pipeline_k8s_jobs_id_seq;
    CREATE TABLE public.training_pipeline_k8s_jobs
    (
        training_pipeline_k8s_jobs_id bigint NOT NULL DEFAULT nextval('training_pipeline_k8s_jobs_id_seq' :: regclass) PRIMARY KEY,
        job_id text NOT NULL,
        mode text NOT NULL,
        algorithm_name text NOT NULL,
        params jsonb DEFAULT '{}' NOT NULL,
        status text NOT NULL,
        events jsonb DEFAULT '{}' NOT NULL,
        create_time timestamp NOT NULL DEFAULT (now() at time zone 'utc')
    );
    CREATE UNIQUE INDEX index_training_pipeline_k8s_jobs ON public.training_pipeline_k8s_jobs(job_id);
    """
    TABLE_NAME = '"public"."training_pipeline_k8s_jobs"'

    STATUS_RUNNING = "Running"
    STATUS_INQUEUE = "InQueue"
    STATUS_SUCCESS = "Success"
    STATUS_FAILED = "Failed"
    STATUS_UNKNOWN = "Unknown"

    TTL_IN_SECONDS = 7200 * 2

    def __init__(self,
                 job_id: str,
                 mode: str,
                 algorithm_name: str,
                 params: dict,
                 status: str = None,
                 events: list = None,
                 create_time: str = None
                 ):
        self.job_id = job_id
        self.mode = mode
        self.algorithm_name = algorithm_name
        self.params = params or {}
        self.status = status or JobRecord.STATUS_UNKNOWN
        self.events = events or []
        self.create_time = create_time or str(datetime.datetime.now())

    def __iter__(self):
        obj = {
            "job_id": self.job_id,
            "mode": self.mode,
            "algorithm_name": self.algorithm_name,
            "params": self.params,
            "status": self.status,
            "events": self.events,
            "create_time": self.create_time
        }
        yield from obj.items()

    def update(self):
        """
        insert the record if not exist.
        :return:
        """
        obj = dict(self)
        obj['events'] = [msg.replace("'", "''") for msg in obj['events']]   # fix postgres insert encode
        columns = ['"%s"' % k for k in obj.keys()]
        values = ["'%s'" % (v if isinstance(v, str) else json.dumps(v))
                  for v in obj.values()]
        set_exp = ["%s=%s" % (k, v) for (k, v) in zip(columns, values)]

        sql = """
            UPDATE {table_name} SET {set_exp} WHERE "job_id"='{job_id}';
            INSERT INTO {table_name} ({columns}) 
            SELECT {values} WHERE NOT EXISTS (SELECT 1 FROM {table_name} WHERE "job_id"='{job_id}');
        """.format(
            table_name=self.TABLE_NAME, set_exp=", ".join(set_exp), values=", ".join(values),
            columns=", ".join(columns), job_id=self.job_id,
        )

        postgres = PostgresClient(constant.PostgreSqlEndpoint, constant.GraphMetaDbName)
        postgres.execute(sql)

    def exceeded_ttl(self):
        create_time = parser.parse(self.create_time).replace(tzinfo=None)
        now = datetime.datetime.now()
        if now > create_time + datetime.timedelta(seconds=self.TTL_IN_SECONDS):
            self.failed("exceeded the TTL limit [%d s]." % self.TTL_IN_SECONDS)
            return True

        return False

    def change_status(self, new_status, info):
        if self.status in [self.STATUS_SUCCESS, self.STATUS_FAILED]:
            return

        new_status = new_status.lower()
        if new_status == self.STATUS_FAILED.lower():
            self.failed(info)
        elif new_status == self.STATUS_SUCCESS.lower():
            self.success(info)
        elif new_status == self.STATUS_RUNNING.lower():
            self.run(info)
        elif new_status == self.STATUS_INQUEUE.lower():
            self.enqueue(info)
        else:
            raise Exception("Unknown Status!" + new_status)

    def run(self, info):
        self.status = JobRecord.STATUS_RUNNING
        msg = "%s - Job start running: %s." % (datetime.datetime.now(), info)
        self.events.append(msg)
        self.update()

    def enqueue(self, info):
        self.status = JobRecord.STATUS_INQUEUE
        msg = "%s - Job has sent to queue: %s." % (datetime.datetime.now(), info)
        self.events.append(msg)
        self.update()

    def success(self, info):
        self.status = JobRecord.STATUS_SUCCESS
        msg = "%s - Job succeed: %s." % (datetime.datetime.now(), info)
        self.events.append(msg)
        self.update()

    def failed(self, info):
        self.status = JobRecord.STATUS_FAILED
        msg = "%s - Job failed with: %s." % (datetime.datetime.now(), info)
        self.events.append(msg)
        self.update()

    @staticmethod
    def get_record_by_id(job_id: str):
        sql = """
            SELECT * FROM {table_name} WHERE "job_id"='{job_id}';
        """.format(
            table_name=JobRecord.TABLE_NAME, job_id=job_id,
        )

        postgres = PostgresClient(constant.PostgreSqlEndpoint, constant.GraphMetaDbName)
        data = postgres.query(sql)
        if len(data) == 0:
            return None

        return JobRecord(*data[0][1:])


if __name__ == "__main__":
    def create_jobs():
        for i in range(100):
            job_id = "job_%02d" % i
            job = JobRecord(job_id, 'train', 'algo1', params={})
            job.run(info='test')
            print(job_id)

    i = 10
    job_id = "job_%02d" % i
    job = JobRecord(job_id, 'train', 'algo1', params={})

    job.TTL_IN_SECONDS = 1
    import time
    time.sleep(2)
    print(job.exceeded_ttl())
    print(dict(job))
    print(job.get_record_by_id("3bfc0aa9-8366-46e7-b322-62390fd90dfb-1573537500-1573541946275"))