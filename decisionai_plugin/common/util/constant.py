import enum

class ModelState(enum.Enum):
    Pending = 0
    Training = 1
    Ready = 2
    Deleted = 3
    Failed = 4

class InferenceState(enum.Enum):
    Pending = 0
    Running = 1
    Ready = 2
    Failed = 3

STATUS_SUCCESS = 'Success'
STATUS_FAIL = 'Fail'

TIMESTAMP = 'timestamp'
VALUE = 'value'

LAG = 'lag'
FORECAST = 'forecast'
UPPER = 'upper'
LOWER = 'lower'

DAY_IN_SECONDS = 86400
HOUR_IN_SECONDS = 3600
MINT_IN_SECONDS = 60

USER_ADDR = '@metricsadvisor.ai'

INGESTION_API = '/powerai-ingestion-api'
META_API = '/powerai-metadata3p-api'
TSG_API = '/powerai-time-series-group-api-3p'
STORAGE_GW_API = '/storage-gw-server'

INSTANCE_ID_KEY = 'x-instance-id'
META_ENDPOINT = "http://powerai-metadata3p-api.kensho2-service.svc.cluster.local:2000"
INGESTION_ENDPOINT = "http://powerai-ingestion-api.kensho2-service.svc.cluster.local:8099"
TSG_ENDPOINT = "http://powerai-time-series-group-api-3p.kensho2-service.svc.cluster.local:6666"
INSTANCE_ID_PLACEHOLDER = '__INSTANCE_ID__'
STORAGE_GW_ENDPOINT_PATTERN = "http://ma-gw-__INSTANCE_ID__.__INSTANCE_ID__.svc.cluster.local:8300"