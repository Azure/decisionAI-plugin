import enum
import os

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

IS_MT = True if os.environ.get('IS_MULTI_TENANCY', 'false') == 'true' else False
IS_INTERNAL = True if os.environ.get('MA_INTERNAL', 'false') == 'true' else False
EVENTHUB_USE_MI = True if os.environ.get('EVENTHUB_USE_MI', 'false') == 'true' else False
AZURE_ENVIRONMENT = os.environ.get('AZURE_ENVIRONMENT', 'AzureCloud')

INSTANCE_ID_KEY = 'x-instance-id'

NAME_SPACE = os.getenv('NANESPACE_SERVICE', 'metricsadvisor-mt')
META_ENDPOINT = "http://powerai-metadata3p-api.kensho2-service.svc.cluster.local:2000" if not IS_MT else f"http://powerai-metadata3p-api.{NAME_SPACE}.svc.cluster.local:2000"
INGESTION_ENDPOINT = "http://powerai-ingestion-api.kensho2-service.svc.cluster.local:8099" if not IS_MT else f"http://powerai-ingestion-api.{NAME_SPACE}.svc.cluster.local:8099"
TSG_ENDPOINT = "http://powerai-time-series-group-api-3p.kensho2-service.svc.cluster.local:6666" if not IS_MT else f"http://powerai-time-series-group-api-3p.{NAME_SPACE}.svc.cluster.local:6666"
INSTANCE_ID_PLACEHOLDER = '__INSTANCE_ID__'
STORAGE_GW_MT_ENDPOINT_PATTERN = f"http://gw-__INSTANCE_ID__.{NAME_SPACE}.svc.cluster.local:8300"
STORAGE_GW_ST_ENDPOINT_PATTERN = "http://storage-gw-server.kensho2-infra.svc.cluster.local:8300"

AZURE_STORAGE_ACCOUNT = os.environ.get('KENSHO2_BLOB_ACCOUNT' if IS_INTERNAL else 'AZURE_STORAGE_ACCOUNT')
AZURE_STORAGE_TABLE_KEY = os.environ.get('KENSHO2_BLOB_KEY' if IS_INTERNAL else 'AZURE_STORAGE_ACCOUNT_KEY')
AZURE_STORAGE_ACCOUNT_KEY = os.environ.get('KENSHO2_BLOB_KEY' if IS_INTERNAL else 'AZURE_STORAGE_ACCOUNT_KEY')
AZURE_STORAGE_ACCOUNT_DOMAIN = os.environ.get('KENSHO2_BLOB_DOMAIN' if IS_INTERNAL else 'AZURE_STORAGE_ACCOUNT_DOMAIN')
