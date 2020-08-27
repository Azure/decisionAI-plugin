from common.plugin_service import PluginService
from common.util.constant import STATUS_SUCCESS, STATUS_FAIL
from common.util.timeutil import get_time_offset, str_to_dt, dt_to_str
from telemetry import log
import copy

class DemoService(PluginService):
    def __init__(self):
        super().__init__(False)

    def do_verify(self, parameters, context):
        # Check series set permission
        for data in parameters['seriesSets']:
            meta = self.tsanaclient.get_metric_meta(parameters['apiEndpoint'], parameters['apiKey'], data['metricId'])

            if meta is None:
                return STATUS_FAIL, 'You have no permission to read Metric {}'.format(data['metricId'])

        return STATUS_SUCCESS, ''

    def do_inference(self, model_dir, parameters, context):
        log.info('Start to inference {}'.format('Demo'))
        try:
            amplifier = parameters['instance']['params']['amplifier']
            end_time = str_to_dt(parameters['endTime'])
            if 'startTime' in parameters:
                start_time = str_to_dt(parameters['startTime'])
            else:
                start_time = end_time

            series = self.tsanaclient.get_timeseries(parameters['apiEndpoint'], parameters['apiKey'], parameters['seriesSets'], start_time, end_time)

            res = []
            for data in series or []:
                for value in data.value or []:
                    v = {
                        'dim': data.dim,
                        'metric_id': data.metric_id,
                        'series_id': data.series_id,
                        'value': value['value'] * amplifier,
                        'timestamp': value['timestamp']
                    }

                    res.append(v)

            self.tsanaclient.save_inference_result(parameters, res)

            return STATUS_SUCCESS, ''
        except Exception as e:
            log.error('Exception thrown by inference: ' + repr(e))
            return STATUS_FAIL, 'Exception thrown by inference: ' + repr(e)