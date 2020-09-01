import numpy as np
from sklearn import linear_model
from sklearn.metrics import mean_squared_error, r2_score

from common.plugin_service import PluginService
from common.util.constant import InferenceState
from common.util.constant import STATUS_SUCCESS, STATUS_FAIL
from common.util.timeutil import dt_to_str, str_to_dt, get_time_offset, get_time_list
from common.util.data import generate_filled_missing_by_field
from common.util.gran import Gran
from common.util.fill_type import Fill

class LrPluginService(PluginService):

    def __init__(self):
        super().__init__(False)

    def get_data_time_range(self, parameters, is_training=False):
        end_time = str_to_dt(parameters['endTime'])
        if 'startTime' in parameters:
            start_time = str_to_dt(parameters['startTime'])
        else:
            start_time = end_time

        min_start_time = start_time
        max_end_time = end_time
        for series_set in parameters['seriesSets']:
            metric_meta = series_set['metricMeta']
            gran = (metric_meta['granularityName'], metric_meta['granularityAmount'])
            data_end_time = get_time_offset(end_time, gran, + 1)
            trace_back_window = parameters['instance']['params']['tracebackWindow']
            data_start_time = get_time_offset(start_time, gran, -trace_back_window)
            if data_end_time > max_end_time:
                max_end_time = data_end_time
            if data_start_time < min_start_time:
                min_start_time = data_start_time

        return min_start_time, max_end_time

    def get_inference_time_range(self, parameters):
        end_time = str_to_dt(parameters['endTime'])
        if 'startTime' in parameters:
            start_time = str_to_dt(parameters['startTime'])
        else:
            start_time = end_time

        start_time_list = []
        for series_set in parameters['seriesSets']:
            metric_meta = series_set['metricMeta']
            gran = (metric_meta['granularityName'], metric_meta['granularityAmount'])
            start_time_list.append((get_time_offset(start_time, gran, -1), gran))

        max_start_time = max(start_time_list, key=lambda i: i[0])

        return start_time, end_time, max_start_time[1]

    def prepare_inference_data(self, parameters):
        start_time, end_time = self.get_data_time_range(parameters)

        factor_def = parameters['seriesSets']
        factors_data = self.tsanaclient.get_timeseries(parameters['apiEndpoint'], parameters['apiKey'], factor_def,
                                                       start_time, end_time, offset=0,
                                                       top=self.config.series_limit_per_series_set)

        #fill_missing = generate_filled_missing_by_field(factors_data, start_time, end_time, 'Custom', 300, Fill.Previous, 0)

        return factors_data

    def do_verify(self, parameters, context):
        # Check series set permission
        for data in parameters['seriesSets']:
            meta = self.tsanaclient.get_metric_meta(parameters['apiEndpoint'], parameters['apiKey'], data['metricId'])

            if meta is None:
                return STATUS_FAIL, 'You have no permission to read Metric {}'.format(data['metricId'])

        return STATUS_SUCCESS, ''
        
    def do_inference(self, model_dir, parameters, context):
        results = []
        factors_data = self.prepare_inference_data(parameters)
        start_time, end_time, gran = self.get_inference_time_range(parameters)

        traceback_window = parameters['instance']['params']['tracebackWindow']
        for timestamp in get_time_list(start_time, end_time, gran):
            single_point = []
            for factor in factors_data:
                x = np.array([tuple['timestamp'].timestamp() for tuple in factor.value if tuple['timestamp'] < timestamp])[-traceback_window:].reshape(-1, 1)
                y = np.array([tuple['value'] for tuple in factor.value if tuple['timestamp'] < timestamp])[-traceback_window:]
                
                model = linear_model.LinearRegression().fit(x, y)
                y_new = model.predict(x)
                
                single_point.append(dict(seriesId=factor.series_id,value=model.predict(np.array([timestamp.timestamp()]).reshape(-1,1))[0],mse=mean_squared_error(y, y_new),r2score=r2_score(y, y_new)))
            results.append(dict(timestamp=dt_to_str(timestamp),status=InferenceState.Ready.name,result=single_point))
        
        status, message = self.tsanaclient.save_inference_result(parameters, results)
        if status != STATUS_SUCCESS:
            raise Exception(message)

        return STATUS_SUCCESS, ''