import numpy as np
import pandas as pd

from sklearn import linear_model
from sklearn.metrics import mean_squared_error, r2_score

from decisionai_plugin.common.plugin_service import PluginService
from decisionai_plugin.common.util.constant import InferenceState
from decisionai_plugin.common.util.constant import STATUS_SUCCESS, STATUS_FAIL
from decisionai_plugin.common.util.timeutil import dt_to_str, str_to_dt, get_time_offset, get_time_list
from decisionai_plugin.common.util.data import generate_filled_missing_by_field
from decisionai_plugin.common.util.gran import Gran
from decisionai_plugin.common.util.fill_type import Fill

import datetime

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

    def do_verify(self, parameters, context):
        # Check series set permission
        for data in parameters['seriesSets']:
            meta = self.tsanaclient.get_metric_meta(parameters['apiEndpoint'], parameters['apiKey'], data['metricId'])

            if meta is None:
                return STATUS_FAIL, 'You have no permission to read Metric {}'.format(data['metricId'])

        return STATUS_SUCCESS, ''
        
    def do_inference(self, model_dir, parameters, series, context):
        results = []

        start_time, end_time, gran = self.get_inference_time_range(parameters)
        traceback_window = parameters['instance']['params']['tracebackWindow']

        for factor in series:
            timestamps = []
            values = []
            mses = []
            r2scores = []

            df = pd.DataFrame(factor.value, columns=factor.fields)
            df = df[['time', '__VAL__']]
            df.columns = ['timestamp', 'value']
            df['timestamp'] = df['timestamp'].apply(str_to_dt)
            for timestamp in get_time_list(start_time, end_time, gran):
                sub_df = df[df['timestamp'] < timestamp]
                sub_df = sub_df.iloc[-traceback_window:]
                
                x = sub_df['timestamp'].apply(lambda x: x.timestamp()).to_numpy().reshape(-1, 1)
                y = sub_df['value'].to_numpy().reshape(-1, 1)
                model = linear_model.LinearRegression().fit(x, y)
                y_new = model.predict(x)

                timestamps.append(dt_to_str(timestamp))
                values.append(model.predict(np.array([timestamp.timestamp()]).reshape(-1,1))[0][0])
                mses.append(mean_squared_error(y, y_new))
                r2scores.append(r2_score(y, y_new))

            dimension = dict(seriesId=factor.series_id)
            results.append(dict(metricId=parameters['instance']['target']['metrics'][0]['metricId'], dimension=dimension, timestamps=timestamps, values=values))
            results.append(dict(metricId=parameters['instance']['target']['metrics'][1]['metricId'], dimension=dimension, timestamps=timestamps, values=mses))
            results.append(dict(metricId=parameters['instance']['target']['metrics'][2]['metricId'], dimension=dimension, timestamps=timestamps, values=r2scores))

        return STATUS_SUCCESS, results, ''