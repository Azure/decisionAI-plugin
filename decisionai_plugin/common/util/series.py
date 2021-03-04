class Series:
    def __init__(self, metric_id, series_id, dim, fields, value):
        self.metric_id = metric_id
        self.series_id = series_id
        #dim dict
        self.dim = dim
        #1-d string array
        #['time', '__VAL__', '__FIELD__.ExpectedValue', '__FIELD__.IsAnomaly', '__FIELD__.PredictionValue', '__FIELD__.PredictionModelScore', '__FIELD__.IsSuppress', '__FIELD__.Period', '__FIELD__.CostPoint', '__FIELD__.Mean', '__FIELD__.STD', '__FIELD__.TrendChangeAnnotate', '__FIELD__.TrendChang...tateIgnore', '__FIELD__.AnomalyAnnotate', ...]
        self.fields = fields
        #2-d array
        #[['2020-10-12T17:55:00Z', 1.0, None, None, None, None, None, None, None, None, None, None, None, None, ...]]
        self.value = value