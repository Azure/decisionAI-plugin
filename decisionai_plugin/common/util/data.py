import pandas as pd
import numpy as np
from functools import reduce

from .constant import VALUE, TIMESTAMP
from .fill_type import Fill
from .gran import Gran
from .timeutil import dt_to_str, str_to_dt, get_time_offset, convert_freq

def normalize(df, normalize_base=None):
    def max_min_scaler(x, base):
        maxx = np.max(x)
        minn = np.min(x)
        if base:
            maxx = base['max']
            minn = base['min']
        if maxx != minn:
            return (x - minn) / (maxx - minn)
        else:
            x[:] = 1
            return x
    data = pd.DataFrame(index=df.index)
    for item in df.columns:
        if item == 'timestamp':
            data[item] = df[item]
            continue
        base = normalize_base[item] if normalize_base is not None and item in normalize_base else None
        data[item] = df[[item]].apply(lambda x: max_min_scaler(x, base))
    return data

def fill_missing(input_series, fill_type: Fill, fill_value):
    if fill_type == Fill.NotFill:
        return input_series
    if fill_type == Fill.Previous:
        return input_series.fillna(method='ffill', limit=len(input_series)).fillna(method='bfill', limit=len(input_series))
    if fill_type == Fill.Subsequent:
        return input_series.fillna(method='bfill', limit=len(input_series)).fillna(method='ffill', limit=len(input_series))
    if fill_type == Fill.Linear:
        return input_series.interpolate(method='linear', limit_direction='both', axis=0, limit=len(input_series))
    if fill_type == Fill.Pad:
        return input_series.fillna(fill_value)

    return input_series.fillna(0)

def generate_filled_missing_by_time_range(input_frame, start_time, end_time, gran, custom_in_seconds, fill_type: Fill, fill_value):
    if fill_type == Fill.NotFill:
        return input_frame
    full_data_range = pd.date_range(start=start_time, end=end_time, freq=convert_freq(gran, custom_in_seconds))
    full_data_range = pd.DataFrame(full_data_range, columns=[TIMESTAMP])
    full_data_range[TIMESTAMP] = full_data_range[TIMESTAMP].dt.tz_localize(None)
    input_frame = pd.merge(full_data_range, input_frame, how='left', on=TIMESTAMP)
    return fill_missing(input_frame, fill_type, fill_value)

def generate_filled_missing_by_period(input_frame, end_time, gran, custom_in_seconds, periods, fill_type: Fill, fill_value):
    if fill_type == Fill.NotFill:
        return input_frame
    full_data_range = pd.date_range(end=end_time, freq=convert_freq(gran, custom_in_seconds), periods=periods)
    full_data_range = pd.DataFrame(full_data_range, columns=[TIMESTAMP])
    full_data_range[TIMESTAMP] = full_data_range[TIMESTAMP].dt.tz_localize(None)
    input_frame = pd.merge(full_data_range, input_frame, how='left', on=TIMESTAMP)
    return fill_missing(input_frame, fill_type, fill_value)

def generate_inner_join_frame(input_frames):
    return reduce(lambda left, right: pd.merge(left, right, on=TIMESTAMP, how='inner'), input_frames)

def generate_outer_join_frame(input_frames, fill_type: Fill, fill_value):
    if fill_type == Fill.NotFill:
        return generate_inner_join_frame(input_frames)
    merged = reduce(lambda left, right: pd.merge(left, right, on=TIMESTAMP, how='outer'), input_frames)
    return fill_missing(merged, fill_type, fill_value)

# series_data should have this format: [{"series_id":"xxx","metric_id":"xxx","dim":{xxx},"value":[{"timestamp":xxx,"value":xxx,"fieldxxx":xxx},...]]
def generate_filled_missing_by_field(series_data, start, end, gran, custom_in_seconds, fill_type:Fill, fill_value, fields=None):
    if fields is None:
        fields = []
    fields.insert(0, VALUE)

    data_panel = {}

    full_data_range = pd.date_range(start=start, end=get_time_offset(end, (gran, custom_in_seconds), -1), freq=convert_freq(Gran[gran], custom_in_seconds))
    full_data_range = pd.DataFrame(full_data_range, columns=[TIMESTAMP])
    full_data_range[TIMESTAMP] = full_data_range[TIMESTAMP].dt.tz_localize(None)
    # set index to improve merging performance
    full_data_range = full_data_range.set_index(TIMESTAMP)

    series_info = []
    for series in series_data:
        if series is None:
            pass

        metric_id = series.metric_id
        series_id = series.series_id
        taglist = series.dim
        series_info.append([metric_id, series_id, taglist])
        
        ts_df = pd.DataFrame([value[TIMESTAMP] for value in series.value], columns=[TIMESTAMP])
        ts_df[TIMESTAMP] = ts_df[TIMESTAMP].dt.tz_localize(None)
        ts_df.set_index(TIMESTAMP, inplace=True)

        for field in fields:
            ts_df[series_id] = pd.DataFrame([value[field] for value in series.value], columns=[series_id])            
            ts_df = pd.merge(full_data_range, ts_df, how='left', on=TIMESTAMP)

            if field not in data_panel:
                data_panel[field] = ts_df
            else:
                data_panel[field][series_id] = ts_df[series_id]

    timestamps = []
    cols_len = len(data_panel[VALUE].columns.values.tolist())
    for index, row in data_panel[VALUE].iterrows():
        not_missing = False
        for i in range(cols_len):
            if pd.notna(row[i]):
                not_missing = True
                break
        timestamps.append([index, not_missing])

    for field in fields:
        if field == VALUE:
            data_panel[field] = fill_missing(data_panel[field], fill_type, fill_value)
        else:
            data_panel[field] = data_panel[field].fillna(0)
        data_panel[field].reset_index(drop=False, inplace=True)
    
    return data_panel, series_info, timestamps