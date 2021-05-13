
# Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

# Usage

## Use Maga plugin
    cd decisionAI-plugin 
    docker build -f maga-plugin.Dockerfile -t maga:latest .
    docker run -p 56789:56789 maga
## Use Forecast plugin
    cd decisionAI-plugin 
    docker build -f forecast-plugin.Dockerfile -t forecast:latest .
    docker run -p 56789:56789 forecast
## Implement your own plugin
    Inherit PluginService in common and implement do_verify, do_train, do_inference, do_delete, etc. if needed.
    Add apis to run_server, bind restful apis with your plugin.
    Add dockerfile for your plugin.

# API (MAGA)
## train
### URL: /multivariate/models/train
### Method: **POST**
### URL Params:
### Data Params(json):
    {"groupId":"8e826a5d-1b01-4ff4-a699-38bea97e17de","seriesSets":[{"seriesSetId":"b643e346-6883-4764-84a5-e63a3788eec9","metricId":"dc5b66cf-6dd0-4c83-bb8f-d849e68a7660","dimensionFilter":{"ts_code":"600030.SH"},"seriesSetName":"Stock price_high","metricMeta":{"granularityName":"Daily","granularityAmount":0,"datafeedId":"29595b1c-531f-445c-adcf-b75b2ab93c34","metricName":"high","datafeedName":"Stock price","dataStartFrom":1105315200000}},{"seriesSetId":"0d4cce4d-f4d4-4cef-be87-dbd28062abfc","metricId":"3274f7e6-683b-4d92-b134-0c1186e416a1","dimensionFilter":{"ts_code":"600030.SH"},"seriesSetName":"Stock price_change","metricMeta":{"granularityName":"Daily","granularityAmount":0,"datafeedId":"29595b1c-531f-445c-adcf-b75b2ab93c34","metricName":"change","datafeedName":"Stock price","dataStartFrom":1105315200000}}],"instance":{"instanceName":"Maga_Instance_1586447708033","instanceId":"528cbe52-cb6a-44c0-b388-580aba57f2f8","status":"Active","appId":"173276d9-a7ed-494b-9300-6dd1aa09f2c3","appName":"Maga","appDisplayName":"Maga","params":{"mergeMode":"Outer","tracebackWindow":28}},"startTime":"2020-03-05T00:00:00Z","endTime":"2020-05-18T00:00:00Z"}
### Response:
    If the params are verified failed(defined by plugin itself) or there's already a model in training state for the same app instance, this call will fail. Otherwise, a modelId will be returned.

    {
        "instanceId": "227052d2-efe7-45fb-8208-0183d193d49a",
        "message": "Training task created",
        "modelId": "b1eb6c34-b954-11ea-8401-e6942df0c224",
        "modelState": "Training",
        "result": "Success"
    }

## state
### URL: /multivariate/models/<*modleId*>
### Method: **GET**
### URL Params: modelId=[string]
### Data Params: None
### Response:
    {
        "instanceId": "",
        "message": "{\"modelId\": \"db9f9054-b954-11ea-bbbb-369b899e2f6a\", \"createdDateTime\": \"2020-06-28T15:34:30Z\", \"lastUpdatedDateTime\": \"2020-06-28T15:34:39Z\", \"summary\": {\"status\": \"READY\", \"errors\": [], \"modelDiagnoseInfo\": {\"column\": [\"vaeLoss\", \"forecastLoss\", \"validVaeLoss\", \"validForecastLoss\", \"latency\", \"epoc\"], \"values\": [[0.36485904455184937, 0.42150092124938965, 1.4000229835510254, 0.47028300166130066, 0.027468442916870117, 10.0], [0.11167675256729126, 0.07265239953994751, 1.4841482639312744, 0.0107630779966712, 0.029245376586914062, 20.0], [0.1011393666267395, 0.08118119835853577, 0.6297895908355713, 0.005929219536483288, 0.037653207778930664, 30.0], [0.07765479385852814, 0.06597049534320831, 0.40659427642822266, 0.017927056178450584, 0.02791619300842285, 40.0], [0.09029079228639603, 0.06250426918268204, 0.1961820125579834, 0.03054901771247387, 0.02936244010925293, 50.0], [0.07308734953403473, 0.05839956924319267, 0.10274775326251984, 0.031765230000019073, 0.0315244197845459, 60.0], [0.07333934307098389, 0.05786588788032532, 0.09845297038555145, 0.030415061861276627, 0.0293424129486084, 70.0], [0.07507610321044922, 0.057302508503198624, 0.07310837507247925, 0.02938724495470524, 0.03820085525512695, 80.0], [0.080164834856987, 0.05616353079676628, 0.05385735630989075, 0.02897876873612404, 0.038260459899902344, 90.0], [0.08177370578050613, 0.05543798208236694, 0.06642584502696991, 0.028607651591300964, 0.04195356369018555, 100.0]]}, \"variables\": [{\"variable\": \"2de4ac53-baba-46f0-92ac-c7660acf5398\", \"missingRatio\": 0.0, \"effectiveCount\": 239, \"startTime\": \"2019-07-01T00:00:00Z\", \"endTime\": \"2020-06-24T00:00:00Z\"}, {\"variable\": \"305e14ac-964a-4fd8-b2ad-3024e54f9779\", \"missingRatio\": 0.0, \"effectiveCount\": 239, \"startTime\": \"2019-07-01T00:00:00Z\", \"endTime\": \"2020-06-24T00:00:00Z\"}, {\"variable\": \"c9577ea1-d608-44df-b318-d68e783c73df\", \"missingRatio\": 0.0, \"effectiveCount\": 239, \"startTime\": \"2019-07-01T00:00:00Z\", \"endTime\": \"2020-06-24T00:00:00Z\"}, {\"variable\": \"fd2bbb63-e2b8-4807-b5e6-d878f79c4537\", \"missingRatio\": 0.0, \"effectiveCount\": 239, \"startTime\": \"2019-07-01T00:00:00Z\", \"endTime\": \"2020-06-24T00:00:00Z\"}]}, \"setupInfo\": {\"variable\": {\"2de4ac53-baba-46f0-92ac-c7660acf5398\": \"2de4ac53-baba-46f0-92ac-c7660acf5398.csv\", \"c9577ea1-d608-44df-b318-d68e783c73df\": \"c9577ea1-d608-44df-b318-d68e783c73df.csv\", \"fd2bbb63-e2b8-4807-b5e6-d878f79c4537\": \"fd2bbb63-e2b8-4807-b5e6-d878f79c4537.csv\", \"305e14ac-964a-4fd8-b2ad-3024e54f9779\": \"305e14ac-964a-4fd8-b2ad-3024e54f9779.csv\"}, \"tracebackWindow\": 28, \"mergeMode\": \"Outer\", \"fillMergeNAMethod\": \"Previous\", \"fillMergeNAValue\": 0, \"source\": \"https://tsana.blob.core.windows.net/maga/training_data_2020-06-27T_00_00_00Z?se=2020-06-29T15%3A34%3A29Z&sp=r&sv=2019-07-07&sr=b&sig=2jJDXuUawWh2qTYm2hd2N1hhY4WT9GLYANo%2BLQcNROE%3D\", \"startTime\": \"2019-07-01T00:00:00Z\", \"endTime\": \"2020-06-27T00:00:00Z\"}}",
        "modelId": "b1eb6c34-b954-11ea-8401-e6942df0c224",
        "modelState": "Ready",
        "result": "Success"
    }

## inference
### URL: /multivariate/models/<*modleId*>/inference
### Method: **POST**
### URL Params: modelId=[string]
### Data Params:
    {"groupId":"8e826a5d-1b01-4ff4-a699-38bea97e17de","seriesSets":[{"seriesSetId":"b643e346-6883-4764-84a5-e63a3788eec9","metricId":"dc5b66cf-6dd0-4c83-bb8f-d849e68a7660","dimensionFilter":{"ts_code":"600030.SH"},"seriesSetName":"Stock price_high","metricMeta":{"granularityName":"Daily","granularityAmount":0,"datafeedId":"29595b1c-531f-445c-adcf-b75b2ab93c34","metricName":"high","datafeedName":"Stock price","dataStartFrom":1105315200000}},{"seriesSetId":"0d4cce4d-f4d4-4cef-be87-dbd28062abfc","metricId":"3274f7e6-683b-4d92-b134-0c1186e416a1","dimensionFilter":{"ts_code":"600030.SH"},"seriesSetName":"Stock price_change","metricMeta":{"granularityName":"Daily","granularityAmount":0,"datafeedId":"29595b1c-531f-445c-adcf-b75b2ab93c34","metricName":"change","datafeedName":"Stock price","dataStartFrom":1105315200000}}],"gran":{"granularityString":"Daily","customInSeconds":0},"instance":{"instanceName":"Maga_Instance_1586447708033","instanceId":"528cbe52-cb6a-44c0-b388-580aba57f2f8","status":"Active","appId":"173276d9-a7ed-494b-9300-6dd1aa09f2c3","appName":"Maga","appDisplayName":"Maga","params":{"mergeMode":"Outer","tracebackWindow":28}},"startTime":"2020-03-05T00:00:00Z","endTime":"2020-05-18T00:00:00Z"}
### Response: 
    This call will fail due to any one of below 3 cases:
    1. Verify failed.
    2. Model not ready.
    3. Params or series set not consistent with model meta.

    Else, an inference task will be created.

    {
        "instanceId": "227052d2-efe7-45fb-8208-0183d193d49a",
        "message": "Inference task created",
        "modelId": "b1eb6c34-b954-11ea-8401-e6942df0c224",
        "modelState": "Ready",
        "result": "Success"
    }

    Result will be writen back to TSANA in inference_callback by default.

## delete
### URL: /multivariate/models/<*modleId*>
### Method: **DELETE**
### URL Params: modelId=[string]
### Data Params: None
### Response:
    {
        "instanceId": "",
        "message": "Model 14cf4b06-babf-11ea-9c89-e6942df0c224 has been deleted",
        "modelId": "14cf4b06-babf-11ea-9c89-e6942df0c224",
        "modelState": "Deleted",
        "result": "Success"
    }

## list_models
### URL: /multivariate/models
### Method: **GET**
### URL Params: None
### Data Params: None
### Response:
    [
        {
            "modelId": "2f0abb40-b928-11ea-a4f0-e6942df0c224",
            "state": "Training",
            "timekey": 1593339282.8674543
        },
        {
            "modelId": "2fdd7234-b523-11ea-a62f-2ea74eae5387",
            "state": "Ready",
            "timekey": 1592902065.961198
        },
        {
            "modelId": "3057b60c-b8ed-11ea-8d3e-76b4c9a4a697",
            "state": "Ready",
            "timekey": 1593415844.0857697
        },
        {
            "modelId": "39900a86-b603-11ea-b756-12b51d836f58",
            "state": "Ready",
            "timekey": 1593001023.5949621
        },
    ]

# Custom Plugin
## do_verify
### Params:
#### parameters:
    {"groupId":"8e826a5d-1b01-4ff4-a699-38bea97e17de","seriesSets":[{"seriesSetId":"b643e346-6883-4764-84a5-e63a3788eec9","metricId":"dc5b66cf-6dd0-4c83-bb8f-d849e68a7660","dimensionFilter":{"ts_code":"600030.SH"},"seriesSetName":"Stock price_high","metricMeta":{"granularityName":"Daily","granularityAmount":0,"datafeedId":"29595b1c-531f-445c-adcf-b75b2ab93c34","metricName":"high","datafeedName":"Stock price","dataStartFrom":1105315200000}},{"seriesSetId":"0d4cce4d-f4d4-4cef-be87-dbd28062abfc","metricId":"3274f7e6-683b-4d92-b134-0c1186e416a1","dimensionFilter":{"ts_code":"600030.SH"},"seriesSetName":"Stock price_change","metricMeta":{"granularityName":"Daily","granularityAmount":0,"datafeedId":"29595b1c-531f-445c-adcf-b75b2ab93c34","metricName":"change","datafeedName":"Stock price","dataStartFrom":1105315200000}}],"gran":{"granularityString":"Daily","customInSeconds":0},"instance":{"instanceName":"Maga_Instance_1586447708033","instanceId":"528cbe52-cb6a-44c0-b388-580aba57f2f8","status":"Active","appId":"173276d9-a7ed-494b-9300-6dd1aa09f2c3","appName":"Maga","appDisplayName":"Maga","params":{"mergeMode":"Outer","tracebackWindow":28}},"startTime":"2020-03-05T00:00:00Z","endTime":"2020-05-18T00:00:00Z"}
#### context: subscription
### Response:
    'Success/Fail', ''

## do_train
### Params:
#### model_dir: model output dir
#### parameters:
    {"groupId":"8e826a5d-1b01-4ff4-a699-38bea97e17de","seriesSets":[{"seriesSetId":"b643e346-6883-4764-84a5-e63a3788eec9","metricId":"dc5b66cf-6dd0-4c83-bb8f-d849e68a7660","dimensionFilter":{"ts_code":"600030.SH"},"seriesSetName":"Stock price_high","metricMeta":{"granularityName":"Daily","granularityAmount":0,"datafeedId":"29595b1c-531f-445c-adcf-b75b2ab93c34","metricName":"high","datafeedName":"Stock price","dataStartFrom":1105315200000}},{"seriesSetId":"0d4cce4d-f4d4-4cef-be87-dbd28062abfc","metricId":"3274f7e6-683b-4d92-b134-0c1186e416a1","dimensionFilter":{"ts_code":"600030.SH"},"seriesSetName":"Stock price_change","metricMeta":{"granularityName":"Daily","granularityAmount":0,"datafeedId":"29595b1c-531f-445c-adcf-b75b2ab93c34","metricName":"change","datafeedName":"Stock price","dataStartFrom":1105315200000}}],"gran":{"granularityString":"Daily","customInSeconds":0},"instance":{"instanceName":"Maga_Instance_1586447708033","instanceId":"528cbe52-cb6a-44c0-b388-580aba57f2f8","status":"Active","appId":"173276d9-a7ed-494b-9300-6dd1aa09f2c3","appName":"Maga","appDisplayName":"Maga","params":{"mergeMode":"Outer","tracebackWindow":28}},"startTime":"2020-03-05T00:00:00Z","endTime":"2020-05-18T00:00:00Z"}
#### context: subscription and model_id
### Response:
    'Success/Fail', ''

## do_inference
### Params:
#### model_dir: download model from AzureBlob to this dir for inference
#### parameters:
    {"groupId":"8e826a5d-1b01-4ff4-a699-38bea97e17de","seriesSets":[{"seriesSetId":"b643e346-6883-4764-84a5-e63a3788eec9","metricId":"dc5b66cf-6dd0-4c83-bb8f-d849e68a7660","dimensionFilter":{"ts_code":"600030.SH"},"seriesSetName":"Stock price_high","metricMeta":{"granularityName":"Daily","granularityAmount":0,"datafeedId":"29595b1c-531f-445c-adcf-b75b2ab93c34","metricName":"high","datafeedName":"Stock price","dataStartFrom":1105315200000}},{"seriesSetId":"0d4cce4d-f4d4-4cef-be87-dbd28062abfc","metricId":"3274f7e6-683b-4d92-b134-0c1186e416a1","dimensionFilter":{"ts_code":"600030.SH"},"seriesSetName":"Stock price_change","metricMeta":{"granularityName":"Daily","granularityAmount":0,"datafeedId":"29595b1c-531f-445c-adcf-b75b2ab93c34","metricName":"change","datafeedName":"Stock price","dataStartFrom":1105315200000}}],"gran":{"granularityString":"Daily","customInSeconds":0},"instance":{"instanceName":"Maga_Instance_1586447708033","instanceId":"528cbe52-cb6a-44c0-b388-580aba57f2f8","status":"Active","appId":"173276d9-a7ed-494b-9300-6dd1aa09f2c3","appName":"Maga","appDisplayName":"Maga","params":{"mergeMode":"Outer","tracebackWindow":28}},"startTime":"2020-03-05T00:00:00Z","endTime":"2020-05-18T00:00:00Z"}
#### context: subscription and model_id
### Response:
    'Success/Fail', ''

## do_delete
### Params:
#### subscription: apim-subscription-id
#### model_id: model id
### Response:
    'Success/Fail', ''

# TSANA API
## get_metric_meta
### To get the meta of a specific metric from TSANA
### Params:
#### api_endpoint: api endpoint for specific user
#### api_key: api key for specific user
#### metric_id: a UUID string
### Response:
    meta: the meta of the specified metric, or None if there is something wrong. 

## get_dimesion_values
### To get the dimension value of a specific dimension of a metric from TSANA
### Params:
#### api_endpoint: api endpoint for specific user
#### api_key: api key for specific user
#### metric_id: a UUID string
#### dimension_name: a string
### Response:
    dimension value: a string value of the specified dimension of a metric, or None if there is something wrong.

## get_timeseries
### Query time series from TSANA
### Params:
#### api_endpoint: api endpoint for specific user
#### api_key: api key for specific user
#### series_sets: Array of series set
#### start_time: inclusive, the first timestamp to be query
#### end_time: exclusive
#### offset: a number will be added to each timestamp of each time-series. The unit is defined by granularity
#### granularityName: if Offset > 0, the granularityName is Monthly / Weekly / Daily / Hourly / Minutely / Secondly / Custom
#### granularityAmount: if granularityName is Custom, granularityAmount is the seconds of the exact granularity
### Response:
    An array of Series object

## save_training_result
### Save a training result back to TSANA
### Params:
#### parameters: a dict object which should includes
    apiEndpoint: api endpoint for specific user
    apiKey: api key for specific user
    groupId: groupId in TSANA, which is copied from inference request, or from the entity
    instance: instance object, which is copied from the inference request, or from the entity
#### model_id: model id
#### model_state: model state(Training,Ready,Failed,Deleted)
#### message: detail message
### Response:
#### result: 'Success / Fail'
#### message: description for the result

## save_inference_result
### Save a inference result back to TSANA
### Params:
#### parameters: a dict object which should includes
    apiEndpoint: api endpoint for specific user
    apiKey: api key for specific user
    groupId: groupId in TSANA, which is copied from inference request, or from the entity
    instance: instance object, which is copied from the inference request, or from the entity
#### result: an array of inference result.
### Response:
#### result: 'Success / Fail'
#### message: description for the result

## save_data_points
### Save a inference result back to TSANA
### Params:
#### parameters: a dict object which should includes
    apiEndpoint: api endpoint for specific user
    apiKey: api key for specific user
    groupId: groupId in TSANA, which is copied from inference request, or from the entity
    instance: instance object, which is copied from the inference request, or from the entity
#### metric_id: a UUID string
#### dimensions: a dict includes dimension name and value
#### timestamps: an array of timestamps
#### values: an array of inference result values
### Response:
#### result: 'Success / Fail'
#### message: description for the result