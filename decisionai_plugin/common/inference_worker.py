import time
import json
from JobController.utils import job_controller_api
from JobController.utils.kafka_operator import consume_loop
from os import environ
from JobController.configs import constant
environ['MON3_SERVER'] = constant.LOG
environ['MON3_APP'] = 'kensho2-training-pipeline'
environ['MON3_SERVICE'] = 'inference-worker'
from telemetry import log
import traceback


def call_algorithm_service(mode, algo, params, job_id):
    try:
        if algo == 'KmeansAnomalyDetection':
            from Algorithms.KmeansAnomalyDetection.KmeansAnomalyDetection import run
            run(mode, params, job_id)
        elif algo == 'OmniAnomalyDetection':
            from Algorithms.OmniAnomalyDetection.OmniAnomalyDetection import run
            run(mode, params, job_id)
        elif algo == 'Forecast':
            from Algorithms.forecast.forecast_worker import run
            run(mode, params, job_id)
        elif algo == "Correlation":
            from Algorithms.correlation.correlation_worker import run
            run(mode, params, job_id)
        elif algo == 'MagaAnomalyDetection':
            from Algorithms.maganet.maga_worker import run
            run(mode, params, job_id)
    except:
        raise Exception("Call algorithm service failed: " + traceback.format_exc())


def process_message(msg):
    log.count("training_pipeline_inference_in", 1)
    log.info("receive inference msg " + json.dumps(msg))
    job_id = msg['job_id']
    job_controller_api.update_job_status(job_id, "Running")

    call_algorithm_service(msg['mode'], msg['algorithm_name'], json.loads(msg['params']), job_id)

    job_controller_api.update_job_status(job_id, "Success")

    log.count("training_pipeline_inference_out", 1)


def notify_error(message, err):
    job_id = message.value['job_id']
    info = "Error: %s." % str(err)
    job_controller_api.update_job_status(job_id, "Failed", info)
    log.error("training_pipeline_inference Updated job [" + job_id + "] to failed." + str(err))


def main_loop(working_topic):
    log.info("training_pipeline_inference Start of main loop " + working_topic)
    time.sleep(10)
    # msg = {
    #     'job_id': 'x',
    #     'mode': 'inference',
    #     'algorithm_name': '',
    #     'params': "{\"gran\": {\"customInSeconds\": 300,\"granularityString\": \"Custom\"},\"series\": [{\"metricId\": \"9e18dce1-8c97-44cb-974e-726262d29cc6\",\"seriesId\": \"bc86cd2cdf4dd4e17a077ec0207eb3df\",\"dimensions\": {\"tag\": \"1\"}},{\"metricId\": \"c0a9a2c1-934c-4ee5-b419-541647ab5b4e\",\"seriesId\": \"a87cd0382256ee33f8b3896bafd9cc6f\",\"dimensions\": {\"tag\": \"1\"}},{\"metricId\": \"f4b6269d-dfe1-4b2e-874a-e0e7fcb2a8b7\",\"seriesId\": \"1852d819355d7e60d0c3abd40d0892e0\",\"dimensions\": {\"tag\": \"1\"}},{\"metricId\": \"8dbd9cb6-af16-49db-87e8-129024f9853b\",\"seriesId\": \"17ad67d1695944bf2d4aa92660c20e16\",\"dimensions\": {\"tag\": \"1\"}},{\"metricId\": \"befea618-71ec-491a-a895-187d18afb13f\",\"seriesId\": \"c6d383786d7dbec6587da3470c570b12\",\"dimensions\": {\"tag\": \"1\"}},{\"metricId\": \"3e3f0e15-f284-4f94-a9bc-d55c8d8df03c\",\"seriesId\": \"d8ba2c4a9f5c57b17bc60bc1e8b45024\",\"dimensions\": {\"tag\": \"1\"}},{\"metricId\": \"6b7759a4-c964-4efc-ba08-06cad2431fcb\",\"seriesId\": \"ebec0a98c81116e0a31800e726501923\",\"dimensions\": {\"tag\": \"1\"}},{\"metricId\": \"68e1a74e-8f0a-40a6-a036-7d48bd1a55a3\",\"seriesId\": \"d99f61ab7f339cbdc3e1796acfc3ec2a\",\"dimensions\": {\"tag\": \"1\"}},{\"metricId\": \"6e2e8283-a5de-4683-acf9-e72cb2351651\",\"seriesId\": \"e1284c340a78c821e9707000d7de7bdd\",\"dimensions\": {\"tag\": \"1\"}},{\"metricId\": \"8fc7c268-0c86-4206-8954-52628d1d2b9e\",\"seriesId\": \"003f01160ea66dc16d269f84521c2f03\",\"dimensions\": {\"tag\": \"1\"}},{\"metricId\": \"47f17165-d0cf-45c2-8cd9-b29d9007aff6\",\"seriesId\": \"af65b8d67fbbd0eaa609cb62b5b6eef2\",\"dimensions\": {\"tag\": \"1\"}},{\"metricId\": \"0b43cde5-21f5-4ad8-ab88-1898bdc0ce5d\",\"seriesId\": \"dc9d452b305115ed221938eb51d4668c\",\"dimensions\": {\"tag\": \"1\"}},{\"metricId\": \"34cb23c4-d1e9-43c2-8fe3-b5a046a9ea65\",\"seriesId\": \"bd25329d982c38eac9b6900c5e908285\",\"dimensions\": {\"tag\": \"1\"}},{\"metricId\": \"7d61b97b-10d3-4931-af09-cf45b32bea8a\",\"seriesId\": \"659b4906b16cd8c2526ec856ce9ee50c\",\"dimensions\": {\"tag\": \"1\"}},{\"metricId\": \"a6f23f34-7237-438f-80fb-55404bd1d6e3\",\"seriesId\": \"d00308c0ed6c7cd8f2a29ab30c1c6cf1\",\"dimensions\": {\"tag\": \"1\"}},{\"metricId\": \"9f80b9fa-f14c-40aa-b6d0-b05eaa9107ad\",\"seriesId\": \"691d8b152fa9e1851195fa2c2c4735ea\",\"dimensions\": {\"tag\": \"1\"}},{\"metricId\": \"c6c80624-2e6e-47f1-9c16-787b9a78975c\",\"seriesId\": \"2392b2ad9244260e9eb8cd38f29b7bd7\",\"dimensions\": {\"tag\": \"1\"}},{\"metricId\": \"4662a258-ff0d-4245-99ec-094e2133c834\",\"seriesId\": \"e4d6da06cca14107dea937b14e304a4a\",\"dimensions\": {\"tag\": \"1\"}}],\"endTime\": \"2020-01-05T12:10:00Z\",\"groupId\": \"bf6d0057-31c2-4cb2-b78c-709ba9c21009\",\"modelId\": \"1058\",\"instance\": {\"appId\": \"f6b781d9-f27d-4707-bdc6-1775a11fad9d\",\"params\": {\"trainId\": \"57fde394-9390-42ed-851d-bb349f1a8eb0\",\"windowSize\": 288,\"missingRatio\": 0.1,\"waitInSeconds\": 900},\"status\": \"Active\",\"appName\": \"KmeansAnomalyDetection\",\"hookIds\": [],\"instanceId\": \"4650b78d-3b60-4aa9-ba7f-4b3e70233500\",\"instanceName\": \"K-means based Multi-variate Anomaly Detection_Instance_1576900877021\",\"appDisplayName\": \"K-means based Multi-variate Anomaly Detection\"},\"startTime\": \"2020-01-05T12:05:00Z\"}"
    # }
    # process_message(msg)
    while True:
        try:
            config = None
            if working_topic == 'multivariate-omni-inference':
                config = {
                    'max_poll_records': 1,
                    'max_poll_interval_ms': 3600 * 6 * 1000
                }
            consume_loop(process_message, working_topic, error_callback=notify_error, config=config)
        except:
            log.error("training_pipeline_inference Error in consume_loop. " + traceback.format_exc())
