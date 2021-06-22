import time
from JobController.jobs.renderer import generate_job
from JobController.utils import job_controller_api
from JobController.utils.k8s_api import get_running_pods_count, submit_job
from JobController.utils.kafka_operator import consume_loop
from os import environ
from JobController.configs import constant
environ['MON3_SERVER'] = constant.LOG
environ['MON3_APP'] = 'kensho2-training-pipeline'
environ['MON3_SERVICE'] = 'training-worker'
from telemetry import log
import traceback


def waiting_for_resources():
    while True:
        running_pods = get_running_pods_count()
        if running_pods >= constant.MAX_RUNNING_JOBS:
            log.info("## RunningPod = %s" % running_pods)
            log.info("Waiting for resources: sleeping %s s." % constant.WAITING_INTERVAL_SECONDS)
            time.sleep(constant.WAITING_INTERVAL_SECONDS)
        else:
            return running_pods


def process_message(msg):
    log.count("training_pipeline_training_in", 1)
    # log.info("Precessing message: %s" % msg)

    log.info("Checking resources.")
    running_pods = waiting_for_resources()
    log.info("## RunningPod = %s" % running_pods)

    log.info("Submitting job.")
    job_content = generate_job(
        job_id=msg['job_id'],
        algo_mode=msg['mode'],
        algo_name=msg['algorithm_name'],
        params=msg['params'],
    )

    job_controller_api.update_job_status(msg['job_id'], "Running")
    status = submit_job(job_content)
    log.info("Submit result: %s" % status)

    log.count("training_pipeline_training_out", 1)


def notify_error(message, err):
    job_id = message.value['job_id']
    info = "Submit to k8s failed: %s." % str(err)
    job_controller_api.update_job_status(job_id, "Failed", info)
    log.error("training_pipeline_training Updated job  [" + job_id + "] to failed." + str(err))


def main_loop():
    log.info("training_pipeline_training Start of main loop...")
    while True:
        try:
            consume_loop(process_message, constant.TrainingTopic, error_callback=notify_error)
        except Exception as e:
            log.error("training_pipeline_training Error in consume_loop. " + traceback.format_exc())
