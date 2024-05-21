import json
import os
from confluent_kafka import Consumer, Producer, KafkaException
from telemetry import log
import time
import traceback
from .configuration import Configuration, get_config_as_str
from .constant import IS_INTERNAL, IS_MT, EVENTHUB_USE_MI, AZURE_ENVIRONMENT
from .managedidentityauthhelper import ManagedIdentityAuthHelper
import json

producer=None

# kafka topics
DeadLetterTopicFormat = "{base_topic}-dl"

# get config info
def _get_endpoint_with_pattern(name):

    config_dir = os.environ['KENSHO2_CONFIG_DIR']
    endpoints = Configuration(config_dir + 'endpoints.ini')

    kvs = endpoints[name]
    if 'endpoint' in kvs:
        return kvs['endpoint']
    elif 'endpoint-pattern' in kvs:
        num_replicas = int(get_config_as_str(('%ssystem/%s/replicas' % (config_dir, name)).strip()))
        pattern = kvs['endpoint-pattern']
        val = ','.join(map(lambda x: pattern % (x), range(num_replicas)))
        print("kafka-endpoint: " + val)
        return val 
    else:
        raise Exception('missing endpoint for %s' % (name))

KAFKA_BOOTSTRAP_SERVERS = _get_endpoint_with_pattern('kafka') if IS_INTERNAL else os.environ['KAFKA_ENDPOINT']

def get_kafka_configs():
    if IS_MT or not IS_INTERNAL:
        if EVENTHUB_USE_MI:
            auth = ManagedIdentityAuthHelper(AZURE_ENVIRONMENT, KAFKA_BOOTSTRAP_SERVERS.split(","))
            kafka_configs = {
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "OAUTHBEARER",
                "oauth_cb": auth.token,
            }
        else:
            sasl_password = os.environ['KAFKA_CONN_STRING']
            kafka_configs = {
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN",
                "sasl.username": "$ConnectionString",
                "sasl.password": sasl_password
            }
    else:
        kafka_configs = {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}
    return kafka_configs

def send_message(topic, message):
    global producer
    kafka_configs = get_kafka_configs()
    if producer is None:
        producer = Producer(**{**kafka_configs,
                                    'retries': 5
                                    })
    try:
        producer.produce(topic, json.dumps(message).encode('utf-8'))
        producer.flush()
        log.count("write_to_kafka", 1,  topic=topic, result='Success')
    except Exception as e:
        producer = None
        log.count("write_to_kafka", 1,  topic=topic, result='Failed')
        log.error("Produce message failed. Error message: " + str(e))

def append_to_failed_queue(message, err):
    record_value = json.loads(message.value().decode('utf-8'))
    errors = record_value.get('__ERROR__', [])
    errors.append(str(err))
    record_value['__ERROR__'] = errors
    return send_message(DeadLetterTopicFormat.format(base_topic=message.topic), record_value)

def consume_loop(process_func, topic, retry_limit=0, error_callback=None, config=None):
    log.info(f"Start of consume_loop for topic {topic}...")
    while True:
        try:
            kafka_configs = get_kafka_configs()
            if config is not None:
                kafka_configs.update(config)

            consumer_configs = {
                **kafka_configs,
                'group.id': 'job-controller-v2-%s' % topic,
                'max.poll.interval.ms': 3600 * 6 * 1000,
                'enable.auto.commit': False
            }

            consumer = Consumer(consumer_configs)
            
            def print_assignment(consumer, partitions):
                log.info('Assignment:', partitions)
            
            consumer.subscribe([topic], on_assign=print_assignment)

            try:
                while True:
                    message = consumer.poll(timeout=1.0)
                    if message is None:
                        continue
                    if message.error():
                        raise KafkaException(message.error())
                    else:
                        # log.info("Received message: %s" % str(message))
                        log.count("read_from_kafka", 1,  topic=topic)
                        log.duration("read_from_kafka", 1,  topic=topic)
                        try:
                            record_value = json.loads(message.value().decode('utf-8'))
                            process_func(record_value)
                            consumer.commit()
                        except Exception as e:
                            count = record_value.get('__RETRY__', 0)
                            if count >= retry_limit:
                                log.error("Exceed the maximum number of retries.")
                                if error_callback:
                                    error_callback(message, e)
                                append_to_failed_queue(message, e)
                            else:
                                log.error("Processing message failed, will retry. Error message: " + str(e) + traceback.format_exc())
                                record_value['__RETRY__'] = count + 1
                                send_message(message.topic, record_value)
            finally:
                consumer.close()
        except Exception as e:
            log.error(f"Error in consume_loop for topic {topic}. " + traceback.format_exc())
            time.sleep(10)

if __name__ == "__main__":
    sample_msg = {
        "JobId": "",
        "Mode": "train",
        "TsName": "test-02",
        "AlgorithmName": "test-02",
        "InputPath": "",
        "OutputPath": ""
    }

    print("sending message...")
    for i in range(100):
        print(i)
        send_message('chuwan-test-topic', {"dataObjectID": i})