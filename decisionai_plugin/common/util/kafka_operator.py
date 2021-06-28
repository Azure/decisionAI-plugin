import json
import os
from kafka import KafkaConsumer, KafkaProducer
from telemetry import log
import time
import traceback
from configuration import Configuration, get_config_as_str
from constant import IS_INTERNAL, IS_MT

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

KAFKA_BOOTSTRAP_SERVERS = _get_endpoint_with_pattern('kafka').split(',') if IS_INTERNAL else os.environ['KAFKA_ENDPOINT']

def get_kafka_configs():
    if IS_MT:
        sasl_password = os.environ['KAFKA_CONN_STRING']
        kafka_configs = {"bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
                        "security_protocol": "SASL_SSL",
                        "sasl_mechanism": "PLAIN",
                        "sasl_plain_username": "$ConnectionString",
                        "sasl_plain_password": sasl_password
                        }
    else:
        kafka_configs = {"bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS}
    return kafka_configs

def send_message(topic, message):
    global producer
    kafka_configs = get_kafka_configs()
    if producer is None:
        producer = KafkaProducer(**{**kafka_configs,
                                    'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
                                    'retries': 5
                                    })
    future = producer.send(topic, message)
    future.get(10)
    producer.flush()

def append_to_failed_queue(message, err):
    errors = message.value.get('__ERROR__', [])
    errors.append(str(err))
    message.value['__ERROR__'] = errors
    return send_message(DeadLetterTopicFormat.format(base_topic=message.topic), message.value)

def consume_loop(process_func, topic, retry_limit=0, error_callback=None, config=None):
    log.info(f"Start of consume_loop for topic {topic}...")
    while True:
        try:
            kafka_configs = get_kafka_configs()
            if config is not None:
                kafka_configs.update(config)
            consumer = KafkaConsumer(topic, **{**kafka_configs,
                                                'group_id': 'job-controller-%s' % topic,
                                                'value_deserializer': lambda m: json.loads(m.decode('utf-8')),
                                                'max_poll_records': 1,
                                                'max_poll_interval_ms': 3600 * 6 * 1000
                                            })
            try:
                for message in consumer:
                    # log.info("Received message: %s" % str(message))
                    try:
                        process_func(message.value)
                        consumer.commit()
                    except Exception as e:
                        count = message.value.get('__RETRY__', 0)
                        if count >= retry_limit:
                            log.error("Exceed the maximum number of retries.")
                            if error_callback:
                                error_callback(message, e)
                            append_to_failed_queue(message, e)
                        else:
                            log.error("Processing message failed, will retry. Error message: " + str(e) + traceback.format_exc())
                            message.value['__RETRY__'] = count + 1
                            send_message(message.topic, message.value)
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