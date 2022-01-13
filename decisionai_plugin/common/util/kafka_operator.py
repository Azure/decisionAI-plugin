import json
import os
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaTimeoutError
from telemetry import log
import time
import traceback
from .configuration import Configuration, get_config_as_str
from .constant import IS_INTERNAL, IS_MT
import json
from .kafka_util import RoundRobinPartitioner
from azure.eventhub import EventHubProducerClient, EventHubConsumerClient, EventData
from azure.eventhub.exceptions import OperationTimeoutError

producer = None
# <topic-producer> map
producer_map = dict()
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
        sasl_password = os.environ['KAFKA_CONN_STRING']
        kafka_configs = {"bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
                         "security_protocol": "SASL_SSL",
                         "sasl_mechanism": "PLAIN",
                         "sasl_plain_username": "$ConnectionString",
                         "sasl_plain_password": sasl_password,
                         }
    else:
        kafka_configs = {"bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS}
    return kafka_configs

def get_eventhubs_configs():
    sasl_password = os.environ['KAFKA_CONN_STRING']
    eventhubs_configs = {
        "conn_str": sasl_password,
    }
    return eventhubs_configs

def send_message(topic, message, retry=3):
    global producer_map

    producer = None
    retry -= 1

    while True:
        try:
            if topic not in producer_map or producer_map[topic] is None:
                eventhubs_configs = get_eventhubs_configs()
                producer_map[topic] = EventHubProducerClient.from_connection_string(**{**eventhubs_configs,
                                                                                       "maxRetries": 5,
                                                                                       })
            producer = producer_map[topic]
            event_data_batch = producer.create_batch()
            event_data_batch.add(EventData(json.dumps(message)))
            producer.send_batch(event_data_batch)

        except Exception as e:
            if producer is not None:
                producer.close()
                producer_map[topic] = None
            if isinstance(e, OperationTimeoutError) and retry:
                retry -= 1
                log.warn(f"Event Hubs producer send failed. Error: {str(e)}")
                continue
            else:
                log.count("write_to_eventhubs", 1, topic=topic, result='Failed')
                log.error(f"Event Hubs producer send failed. Error: {str(e)}")
                raise e


# def send_message(topic, message, err_callback=None, retry=3):
#     global producer
#
#     # keep consistency
#     retry -= 1
#
#     while True:
#         try:
#             if producer is None:
#                 kafka_configs = get_kafka_configs()
#                 producer = KafkaProducer(**{**kafka_configs,
#                                             'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
#                                             'retries': 5,
#                                             "partitioner": RoundRobinPartitioner()
#                                             })
#             if err_callback is not None:
#                 producer.send(topic, message).add_errback(err_callback, message)
#             else:
#                 future = producer.send(topic, message)
#                 producer.flush()
#                 # wait 10 seconds for kafka writing completed!
#                 future.get(10)
#             log.count("write_to_kafka", 1, topic=topic, result='Success')
#             break
#         except Exception as e:
#             if producer is not None:
#                 producer.close()
#                 producer = None
#             if isinstance(e, KafkaTimeoutError) and retry:
#                 retry -= 1
#                 log.info("Kafka producer retries.")
#                 continue
#             else:
#                 log.count("write_to_kafka", 1, topic=topic, result='Failed')
#                 log.error(f"Kafka producer send failed. Error: {str(e)}")
#                 raise e


def append_to_failed_queue(message, err):
    errors = message.value.get('__ERROR__', [])
    errors.append(str(err))
    message.value['__ERROR__'] = errors
    return send_message(DeadLetterTopicFormat.format(base_topic=message.topic), message.value)


# def consume_loop(process_func, topic, retry_limit=0, error_callback=None, config=None):
#     log.info(f"Start of consume_loop for topic {topic}...")
#     while True:
#         try:
#             kafka_configs = get_kafka_configs()
#             if config is not None:
#                 kafka_configs.update(config)
#             log.info("kafka configs: " + json.dumps(kafka_configs))
#             consumer = KafkaConsumer(topic, **{**kafka_configs,
#                                                'group_id': 'job-controller-v2-%s' % topic,
#                                                'value_deserializer': lambda m: json.loads(m.decode('utf-8')),
#                                                'max_poll_records': 1,
#                                                'max_poll_interval_ms': 3600 * 6 * 1000,
#                                                'enable_auto_commit': False,
#                                                })
#             try:
#                 for message in consumer:
#                     # log.info("Received message: %s" % str(message))
#                     try:
#                         log.duration("message_latency", time.time() - message.timestamp / 1000, topic=topic,
#                                      partition=message.partition)
#                         log.count("read_from_kafka", 1, topic=topic, partition=message.partition)
#                         process_func(message.value)
#                         consumer.commit()
#                     except Exception as e:
#                         count = message.value.get('__RETRY__', 0)
#                         if count >= retry_limit:
#                             log.error("Exceed the maximum number of retries.")
#                             if error_callback:
#                                 error_callback(message, e)
#                             append_to_failed_queue(message, e)
#                         else:
#                             log.error("Processing message failed, will retry. Error message: " + str(
#                                 e) + traceback.format_exc())
#                             message.value['__RETRY__'] = count + 1
#                             send_message(message.topic, message.value)
#             finally:
#                 consumer.close()
#         except Exception as e:
#             log.error(f"Error in consume_loop for topic {topic}. " + traceback.format_exc())
#             time.sleep(10)

def consume_loop(process_func, topic, retry_limit=0, error_callback=None, config=None):
    log.info(f"Start of consume_loop for topic {topic}...")

    def process_function_wrapper(partition_context, event):

        message = event.body_as_json(encoding="UTF-8")
        try:
            process_func(message)
        except Exception as e:
            count = message.get('__RETRY__', 0)
            if count >= retry_limit:
                log.error("Exceed the maximum number of retries.")
                if error_callback:
                    error_callback(message, e)
                append_to_failed_queue(message, e)
            else:
                log.error("Processing message failed, will retry. Error message: " + str(
                    e) + traceback.format_exc())
                message['__RETRY__'] = count + 1
                send_message(partition_context.eventhub_name, message)


    while True:
        try:
            eventhubs_configs = get_eventhubs_configs()
            if config is not None:
                eventhubs_configs.update(config)
            log.info("Event Hubs configs: " + json.dumps(eventhubs_configs))
            consumer = EventHubConsumerClient.from_connection_string(**{**eventhubs_configs,
                                                                        "consumer_group": 'job-controller-v2-%s' % topic,
                                                                        "eventhub_name": topic,
                                                                        })
            try:
                with consumer:
                    consumer.receive(on_event=process_function_wrapper)
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
