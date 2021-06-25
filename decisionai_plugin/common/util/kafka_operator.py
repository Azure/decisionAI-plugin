import json
import os
from kafka import KafkaConsumer, KafkaProducer
from telemetry import log
import time
import traceback

producer=None

# kafka topics
DeadLetterTopicFormat = "{base_topic}-dl"

def get_kafka_configs():
    kafka_configs = {"bootstrap_servers": os.environ['KAFKA_ENDPOINT']}
    # set secure config items if secure kafka enabled
    if 'KAFKA_SECURE' in os.environ and bool(os.environ['KAFKA_SECURE']) is True:
        sasl_password = os.environ['KAFKA_CONN_STRING']
        kafka_configs["security_protocol"] = "SASL_SSL"
        kafka_configs["sasl_mechanism"] = "PLAIN"
        kafka_configs["sasl_plain_username"] = "$ConnectionString"
        kafka_configs["sasl_plain_password"] = sasl_password

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
                                                'value_deserializer': lambda m: json.loads(m.decode('utf-8')                                        )
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
'''
def run():
    try:
        kafka_configs = InfraRt.get_kafka_configs()
        consumer_configs = {'group_id': config.KAFKA_GROUPID}
        consumer = KafkaConsumer(* config.KAFKA_TOPICS, ** {**kafka_configs, **consumer_configs})
        producer = KafkaProducer(** kafka_configs)
    except Exception as e:
        Logger.error('Kafka initialize error', e)
        raise e

    try:
        for record in consumer:
            try:
                consumer.commit()
                process_with_retry(record)

            except NonDeadletterError as e:
                Logger.warning('Skip sending deadletter, error message {}'.format(e))
            except Exception as e:
                try:
                    producer.send(config.KAFKA_DL_TOPIC, record.value, headers=record.headers)
                except Exception as e_produce:
                    Logger.error('Failed to send deadletter', e_produce)

    finally:
        consumer.close()

def redo_failure_all(start, end):
    parallel = 20
    q = queue.Queue()
    metrics_meta_dict = get_metrics_meta_all()
    for metricid, metricsmeta in metrics_meta_dict.items():
        q.put((start, end, metricid, metricsmeta))

    logger.info('Metrics count {} totally'.format(q.qsize()))
    messages = []
    def qconsumer():
        while not q.empty():
            try:
                taskinfo = q.get(block=False)
                msgs = get_redo_failure_messages_by_metric(* taskinfo)
                messages.extend(msgs)
            except queue.Empty as e:
                continue
            except Exception as e:
                logger.error('Error occurs, task info {}'.format(taskinfo))
                logger.exception(e)

    workers = [threading.Thread(target=qconsumer) for i in range(parallel)]
    for w in workers:
        w.start()
    for w in workers:
        w.join()

    logger.info("{} tasks will be scheduled".format(len(messages)))

    kafka_config = InfraRt.get_kafka_configs()
    producer = KafkaProducer(**kafka_config)
    for metricid, snapshot in messages:
        key = str.encode(metricid)
        value = str.encode(json.dumps(snapshot))
        producer.send(config.KAFKA_TOPIC, key=key, value=value)   

    producer.flush()
    logger.info('Successfully')
 '''   
if __name__ == "__main__":
    sample_msg = {
        "JobId": "",
        "Mode": "train",
        "TsName": "test-02",
        "AlgorithmName": "test-02",
        "InputPath": "",
        "OutputPath": ""
    }

    '''
    from kafka.admin import KafkaAdminClient, NewTopic

    admin_client = KafkaAdminClient(**get_kafka_configs())
    topic_list = []
    topic_list.append(NewTopic(name="example_topic", num_partitions=1, replication_factor=1))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    '''

    print("sending message...")
    for i in range(100):
        print(i)
        send_message('chuwan-test-topic', {"dataObjectID": i})