from kafka.partitioner.default import DefaultPartitioner
import random
import threading
import json


class RoundRobinPartitioner(DefaultPartitioner):
    __topic_counter_map = dict()
    __DEFAULT_TOPIC = "default_topic"

    @classmethod
    def __call__(cls, key, all_partitions, available):
        """
        Get the partition corresponding to key
        :param key: partitioning key
        :param all_partitions: list of all partitions sorted by partition ID
        :param available: list of available partitions in no particular order
        :return: one of the values from all_partitions or available
        """
        if key is None:
            next_value = cls.__next_value()
            if available:
                return next_value % len(available)
            return next_value % len(all_partitions)

        return super().__call__(key, all_partitions, available)

    @classmethod
    def __next_value(cls, topic=None):
        if topic is None:
            topic = cls.__DEFAULT_TOPIC
        atomic_counter = cls.__topic_counter_map.get(topic, None)
        if atomic_counter is None:
            cls.__topic_counter_map[topic] = FastReadCounter()
            atomic_counter = cls.__topic_counter_map.get(topic)
        return atomic_counter.get_and_increment()

    def __str__(self):
        result = type(self).__name__ + "{"

        for k,v in self.__dict__.items():
            result += str(k) + ": " + str(v) + ", "
        result = result[:-2]
        result += "}"
        return result

class FastReadCounter:
    def __init__(self):
        self.value = int(random.random() * 2147483647)
        self._lock = threading.Lock()

    def increment(self):
        with self._lock:
            self.value += 1
            self.value &= 2147483647

    def get_and_increment(self):
        with self._lock:
            self.value += 1
            self.value &= 2147483647
            return self.value
