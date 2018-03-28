from unittest import TestCase

from kafka import KafkaConsumer
import msgpack
import json

'''
Before test:
$ /apps/kafka/default/bin/kafka-topics.sh \
    --zookeeper zookeeper01:2181 \
    --create --topic test-topic \
    --replication-factor 1 \
    --partitions 3

$ /apps/kafka/default/bin/kafka-topics.sh \
    --zookeeper zookeeper01:2181 \
    --list
'''
class TestPythonKafka(TestCase):
    def test_consumer(self):
        '''
        Command line producer to publish message:
        $ /apps/kafka/default/bin/kafka-console-producer.sh \
            --broker-list kafka01:9092 \
            --topic test-topic
        '''
        # To consume latest messages and auto-commit offsets
        consumer = KafkaConsumer('test-topic',
                                group_id='test-group',
                                bootstrap_servers=['localhost:9092'],
                                api_version=(1, 0, 1))   # 必须加上api-version，这是因为一个BUG： https://github.com/dpkp/kafka-python/issues/1308
        for message in consumer:
            # message value and key are raw bytes -- decode if necessary!
            # e.g., for unicode: `message.value.decode('utf-8')`
            print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                message.offset, message.key,
                                                message.value))
        print("End")