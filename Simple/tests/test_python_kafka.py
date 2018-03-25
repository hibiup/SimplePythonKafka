from unittest import TestCase

import json

'''
Create topic before test:

$ /apps/kafka/default/bin/kafka-topics.sh \
    --zookeeper zookeeper01:2181 \
    --create --topic test-topic \
    --replication-factor 1 \
    --partitions 3

$ /apps/kafka/default/bin/kafka-topics.sh \
    --zookeeper zookeeper01:2181 \
    --list

Command line producer to publish message:
$ /apps/kafka/default/bin/kafka-console-producer.sh \
    --broker-list kafka01:9092 \
    --topic test-topic
'''

class TestPythonKafka(TestCase):
    def test_confluent_consumer(self):
        '''
        # yum install librdkafka-devel
        # pip install confluent_kafka
        '''
        from confluent_kafka import Consumer, KafkaError

        consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'test-group',
            'default.topic.config': {
                'auto.offset.reset': 'smallest'
            }
        })
        consumer.subscribe(['test-topic'])

        while True:
            msg = consumer.poll()

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            print('Received message: {}'.format(msg.value().decode('utf-8')))

        consumer.close()


    def test_consumer(self):
        '''
        # pip install kafka-python, msgpack-python
        '''
        # To consume latest messages and auto-commit offsets
        from kafka import KafkaConsumer
        import msgpack

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