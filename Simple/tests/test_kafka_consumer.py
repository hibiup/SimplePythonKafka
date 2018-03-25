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
        self.__create_conflient_consumer(1, lambda id, msg: print("Consumer %d: Received message: %s" % (id, msg.value().decode('utf-8'))))
        

    def __create_conflient_consumer(self, id, message):
        '''
        https://github.com/confluentinc/confluent-kafka-python/blob/master/README.md

        # yum install librdkafka-devel
        # pip install confluent_kafka
        '''
        from confluent_kafka import Consumer, KafkaError

        # consume earliest available messages, don't commit offsets
        consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'test-group',
            'default.topic.config': {
                'auto.offset.reset': 'earliest'
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

            # print('Received message: {}'.format(msg.value().decode('utf-8')))
            message(id, msg)

        consumer.close()


    def test_consumer(self):
        self.__create_consumer(1, lambda id, message: print("%d: %s:%d:%d: key=%s value=%s" % (
                                        id, message.topic, message.partition,
                                        message.offset, message.key,
                                        message.value)))
        print("End")

    def __create_consumer(self, id, message_processor):
        '''
        https://github.com/dpkp/kafka-python

        # pip install kafka-python, msgpack-python
        '''
        from kafka import KafkaConsumer
        import msgpack

        # To consume latest messages and auto-commit offsets
        consumer = KafkaConsumer('test-topic',
                                group_id='test-group',
                                bootstrap_servers=['localhost:9092'],
                                api_version=(1, 0, 1))   # 必须加上api-version，这是因为一个BUG： https://github.com/dpkp/kafka-python/issues/1308
        for message in consumer:
            # message value and key are raw bytes -- decode if necessary!
            # e.g., for unicode: `message.value.decode('utf-8')`
            message_processor(id, message)
