from unittest import TestCase

class TestKafkaProducer(TestCase):
    def test_simple_producer(self):
        from kafka import KafkaProducer
        producer = KafkaProducer(bootstrap_servers='localhost:9092')

        for n in range(100):
            future = producer.send('test-topic', key=b'%d' % n , value=b'message_number_%d' % n)
            result = future.get(timeout=1)   # wait 1 second

        producer.flush()  # Wait untill all message sent
    

    def test_confluent_producer(self):
        from confluent_kafka import Producer

        producer = Producer({'bootstrap.servers': 'localhost:9092'})

        for n in range(100):
            producer.produce('test-topic', ('message_number_%d' % n).encode('utf-8'))

        producer.flush()