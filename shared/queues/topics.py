from kafka import KafkaProducer

class KafkaProducerSingleton:
    _instances = {}

    @classmethod
    def get_producer(cls, topic):
        if topic not in cls._instances:
            cls._instances[topic] = KafkaProducer(bootstrap_servers='localhost:9092')
        return cls._instances[topic]