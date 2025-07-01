import configparser
import os
from utils import kafkaProducer


class KafkaMonitorLogs:
    setuProducer = None

    @classmethod
    def push_monitor_logs(cls, message):
        topic_name = "mfi_monitor_logs"

        if cls.setuProducer is None:
            config = configparser.ConfigParser()
            config.read(os.path.expanduser('~/.setuserv/kafka.ini'))
            kafka_config = config['Kafka']
            kafka_server = kafka_config['KAFKA_SERVER']
            print("kafka_server-------------****************((((((", kafka_server)
            cls.setuProducer = kafkaProducer()

        cls.setuProducer.setu_producer(message, topic_name)
