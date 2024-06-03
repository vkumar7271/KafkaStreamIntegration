from kafka import KafkaProducer
from utils.SparkSessionHelperUtil import get_spark_session
from constants.constants import *
from lib.logger import Log4j
from datetime import datetime
import time
from json import dumps
import random
import configparser as cp
import sys

if __name__ == "__main__":

    environment = sys.argv[1]
    spark = get_spark_session("Kafka Producer Demo")

    logger = Log4j(spark)
    logger.info("Kafka producer Application Started ...")

    logger.info("Reading parameters from property file")
    props = cp.RawConfigParser()
    props.read("application.properties")

    kafka_input_topic_name = props.get(environment, KAFKA_INPUT_TOPIC_NAME_CONS)
    kafka_bootstrap_server_name = props.get(environment, KAFKA_BOOTSTRAP_SERVERS_CONS)
    try:
        kafka_producer_obj = KafkaProducer(bootstrap_servers=kafka_bootstrap_server_name,
                                            value_serializer=lambda x: dumps(x).encode('utf-8'))

        transaction_card_type_list = ["Visa", "MasterCard", "Maestro"]

        message = None
        for i in range(500):
            i = i + 1
            message = {}
            logger.info("Sending message to Kafka topic: " + str(i))
            event_datetime = datetime.now()

            message["transaction_id"] = str(i)
            message["transaction_card_type"] = random.choice(transaction_card_type_list)
            message["transaction_amount"] = round(random.uniform(5.5, 555.5), 2)
            message["transaction_datetime"] = event_datetime.strftime("%Y-%m-%d %H:%M:%S")

            logger.info("Message to be sent: " + str(message))
            kafka_producer_obj.send(kafka_input_topic_name, message)
            time.sleep(1)
    except Exception as err:
        logger.error("Failed to write message into  Kafka topic.")
        logger.error("Error Details:" + str(err))
        exit(1)