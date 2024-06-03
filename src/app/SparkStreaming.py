from lib.logger import Log4j
from utils.SparkSessionHelperUtil import get_spark_session
from utils.TransactionDetailsSchema import transaction_detail_schema
from src.writer.console_stream_writer import console_stream_writer
from src.writer.file_sink_writer import write_to_file_sink
from constants.constants import *
from src.reader.kafka_stream_reader import read_from_kafka_stream
from src.writer.kafka_stream_writer import write_to_kafka_stream
from src.transformer.StreamTransformation import stream_transformation, file_sink_transformation
import configparser as cp
import sys


if __name__ == "__main__":
    environment = sys.argv[1]
    spark = get_spark_session("PySpark Structured Streaming with Kafka Demo")

    logger = Log4j(spark)
    logger.info("PySpark Structured Streaming with Kafka Demo Application Started ...")

    logger.info("Reading parameters from property file")
    props = cp.RawConfigParser()
    props.read("application.properties")

    kafka_input_topic_name = props.get(environment, KAFKA_INPUT_TOPIC_NAME_CONS)
    kafka_output_topic_name = props.get(environment, KAFKA_OUTPUT_TOPIC_NAME_CONS)
    kafka_bootstrap_server_name = props.get(environment, KAFKA_BOOTSTRAP_SERVERS_CONS)
    kafka_starting_offsets = props.get(environment, KAFKA_STARTING_OFFSETS)
    output_processing_time = props.get(environment, OUTPUT_PROCESSING_TIME)
    file_output_mode = props.get(environment, FILE_OUTPUT_MODE)
    console_output_mode = props.get(environment, CONSOLE_OUTPUT_MODE)
    kafka_output_mode = props.get(environment, KAFKA_OUTPUT_MODE)
    console_check_point_location = props.get(environment, CONSOLE_CHECK_POINT_LOCATION)
    kafka_check_point_location = props.get(environment, KAFKA_CHECK_POINT_LOCATION)
    file_check_point_location = props.get(environment, FILE_CHECK_POINT_LOCATION)
    file_output_location = props.get(environment, FILE_OUTPUT_PATH)

    logger.info("Reading parameters from property file completed successfully")

    # Construct a streaming DataFrame that reads from testtopic

    logger.info("Reading data from Kafka topic and creating DataFrame.")
    transaction_detail_df = read_from_kafka_stream(spark,
                                                   logger,
                                                   kafka_bootstrap_server_name,
                                                   kafka_input_topic_name,
                                                   kafka_starting_offsets)
    logger.info("Reading stream as a dataframe completed successfully.")

    logger.info("Defining schema for stream transformation.")
    transaction_detail_schema = transaction_detail_schema
    logger.info("Transforming stream dataframe started.")

    transformed_df = stream_transformation(logger,
                                           transaction_detail_df,
                                           transaction_detail_schema)

    logger.info("Transforming stream dataframe completed successfully.")

    logger.info("Write final result into console for debugging purpose")
    # Write final result into console for debugging purpose

    trans_detail_write_stream = console_stream_writer(logger,
                                                      transformed_df,
                                                      output_processing_time,
                                                      console_output_mode,
                                                      console_check_point_location)
    logger.info("Writing DataFrame to a specific Kafka topic specified in an option.")
    trans_detail_write_stream_1 = write_to_kafka_stream(logger,
                                                        transaction_detail_df,
                                                        kafka_bootstrap_server_name,
                                                        kafka_output_topic_name,
                                                        output_processing_time,
                                                        kafka_output_mode,
                                                        kafka_check_point_location)

    logger.info("Writing Dataframe stream to File sink.")

    transformed_df = file_sink_transformation(logger,
                                              transaction_detail_df,
                                              transaction_detail_schema)
    trans_detail_file_stream = write_to_file_sink(logger,
                                                  transformed_df,
                                                  output_processing_time,
                                                  file_output_mode,
                                                  file_check_point_location,
                                                  file_output_location)
    trans_detail_write_stream_1.awaitTermination()

    logger.info("PySpark Structured Streaming with Kafka Demo Application Completed.")
