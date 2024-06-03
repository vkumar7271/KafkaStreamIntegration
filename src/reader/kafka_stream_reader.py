
def read_from_kafka_stream(spark, logger, kafka_bootstrap_server_name, kafka_input_topic_name, kafka_starting_offsets):

    logger.info("Reading from Kafka topic " + kafka_input_topic_name)
    try:
        kafka_stream_df = spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_bootstrap_server_name) \
                .option("subscribe", kafka_input_topic_name) \
                .option("startingOffsets", kafka_starting_offsets) \
                .load()
        return kafka_stream_df
    except Exception as err:
        logger.error("Failed to read from Kafka topic " + kafka_input_topic_name)
        logger.error("Error Details:" + str(err))
        exit(1)
