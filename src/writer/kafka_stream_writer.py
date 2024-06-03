
def write_to_kafka_stream(logger, transaction_detail_df,
                          kafka_bootstrap_server_name, kafka_output_topic_name,
                          output_processing_time, output_mode,
                          check_point_location):
    try:
        trans_detail_write_stream_1 = transaction_detail_df \
            .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_server_name) \
            .option("topic", kafka_output_topic_name) \
            .trigger(processingTime=output_processing_time) \
            .outputMode(output_mode) \
            .option("checkpointLocation", check_point_location) \
            .start()
        return trans_detail_write_stream_1
    except Exception as err:
        logger.error("Failed to write into kafka stream.")
        logger.error("Error Details:" + str(err))
        exit(1)
