
def console_stream_writer(logger, transformed_df, output_processing_time, output_mode, console_check_point_location):
    logger.info("Write final result into console for debugging purpose")
    try:
        trans_detail_write_stream = transformed_df \
            .writeStream \
            .trigger(processingTime=output_processing_time) \
            .outputMode(output_mode) \
            .option("truncate", "false") \
            .option("checkpointLocation", console_check_point_location) \
            .format("console") \
            .start()
        return trans_detail_write_stream
    except Exception as err:
        logger.error("Failed to write stream output to console.")
        logger.error("Error Details:" + str(err))
        exit(1)
