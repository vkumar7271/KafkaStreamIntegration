from pyspark.sql.functions import *


def write_to_file_sink(logger, transformed_df, output_processing_time,
                       output_mode, check_point_location, output_location):
    logger.info("Write final result into file in JSON format")
    try:
        trans_detail_file_sink_write_stream = transformed_df \
            .writeStream \
            .format("json") \
            .queryName("File Sink Writer") \
            .trigger(processingTime=output_processing_time) \
            .outputMode(output_mode) \
            .option("path", output_location) \
            .option("checkpointLocation", check_point_location) \
            .start()
        return trans_detail_file_sink_write_stream
    except Exception as err:
        logger.error("Failed to write stream output to file sink.")
        logger.error("Error Details:" + str(err))
        exit(1)
