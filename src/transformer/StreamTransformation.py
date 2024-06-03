from pyspark.sql.functions import *


def stream_transformation(logger, transaction_detail_df, transaction_detail_schema):
    logger.info("Stream transformation started.")
    try:
        transaction_detail_df2 = transaction_detail_df \
            .select(from_json(col("value").cast("string"), transaction_detail_schema)
                    .alias("transaction_detail"), "timestamp")

        transaction_detail_df3 = transaction_detail_df2.select("transaction_detail.*", "timestamp")

        # Simple aggregate - find total_transaction_amount by grouping transaction_card_type
        transaction_detail_df4 = transaction_detail_df3.groupBy("transaction_card_type") \
            .agg({'transaction_amount': 'sum'}) \
            .select("transaction_card_type", col("sum(transaction_amount)").alias("total_transaction_amount"))

        logger.info("Printing Schema of transaction_detail_df4: ")
        logger.info(transaction_detail_df4.printSchema())

        transaction_detail_df5 = transaction_detail_df4.withColumn("key", lit(100)) \
            .withColumn("value", concat(lit("{'transaction_card_type': '"),
                                        col("transaction_card_type"), lit("', 'total_transaction_amount: '"),
                                        col("total_transaction_amount").cast("string"), lit("'}")))

        logger.info("Printing Schema of transaction_detail_df5: ")
        logger.info(transaction_detail_df5.printSchema())
        return transaction_detail_df5
    except Exception as err:
        logger.error("Failed to do spark stream transformation.")
        logger.error("Error Details:" + str(err))
        exit(1)


def file_sink_transformation(logger, transaction_detail_df, transaction_detail_schema):
    logger.info("Stream file sink transformation started.")
    try:
        transaction_detail_df = transaction_detail_df \
            .select(from_json(col("value").cast("string"), transaction_detail_schema)
                    .alias("transaction_detail"))

        explode_df = transaction_detail_df.selectExpr("transaction_detail.transaction_id",
                                                      "transaction_detail.transaction_card_type",
                                                      "transaction_detail.transaction_amount",
                                                      "transaction_detail.transaction_datetime")
        return explode_df
    except Exception as err:
        logger.error("Failed to do spark stream file sink transformation.")
        logger.error("Error Details:" + str(err))
        exit(1)