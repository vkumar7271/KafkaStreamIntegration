from pyspark.sql.types import *

transaction_detail_schema = StructType() \
    .add("transaction_id", StringType()) \
    .add("transaction_card_type", StringType()) \
    .add("transaction_amount", StringType()) \
    .add("transaction_datetime", StringType())
