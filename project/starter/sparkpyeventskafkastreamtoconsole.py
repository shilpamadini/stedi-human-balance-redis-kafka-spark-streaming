from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, DoubleType

spark = (
    SparkSession.builder
    .appName("StediEventsKafkaStreamToConsole")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

stedi_events_schema = StructType([
    StructField("customer", StringType(), True),
    StructField("score",    DoubleType(), True),
    StructField("riskDate", StringType(), True)
])

# using the spark application object, read a streaming dataframe from the Kafka topic stedi-events as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream

events_raw_streaming_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "stedi-events")
    .option("startingOffsets", "earliest")
    .load()
)

                                   
# cast the value column in the streaming dataframe as a STRING 

events_value_df = events_raw_streaming_df.selectExpr(
    "CAST(value AS STRING) as value"
)


# parse the JSON from the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"custom"...|
# +------------+
#
# and create separated fields like this:
# +------------+-----+-----------+
# |    customer|score| riskDate  |
# +------------+-----+-----------+
# |"sam@tes"...| -1.4| 2020-09...|
# +------------+-----+-----------+
#
# storing them in a temporary view called CustomerRisk

events_parsed_df = events_value_df.select(
    from_json(col("value"), stedi_events_schema).alias("event")
).select("event.*")

events_parsed_df.createOrReplaceTempView("CustomerRisk")


# execute a sql statement against a temporary view, selecting the customer and the score from the temporary view, creating a dataframe called customerRiskStreamingDF

customerRiskStreamingDF = spark.sql("""
    SELECT
        customer,
        score
    FROM CustomerRisk
    WHERE customer IS NOT NULL
""")

#  sink the customerRiskStreamingDF dataframe to the console in append mode
# 
# It should output like this:
#
# +--------------------+-----
# |customer           |score|
# +--------------------+-----+
# |Spencer.Davis@tes...| 8.0|
# +--------------------+-----
# Run the python script by running the command from the terminal:
# 
# Verify the data looks correct 

query = (
    customerRiskStreamingDF.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()