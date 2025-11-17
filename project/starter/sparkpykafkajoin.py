from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr,struct
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, DoubleType

# create a StructType for the Kafka redis-server topic which has all changes made to Redis - before Spark 3.0.0, schema inference is not automatic

redis_server_schema = StructType([
    StructField("key",         StringType(), True),
    StructField("value",       StringType(), True),
    StructField("expiredType", StringType(), True),
    StructField("expiredValue", StringType(), True),
    StructField("existType",   StringType(), True),
    StructField("Ch",          BooleanType(), True),
    StructField("Incr",        BooleanType(), True),
    StructField(
        "zSetEntries",
        ArrayType(
            StructType([
                StructField("element", StringType(), True),
                StructField("score",   StringType(), True)
            ])
        ),
        True
    )
])




# create a StructType for the Customer JSON that comes from Redis- before Spark 3.0.0, schema inference is not automatic

customer_schema = StructType([
    StructField("customerName", StringType(), True),
    StructField("email",        StringType(), True),
    StructField("phone",        StringType(), True),
    StructField("birthDay",     StringType(), True)
])



# create a StructType for the Kafka stedi-events topic which has the Customer Risk JSON that comes from Redis- before Spark 3.0.0, schema inference is not automatic

stedi_events_schema = StructType([
    StructField("customer", StringType(), True),
    StructField("score",    DoubleType(), True),
    StructField("riskDate", StringType(), True)
])


#create a spark application object

spark = (
    SparkSession.builder
    .appName("KafkaJoinCustomersAndRisk")
    .getOrCreate()
)


#set the spark log level to WARN

spark.sparkContext.setLogLevel("WARN")

# using the spark application object, read a streaming dataframe from the Kafka topic redis-server as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream

redis_raw_streaming_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "redis-server")
    .option("startingOffsets", "earliest")
    .load()
)



#  cast the value column in the streaming dataframe as a STRING 

redis_value_df = redis_raw_streaming_df.selectExpr(
    "CAST(value AS STRING) as value"
)

# ; parse the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"key":"Q3..|
# +------------+
#
# with this JSON format: {"key":"Q3VzdG9tZXI=",
# "existType":"NONE",
# "Ch":false,
# "Incr":false,
# "zSetEntries":[{
# "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
# "Score":0.0
# }],
# "zsetEntries":[{
# "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
# "score":0.0
# }]
# }
# 
# (Note: The Redis Source for Kafka has redundant fields zSetEntries and zsetentries, only one should be parsed)
#
# and create separated fields like this:
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |         key|value|expiredType|expiredValue|existType|   ch| incr|      zSetEntries|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |U29ydGVkU2V0| null|       null|        null|     NONE|false|false|[[dGVzdDI=, 0.0]]|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
#
# storing them in a temporary view called RedisSortedSet

redis_parsed_df = redis_value_df.select(
    from_json(col("value"), redis_server_schema).alias("data")
).select(
    col("data.key").alias("key"),
    col("data.value").alias("value"),
    col("data.expiredType").alias("expiredType"),
    col("data.expiredValue").alias("expiredValue"),
    col("data.existType").alias("existType"),
    col("data.Ch").alias("ch"),
    col("data.Incr").alias("incr"),
    col("data.zSetEntries").alias("zSetEntries")
)

redis_parsed_df.createOrReplaceTempView("RedisSortedSet")

# execute a sql statement against a temporary view, which statement takes the element field from the 0th element in the array of structs and create a column called encodedCustomer
# the reason we do it this way is that the syntax available select against a view is different than a dataframe, and it makes it easy to select the nth element of an array in a sql column

encoded_customer_df = spark.sql("""
    SELECT
        zSetEntries[0].element AS encodedCustomer
    FROM RedisSortedSet
    WHERE zSetEntries IS NOT NULL
""")

# take the encodedCustomer column which is base64 encoded at first like this:
# +--------------------+
# |            customer|
# +--------------------+
# |[7B 22 73 74 61 7...|
# +--------------------+

# and convert it to clear json like this:
# +--------------------+
# |            customer|
# +--------------------+
# |{"customerName":"...|
#+--------------------+
#
# with this JSON format: {"customerName":"Sam Test","email":"sam.test@test.com","phone":"8015551212","birthDay":"2001-01-03"}

decoded_customer_df = encoded_customer_df.select(
    unbase64(col("encodedCustomer")).cast("string").alias("customer")
)


# parse the JSON in the Customer record and store in a temporary view called CustomerRecords

customer_parsed_df = decoded_customer_df.select(
    from_json(col("customer"), customer_schema).alias("customer")
).select("customer.*")

customer_parsed_df.createOrReplaceTempView("CustomerRecords")

# JSON parsing will set non-existent fields to null, so let's select just the fields we want, where they are not null as a new dataframe called emailAndBirthDayStreamingDF

emailAndBirthDayStreamingDF = spark.sql("""
    SELECT
        email,
        birthDay
    FROM CustomerRecords
    WHERE email IS NOT NULL
      AND birthDay IS NOT NULL
""")


# Split the birth year as a separate field from the birthday
# Select only the birth year and email fields as a new streaming data frame called emailAndBirthYearStreamingDF

emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.select(
    col("email"),
    split(col("birthDay"), "-").getItem(0).alias("birthYear")
)


#  using the spark application object, read a streaming dataframe from the Kafka topic stedi-events as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream

events_raw_streaming_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "stedi-events")
    .option("startingOffsets", "earliest")
    .load()
)

                                   
#  cast the value column in the streaming dataframe as a STRING 

events_value_df = events_raw_streaming_df.selectExpr(
    "CAST(value AS STRING) as value"
)

#  parse the JSON from the single column "value" with a json object in it, like this:
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
        score,
        riskDate
    FROM CustomerRisk
    WHERE customer IS NOT NULL
""")


#  join the streaming dataframes on the email address to get the risk score and the birth year in the same dataframe

joined_streaming_df = customerRiskStreamingDF.join(
    emailAndBirthYearStreamingDF,
    customerRiskStreamingDF.customer == emailAndBirthYearStreamingDF.email,
    "inner"
)

riskScoreByBirthYearStreamingDF = joined_streaming_df.select(
    col("customer"),
    col("score"),
    col("email"),
    col("birthYear")
)


# sink the joined dataframes to a new kafka topic to send the data to the STEDI graph application 
# +--------------------+-----+--------------------+---------+
# |            customer|score|               email|birthYear|
# +--------------------+-----+--------------------+---------+
# |Santosh.Phillips@...| -0.5|Santosh.Phillips@...|     1960|
# |Sean.Howard@test.com| -3.0|Sean.Howard@test.com|     1958|
# |Suresh.Clark@test...| -5.0|Suresh.Clark@test...|     1956|
# |  Lyn.Davis@test.com| -4.0|  Lyn.Davis@test.com|     1955|
# |Sarah.Lincoln@tes...| -2.0|Sarah.Lincoln@tes...|     1959|
# |Sarah.Clark@test.com| -4.0|Sarah.Clark@test.com|     1957|
# +--------------------+-----+--------------------+---------+
#
# In this JSON Format {"customer":"Santosh.Fibonnaci@test.com","score":"28.5","email":"Santosh.Fibonnaci@test.com","birthYear":"1963"} 

customer_risk_output_df = riskScoreByBirthYearStreamingDF.select(
    col("customer").alias("key"),
    to_json(
        struct(
            col("customer").alias("customer"),
            col("score").cast("string").alias("score"),
            col("email").alias("email"),
            col("birthYear").cast("string").alias("birthYear")
        )
    ).alias("value")
)

CustomerRiskSinkQuery = (
    customer_risk_output_df.writeStream
    .format("kafka")
    .outputMode("append")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "customer-risk")
    .option("checkpointLocation", "/tmp/spark-checkpoints/kafkajoin")
    .start()
)

CustomerRiskSinkQuery.awaitTermination()
