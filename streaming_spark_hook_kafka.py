from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType

ss = SparkSession.builder.appName('stream-processing')\
                 .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-use your version")\
                 .getOrCreate()

kafka_data = ss.readStream.format('kafka') \
               .option('kafka.bootstrap.servers', 'brokers') \
               .option('subscribe', 'topic_name') \
               .load()

# for transform schema
schema = StructType()\
    .add("Date", StringType()) \
    .add("Time", StringType()) \
    .add("Type", StringType()) \
    .add("Amount", IntegerType()) \
    .add("Send", StringType())


kafka_df = kafka_data.withColumn("value", from_json(col("value").cast("string"), schema))


query = kafka_df.select("value.Date", "value.Time", "value.Type", "value.Amount", "value.Send")\
               .writeStream \
               .outputMode("append") \
               .format("console") \
               .start()

query.awaitTermination()

# spark-submit --packages org.apache.spark:spark-sql-kafka-use your version /dir