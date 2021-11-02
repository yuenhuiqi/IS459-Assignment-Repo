from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

def parse_data_from_kafka_message(sdf, schema):
    assert sdf.isStreaming == True, "DataFrame doesn't receive streaming data"
    # changed separator to '",' instead, because there are commas that exists in topics & posts as well. 
    col = split(sdf['value'], '",')

    #split attributes to nested array in one Column
    #now expand col to multiple top-level columns
    for idx, field in enumerate(schema):
        sdf = sdf.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return sdf.select([field.name for field in schema])

if __name__ == "__main__":

    spark = SparkSession.builder \
               .appName("KafkaWordCount") \
               .getOrCreate()

    #Read from Kafka's topic scrapy-output
    df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "hwz-output") \
            .option("startingOffsets", "latest") \
            .load()

    #Parse the fields in the value column of the message
    lines = df.selectExpr("CAST(value AS STRING)")

    #Specify the schema of the fields
    hardwarezoneSchema = StructType([ \
        StructField("topic", StringType()), \
        StructField("author", StringType()), \
        StructField("content", StringType()) \
        ])

    #Use the function to parse the fields
    lines = parse_data_from_kafka_message(lines, hardwarezoneSchema)
    lines = lines.withColumn("timestamp", current_timestamp())

    # Get top 10 authors with most posts in 2 minute window
    # Top 10 authors with their number of posts will be output to the console every minute.

    authors = lines.select("timestamp", "author") \
                    .groupBy(window("timestamp", "2 minutes", "1 minutes"), lines.author) \
                    .count()

    authorsDF = authors.select("window", "author", "count") \
                        .where( unix_timestamp("window.start") - unix_timestamp(current_timestamp()) >= -60)  \
                        .orderBy(desc("count")).limit(10)

    # Select top authors to output into the console
    topAuthors = authorsDF.writeStream \
        .queryName("WriteTopAuthors") \
        .outputMode("complete") \
        .format("console") \
        .option("checkpointLocation", "hdfs://localhost:9000/user/huiqi/spark-checkpoint") \
        .option("truncate", False) \
        .trigger(processingTime='60 seconds') \
        .start()

    # #Start the job and wait for the incoming messages
    topAuthors.awaitTermination()
