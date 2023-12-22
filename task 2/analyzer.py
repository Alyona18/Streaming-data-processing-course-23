from pyspark.sql import SparkSession
from pyspark.sql.functions import window, count, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


spark = SparkSession.builder.appName("TelegramAnalyzer").getOrCreate()

# Указание схемы
schema = StructType([
    StructField("time", TimestampType(), True),
    StructField("source", StringType(), True),
    StructField("text", StringType(), True),
    StructField("has_media", StringType(), True),
])

# Чтение данных в режиме Structured Streaming
messages = spark.readStream \
    .format("parquet") \
    .schema(schema) \
    .option("maxFilesPerTrigger", 1) \
    .option("path", "messages/") \
    .load()

messages = messages.select("time", "source", "text", "has_media")

# Обсуждение темы
#topic_keyword = "комета"
#topic_discussion = messages.filter(col("text").contains(topic_keyword))

# Расчет всплеска частоты сообщений
message_counts = messages.groupBy(window("time", "1 minute")).agg(count("text").alias("message_count"))

N = 10
anomaly_counts = message_counts.filter(col("message_count") > N)

# Вывод результатов в консоль
query = anomaly_counts.writeStream.outputMode("append").format("console").start()
query.awaitTermination()
