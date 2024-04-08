import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types

pyspark_version = pyspark.__version__
kafka_jar_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark_version}"

local_stream = None

def get_data():
    print(f"Getting data...")

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("GreenTripsConsumer") \
        .config("spark.jars.packages", kafka_jar_package) \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    green_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "green-trips") \
        .option("startingOffsets", "earliest") \
        .load()

    schema = types.StructType() \
        .add("lpep_pickup_datetime", types.StringType()) \
        .add("lpep_dropoff_datetime", types.StringType()) \
        .add("PULocationID", types.IntegerType()) \
        .add("DOLocationID", types.IntegerType()) \
        .add("passenger_count", types.DoubleType()) \
        .add("trip_distance", types.DoubleType()) \
        .add("tip_amount", types.DoubleType())

    df_selected = green_stream \
        .select(F.from_json(F.col("value").cast('STRING'), schema).alias("data")) \
        .select("data.*")

    # Agregar la columna "timestamp" utilizando current_timestamp()
    df_with_timestamp = df_selected.withColumn("timestamp", F.current_timestamp())

    # Agrupar por "DOLocationID" y ventana de tiempo de 5 minutos
    grouped_df = df_with_timestamp \
        .groupBy("DOLocationID", F.window("timestamp", "5 minutes")) \
        .agg(F.count("DOLocationID").alias("count")) \
        .orderBy("count", ascending=False)

    local_stream = grouped_df \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    local_stream.awaitTermination()

    local_stream.stop()

if __name__ == "__main__":
    get_data()
