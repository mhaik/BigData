# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 2_app.py localhost:9092
import sys

from pyspark.ml.feature import VectorAssembler, StandardScalerModel
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)


if __name__ == "__main__":
    server = sys.argv[1] if len(sys.argv) == 2 else "localhost:9092"

    spark = SparkSession.builder.appName("streamKafka").getOrCreate()
    df = spark.read.csv("high_diamond_ranked_10min.csv", header=True, inferSchema=True)
    json_schema = df.schema
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", server)
        .option("subscribe", "lolRankedIn")
        .load()
    )
    # json message schema


    parsed = raw.select(
        "timestamp", f.from_json(raw.value.cast("string"), json_schema).alias("json")
    )

    info_df_fin = parsed.select("json.*", "timestamp")
    # parsed = parsed\
    # .withColumn("value", f.to_json(f.struct("*")).cast("string"),)
    # query = (
    #     parsed.writeStream.outputMode("append")
    #     .format("console")
    #     .option("truncate", "false")
    #     .start()
    #     .awaitTermination()
    # )
    #
    df = info_df_fin.drop("gameId","blueDeaths","redDeaths","blueEliteMonsters","redEliteMonsters", "redCSPerMin", "blueCSPerMin")
    df = df.drop("timestamp")
    df = df.drop("redFirstBlood","blueGoldPerMin","redGoldPerMin","blueTotalExperience","redTotalExperience","redExperienceDiff","redGoldDiff")
    df = df.drop("blueExperienceDiff","blueTotalGold","redTotalGold","blueAssists","redAssists","blueAvgLevel","redAvgLevel")
    features = df.drop('blueWins')

    assembler = VectorAssembler(inputCols=features.columns, outputCol='features')
    data_assembled = assembler.transform(df)


    scaler = StandardScalerModel.load("scale_test")
    data_scaled = scaler.transform(data_assembled)

    model = LogisticRegressionModel.load("test")

    predictions = model.transform(data_scaled)

    print(predictions)
    kafka_out = predictions.withColumn("value", f.to_json(f.struct("prediction","probability")).cast("string"),)
    print(kafka_out)
    query = (
        kafka_out
        .select("value")
        .writeStream
        # .outputMode("complete")
        .format("kafka")
        .option("kafka.bootstrap.servers", server)
        .option("topic", "lolRankedOut")
        .option("checkpointLocation", "chckpt")
        .start()
    )

    spark.streams.awaitAnyTermination()
