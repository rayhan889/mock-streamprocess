import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct, to_json

from cleaning import apply_cleaning
from schema import RAW_TWEET_SCHEMA
from sentiment import score_sentiment

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092")
INPUT_TOPIC = os.environ.get("INPUT_TOPIC", "tweets.raw")
OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "tweets.sentiment")
TRIGGER_INTERVAL = os.environ.get("TRIGGER_INTERVAL", "10 seconds")
MAX_OFFSETS_PER_TRIGGER = os.environ.get("MAX_OFFSETS_PER_TRIGGER", "5000")
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/checkpoints/sentiment")
SPARK_MASTER_URL = os.environ.get("SPARK_MASTER_URL", "local[*]")

OUT_COLUMNS = [
    "conversation_id",
    "posted_at",
    "full_text",
    "clean_text",
    "is_noise",
    "sentiment_label",
    "sentiment_score",
]


def main():
    spark = (
        SparkSession.builder.appName("spark-sentiment-service")
        .master(SPARK_MASTER_URL)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .option("failOnDataLoss", "false")
        .load()
    )

    tweets = raw.select(
        from_json(col("value").cast("string"), RAW_TWEET_SCHEMA).alias("data")
    ).select("data.*")

    cleaned = apply_cleaning(tweets)

    scored = cleaned.withColumn("sentiment", score_sentiment(col("clean_text")))
    scored = scored.withColumn("sentiment_label", col("sentiment.sentiment_label"))
    scored = scored.withColumn("sentiment_score", col("sentiment.sentiment_score"))

    out = scored.select(
        col("conversation_id").alias("key"),
        to_json(struct(*[col(c) for c in OUT_COLUMNS])).alias("value"),
    )

    query = (
        out.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("topic", OUTPUT_TOPIC)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
