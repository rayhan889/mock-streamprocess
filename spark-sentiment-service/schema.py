from pyspark.sql.types import StructType, StructField, StringType

RAW_TWEET_SCHEMA = StructType([
    StructField("conversation_id", StringType()),
    StructField("full_text", StringType()),
    StructField("author_handle", StringType()),
    StructField("author_name", StringType()),
    StructField("likes", StringType()),
    StructField("replies", StringType()),
    StructField("retweets", StringType()),
    StructField("tweet_url", StringType()),
    StructField("posted_at", StringType()),
    StructField("scraped_at", StringType()),
])