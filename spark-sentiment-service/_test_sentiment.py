import sys
sys.path.insert(0, "/work")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sentiment import score_sentiment

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

rows = [
    ("great news the stock is up and everyone is happy",),
    ("terrible crash awful loss everyone is furious",),
    ("kabar gembiranya rupiah kita menguat",),
    (None,),
]

df = spark.createDataFrame(rows, ["clean_text"])
out = df.withColumn("s", score_sentiment(col("clean_text"))) \
        .select("clean_text", "s.sentiment_label", "s.sentiment_score") \
        .collect()

for r in out:
    print(r["clean_text"], "->", r["sentiment_label"], r["sentiment_score"])
