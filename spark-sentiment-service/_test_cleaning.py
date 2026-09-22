import sys
sys.path.insert(0, "/work")

from pyspark.sql import SparkSession
from cleaning import apply_cleaning

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

text = (
    "KABAR GEMBIRANYA\nRUPIAH KITA MENGUAT LOH🙂‍↕️🙂‍↕️\n\n"
    "masak share hanya saat rupiah melemah, nyalahin pemerintah lah, gini lah.\n\n"
    "pkoknya skrg rupiah menguat\n\n"
    "BTW INI FREE AKSES YAH\nhttps://t.co/mjYNlIJOg8\n"
    "$ihsg $bbca $bumi\n"
    "#rupiah #indonesia #dollar https://t.co/qf9WVZS9Ys"
)

df = spark.createDataFrame([(text,)], ["full_text"])
out = apply_cleaning(df).select("is_noise", "clean_text").collect()[0]
print("IS_NOISE:", out["is_noise"])
print("CLEAN_TEXT:", repr(out["clean_text"]))

noise_text = "this is auto-signal BUY BUY BUY pumpgroup botalert"
df2 = spark.createDataFrame([(noise_text,)], ["full_text"])
out2 = apply_cleaning(df2).select("is_noise", "clean_text").collect()[0]
print("NOISE IS_NOISE:", out2["is_noise"])
