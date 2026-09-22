from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, regexp_replace, trim

BOT_SIGNAL_RE = r"(?i)(auto-signal|BUY BUY BUY|pumpgroup|botalert)"
URL_RE = r"https?://\S+|t\.me/\S+"
EMOJI_RE = r"[\x{1F000}-\x{1FAFF}\x{2600}-\x{27BF}]"
NON_ALNUM_RE = r"[^\p{L}\p{N}\s$]"
MULTI_WS_RE = r"\s+"


def apply_cleaning(df: DataFrame, text_col: str = "full_text") -> DataFrame:
    is_noise = col(text_col).rlike(BOT_SIGNAL_RE)

    stripped = regexp_replace(col(text_col), URL_RE, "")
    stripped = regexp_replace(stripped, EMOJI_RE, "")
    stripped = regexp_replace(stripped, NON_ALNUM_RE, "")
    stripped = regexp_replace(stripped, MULTI_WS_RE, " ")
    clean_text = lower(trim(stripped))

    return df.withColumn("is_noise", is_noise).withColumn("clean_text", clean_text)
