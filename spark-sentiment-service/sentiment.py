import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

_analyzer = None


def _get_analyzer():
    global _analyzer
    if _analyzer is None:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

        _analyzer = SentimentIntensityAnalyzer()
    return _analyzer


def label_from_compound(compound: float) -> str:
    if compound >= 0.05:
        return "positive"
    if compound <= -0.05:
        return "negative"
    return "neutral"


@pandas_udf(StructType([
    StructField("sentiment_label", StringType()),
    StructField("sentiment_score", DoubleType()),
]))
def score_sentiment(texts: pd.Series) -> pd.DataFrame:
    a = _get_analyzer()
    compounds = texts.fillna("").map(lambda t: a.polarity_scores(t)["compound"])
    return pd.DataFrame({
        "sentiment_label": compounds.map(label_from_compound),
        "sentiment_score": compounds,
    })
