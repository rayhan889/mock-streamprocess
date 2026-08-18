import logging
import os
from concurrent import futures

import grpc
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

import sentiment_pb2
import sentiment_pb2_grpc

analyzer = SentimentIntensityAnalyzer()


def label_from_compound(compound: float) -> str:
    if compound >= 0.05:
        return "positive"
    if compound <= -0.05:
        return "negative"
    return "neutral"


class SentimentServiceServicer(sentiment_pb2_grpc.SentimentServiceServicer):
    def AnalyzeSentiment(self, request, context):
        scores = analyzer.polarity_scores(request.text)
        compound = scores["compound"]
        return sentiment_pb2.AnalyzeSentimentResponse(
            label=label_from_compound(compound),
            score=compound,
        )


def serve():
    port = os.environ.get("SENTIMENT_SERVICE_PORT", "50051")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    sentiment_pb2_grpc.add_SentimentServiceServicer_to_server(
        SentimentServiceServicer(), server
    )
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logging.info("sentiment-service listening on port %s", port)
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    serve()