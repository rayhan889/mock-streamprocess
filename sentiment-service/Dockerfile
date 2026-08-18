FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY sentiment_pb2.py sentiment_pb2_grpc.py server.py ./

EXPOSE 50051

CMD ["python", "server.py"]