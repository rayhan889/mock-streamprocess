# mock-streamprocess

A mock streaming pipeline that simulates ingesting tweets, cleaning them, and
sinking the results to MongoDB — built as a Kafka producer/consumer chain in Go.

## Architecture

```
data/bbca_tweets.csv
        │
        ▼
extractor-service ──▶ tweets.raw (Kafka) ──▶ processor-service ──▶ tweets.clean (Kafka) ──▶ sink-service ──▶ MongoDB
```

- **extractor-service** — reads `data/bbca_tweets.csv` row by row and publishes
  each row as a `RawTweet` JSON message to the `tweets.raw` topic, looping back
  to the start of the file once it hits EOF to simulate a continuous stream.
- **processor-service** — consumes `tweets.raw`, strips URLs/emoji/non-alphanumeric
  characters from the tweet text, flags likely bot/spam content (e.g. "BUY BUY BUY",
  "pumpgroup"), and publishes the result to `tweets.clean`.
- **sink-service** — consumes `tweets.clean` and upserts each record into the
  `sentiment_results` collection of a MongoDB database, keyed by `conversation_id`.

## Requirements

- Docker and Docker Compose

## Setup

1. Copy `.env.example` to `.env` and fill in your values:
   ```
   KAFKA_BROKER=kafka:29092
   MONGO_URI=<your MongoDB connection string>
   ```
2. Start the stack:
   ```
   docker compose up --build
   ```

This spins up Kafka, creates the `tweets.raw` and `tweets.clean` topics, then
starts all three services.

## Project layout

```
data/                  sample tweet CSV used as the mock data source
extractor-service/     CSV -> Kafka producer
processor-service/     Kafka -> Kafka text cleaner
sink-service/          Kafka -> MongoDB sink
compose.yml            local dev stack (Kafka + all services)
```
