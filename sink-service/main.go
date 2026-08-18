package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type CleanedTweet struct {
	ConversationID string `json:"conversation_id"`
	PostedAt 	   string `json:"posted_at"`
	FullText       string `json:"full_text"`
	CleanText string `json:"clean_text"`
	IsNoise   bool   `json:"is_noise"`
	SentimentLabel string  `json:"sentiment_label"`
	SentimentScore float64 `json:"sentiment_score"`
}

func main() {
	broker := os.Getenv("KAFKA_BROKER")
	mongoURI := os.Getenv("MONGO_URI")

	client, err := mongo.Connect(options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatal("mongo connect:", err)
	}
	coll := client.Database("mock-streamprocessdb").Collection("sentiment_results")

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:               []string{broker},
		Topic:                 "tweets.clean",
		GroupID:               "sink-service",
		WatchPartitionChanges: true,
	})
	defer reader.Close()

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Println("read error:", err)
			continue
		}

		var t CleanedTweet
		if err := json.Unmarshal(m.Value, &t); err != nil {
			log.Println("unmarshal error:", err)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = coll.UpdateOne(ctx,
			bson.M{"conversation_id": t.ConversationID},
			bson.M{"$set": t},
			options.UpdateOne().SetUpsert(true),
		)
		cancel()

		if err != nil {
			log.Println("upsert error:", err)
		} else {
			log.Printf("sunk -> %s", t.ConversationID)
		}
	}
}
