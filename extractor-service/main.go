package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

type RawTweet struct {
	ConversationID string `json:"conversation_id"`
	FullText       string `json:"full_text"`
	AuthorHandle  string `json:"author_handle"`
	AuthorName    string `json:"author_name"`
	Likes 	   string `json:"likes"`
	Replies 	   string `json:"replies"`
	Retweets 	   string `json:"retweets"`
	TweetURL 	   string `json:"tweet_url"`
	PostedAt 	   string `json:"posted_at"`
	ScrapedAt 	   string `json:"scraped_at"`
}

func main() {
	broker := os.Getenv("KAFKA_BROKER")
	w := &kafka.Writer{
		Addr:     kafka.TCP(broker),
		Topic:    "tweets.raw",
		Balancer: &kafka.Hash{},
	}
	defer w.Close()

	f, err := os.Open("../data/bbca_tweets.csv")
	if err != nil {
		log.Fatal("open csv:", err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.Read()

	for {
		row, err := r.Read()
		if err == io.EOF {
			log.Println("reached end of csv, looping to simulate continuous stream")
			f.Seek(0, io.SeekStart)
			r = csv.NewReader(f)
			r.Read()
			continue
		}
		if err != nil {
			log.Println("read error:", err)
			continue
		}

		tweet := RawTweet{
			ConversationID: row[0],
			FullText:         row[1],
			AuthorHandle:    row[2],
			AuthorName:      row[3],
			Likes:            row[4],
			Replies:          row[5],
			Retweets:         row[6],
			TweetURL:         row[7],
			PostedAt:         row[8],
			ScrapedAt:        row[9],
		}
		payload, _ := json.Marshal(tweet)

		if err := w.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(tweet.ConversationID),
			Value: payload,
		}); err != nil {
			log.Println("write error:", err)
		} else {
			log.Printf("extracted -> %s", tweet.ConversationID)
		}

		time.Sleep(1 * time.Second)
	}
}