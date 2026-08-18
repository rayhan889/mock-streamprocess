package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/rayhan889/mock-streamprocess/sentimentpb"
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

type CleanedTweet struct {
	ConversationID string `json:"conversation_id"`
	PostedAt 	   string `json:"posted_at"`
	FullText       string `json:"full_text"`
	CleanText string `json:"clean_text"`
	IsNoise   bool   `json:"is_noise"`
	SentimentLabel string  `json:"sentiment_label"`
	SentimentScore float64 `json:"sentiment_score"`
}

var (
	urlRe     = regexp.MustCompile(`https?://\S+|t\.me/\S+`)
	emojiRe   = regexp.MustCompile(`[\x{1F000}-\x{1FAFF}\x{2600}-\x{27BF}]`)
	nonAlnum  = regexp.MustCompile(`[^\p{L}\p{N}\s$]`)
	multiWS   = regexp.MustCompile(`\s+`)
	botSignal = regexp.MustCompile(`(?i)(auto-signal|BUY BUY BUY|pumpgroup|botalert)`)
)

func clean(text string) (string, bool) {
	isNoise := botSignal.MatchString(text)
	t := urlRe.ReplaceAllString(text, "")
	t = emojiRe.ReplaceAllString(t, "")
	t = nonAlnum.ReplaceAllString(t, "")
	t = multiWS.ReplaceAllString(t, " ")
	return strings.TrimSpace(strings.ToLower(t)), isNoise
}

func main() {
	broker := os.Getenv("KAFKA_BROKER")

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:               []string{broker},
		Topic:                 "tweets.raw",
		GroupID:               "processor-service",
		WatchPartitionChanges: true,
	})
	defer reader.Close()

	writer := &kafka.Writer{
		Addr:     kafka.TCP(broker),
		Topic:    "tweets.clean",
		Balancer: &kafka.Hash{},
	}
	defer writer.Close()

	sentimentServiceHost := os.Getenv("SENTIMENT_SERVICE_HOST")
	sentimentServicePort := os.Getenv("SENTIMENT_SERVICE_PORT")
	if sentimentServiceHost == "" || sentimentServicePort == "" {
		log.Fatal("SENTIMENT_SERVICE_HOST and SENTIMENT_SERVICE_PORT environment variables are not set")
	}

	conn, err := grpc.NewClient(fmt.Sprintf("%s:%s", sentimentServiceHost, sentimentServicePort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to sentiment service: %v", err)
	}
	defer conn.Close()

	sentimentClient := sentimentpb.NewSentimentServiceClient(conn)

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Println("read error:", err)
			continue
		}

		var raw RawTweet
		if err := json.Unmarshal(m.Value, &raw); err != nil {
			log.Println("unmarshal error:", err)
			continue
		}

		cleanText, isNoise := clean(raw.FullText)

		var sentimentLabel string
		var sentimentScore float64

		sentimentCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		sentimentResp, err := sentimentClient.AnalyzeSentiment(sentimentCtx, &sentimentpb.AnalyzeSentimentRequest{
			Text: cleanText,
		})
		cancel()

		log.Println("Execute sentiment service through gRPC")
		log.Printf("analyzed sentiment for %s: label=%s, score=%f", raw.ConversationID, sentimentResp.Label, sentimentResp.Score)

		if err != nil {
			log.Println("sentiment analysis error:", err)
		} else {
			sentimentLabel = sentimentResp.Label
			sentimentScore = sentimentResp.Score
		}

		out := CleanedTweet{
			ConversationID: raw.ConversationID,
			CleanText: cleanText,
			PostedAt: raw.PostedAt,
			IsNoise:   isNoise,
			FullText:      raw.FullText,
			SentimentLabel: sentimentLabel,
			SentimentScore: sentimentScore,
		}
		payload, _ := json.Marshal(out)

		if err := writer.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(out.ConversationID),
			Value: payload,
		}); err != nil {
			log.Println("write error:", err)
		} else {
			log.Printf("cleaned -> %s (noise=%v)", out.ConversationID, out.IsNoise)
		}
	}
}
