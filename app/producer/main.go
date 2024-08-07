package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/PhubetK/goWorkshop/app/config"
	"github.com/PhubetK/goWorkshop/app/models"
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var writer *kafka.Writer
var mongoClient *mongo.Client
var collection *mongo.Collection
var logger *zap.Logger

func loadConfig(file string) config.KafkaConfig {
	var cfg config.KafkaConfig
	configFile, err := os.Open(file)
	if err != nil {
		logger.Fatal("Error opening config file", zap.Error(err))
	}
	defer configFile.Close()

	jsonParser := json.NewDecoder(configFile)
	if err := jsonParser.Decode(&cfg); err != nil {
		logger.Fatal("Error decoding config file", zap.Error(err))
	}

	return cfg
}

func initLogger() {
	config := zap.NewProductionConfig()
	config.OutputPaths = []string{
		"producer.log",
		"stdout",
	}
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	var err error
	logger, err = config.Build()
	if err != nil {
		panic(err)
	}
}

func init() {
	initLogger()

	var err error
	cfg := loadConfig("../config/config.json")

	writer = kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{cfg.Url},
		Topic:    cfg.Topic,
		Balancer: &kafka.LeastBytes{},
	})
	uri := "mongo"

	mongoClient, err = mongo.NewClient(options.Client().ApplyURI(uri))
	if err != nil {
		logger.Fatal("Failed to create MongoDB client", zap.Error(err))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = mongoClient.Connect(ctx)
	if err != nil {
		logger.Fatal("Failed to connect to MongoDB", zap.Error(err))
	}

	collection = mongoClient.Database("testdb").Collection("testcollection")
}

func sendToKafka(operation string, data models.Product) error {
	productJSON, err := json.Marshal(data)
	if err != nil {
		logger.Error("Error marshalling product", zap.Error(err))
		return err
	}

	message := kafka.Message{
		Value: []byte(fmt.Sprintf(`{"operation":"%s","data":%s}`, operation, productJSON)),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = writer.WriteMessages(ctx, message)
	if err != nil {
		logger.Error("Error writing message to Kafka", zap.Error(err))
		return err
	}
	logger.Info("Message sent to Kafka", zap.ByteString("message", message.Value))
	return nil
}

func handleRequest(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Data models.Product `json:"data"`
	}

	var operation string
	switch r.Method {
	case http.MethodPost:
		operation = "CREATE"
	case http.MethodPut:
		operation = "UPDATE"
	case http.MethodDelete:
		operation = "DELETE"
	default:
		http.Error(w, "Unsupported HTTP method", http.StatusMethodNotAllowed)
		logger.Warn("Unsupported HTTP method", zap.String("method", r.Method))
		return
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		logger.Error("Error decoding request body", zap.Error(err))
		return
	}

	logger.Info("Decoded request data", zap.Any("data", req.Data))

	err := sendToKafka(operation, req.Data)
	if err != nil {
		http.Error(w, "Failed to send data to Kafka", http.StatusInternalServerError)
		logger.Error("Error sending data to Kafka", zap.Error(err))
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Data successfully sent to Kafka"))
	logger.Info("Request processed", zap.String("operation", operation), zap.Any("data", req.Data))
}

func handleRead(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var results []models.Product
	cur, err := collection.Find(ctx, bson.M{})
	if err != nil {
		http.Error(w, "Failed to retrieve data from MongoDB", http.StatusInternalServerError)
		logger.Error("Error finding documents in MongoDB", zap.Error(err))
		return
	}
	defer cur.Close(ctx)

	for cur.Next(ctx) {
		var result struct {
			Operation string         `json:"operation"`
			Data      models.Product `json:"data"`
		}
		if err := cur.Decode(&result); err != nil {
			http.Error(w, "Failed to decode data from MongoDB", http.StatusInternalServerError)
			logger.Error("Error decoding document from MongoDB", zap.Error(err))
			return
		}
		results = append(results, result.Data)
	}

	if err := cur.Err(); err != nil {
		http.Error(w, "Error iterating the cursor", http.StatusInternalServerError)
		logger.Error("Cursor iteration error", zap.Error(err))
		return
	}

	if len(results) == 0 {
		http.Error(w, "No records found", http.StatusNotFound)
		return
	}

	response, err := json.Marshal(results)
	if err != nil {
		http.Error(w, "Failed to marshal response", http.StatusInternalServerError)
		logger.Error("Error marshalling response", zap.Error(err))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(response)
	logger.Info("Request processed", zap.Any("results", results))
}

func main() {
	defer logger.Sync()

	http.HandleFunc("/create", handleRequest)
	http.HandleFunc("/update", handleRequest)
	http.HandleFunc("/delete", handleRequest)
	http.HandleFunc("/read", handleRead)
	logger.Info("Starting server on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
