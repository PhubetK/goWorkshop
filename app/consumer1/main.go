package main

import (
	"context"
	"encoding/json"
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
		"consumer.log",
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
	mongoClient, err = mongo.NewClient(options.Client().ApplyURI("mongodb+srv://bosbesp:Pbkrbs170245@cluster0.nr5ruuc.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"))
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

func main() {
	defer logger.Sync()

	cfg := loadConfig("../config/config.json")
	groupID := "cons1"
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{cfg.Url},
		Topic:   cfg.Topic,
		GroupID: groupID,
	})

	defer reader.Close()

	for {
		message, err := reader.ReadMessage(context.Background())
		if err != nil {
			logger.Error("Error reading message from Kafka", zap.Error(err))
			continue
		}

		logger.Info("Received message",
			zap.String("GroupID", groupID),
			zap.String("Topic", message.Topic),
			zap.Int("Partition", message.Partition),
			zap.Int64("Offset", message.Offset),
			zap.String("Value", string(message.Value)),
		)

		var msg struct {
			Operation string         `json:"operation"`
			Data      models.Product `json:"data"`
		}
		if err := json.Unmarshal(message.Value, &msg); err != nil {
			logger.Error("Error unmarshalling message", zap.Error(err))
			continue
		}

		logger.Info("Parsed message",
			zap.String("Operation", msg.Operation),
			zap.String("Product Name", msg.Data.Name),
			zap.String("Expired", msg.Data.Expired),
			zap.String("Brand", msg.Data.Brand),
		)

		switch msg.Operation {
		case "CREATE":
			_, err = collection.InsertOne(context.Background(), bson.M{
				"operation": msg.Operation,
				"data":      msg.Data,
			})
			if err != nil {
				logger.Error("Error inserting document into MongoDB", zap.Error(err))
				continue
			}
		case "UPDATE":
			filter := bson.M{"data.name": msg.Data.Name}

			var existingDoc struct {
				Data models.Product `bson:"data"`
			}
			err = collection.FindOne(context.Background(), filter).Decode(&existingDoc)
			if err != nil {
				if err == mongo.ErrNoDocuments {
					logger.Warn("Document not found for update, treating as CREATE",
						zap.String("Name", msg.Data.Name),
					)
					_, err = collection.InsertOne(context.Background(), bson.M{
						"operation": msg.Operation,
						"data":      msg.Data,
					})
					if err != nil {
						logger.Error("Error inserting document into MongoDB", zap.Error(err))
						continue
					}
					logger.Info("Created new document", zap.Any("Data", msg.Data))
				} else {
					logger.Error("Error finding existing document in MongoDB", zap.Error(err))
					continue
				}
			} else {
				logger.Info("Old Data", zap.Any("Data", existingDoc.Data))

				update := bson.M{
					"$set": bson.M{
						"data":      msg.Data,
						"operation": "UPDATE",
					},
				}
				_, err = collection.UpdateOne(context.Background(), filter, update)
				if err != nil {
					logger.Error("Error updating document in MongoDB", zap.Error(err))
					continue
				}

				logger.Info("New Data", zap.Any("Data", msg.Data))
			}

		case "DELETE":
			filter := bson.M{"data.name": msg.Data.Name}
			_, err = collection.DeleteOne(context.Background(), filter)
			if err != nil {
				logger.Error("Error deleting document from MongoDB", zap.Error(err))
				continue
			}
		default:
			logger.Warn("Unknown operation", zap.String("Operation", msg.Operation))
			continue
		}

		if err := reader.CommitMessages(context.Background(), message); err != nil {
			logger.Error("Error committing message", zap.Error(err))
			continue
		}
	}
}
