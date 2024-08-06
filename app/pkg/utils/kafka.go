package utils

import (
	"context"
	"log"

	"github.com/PhubetK/goWorkshop/app/config"
	"github.com/segmentio/kafka-go"
)

func KafkaConn(cfg config.KafkaConfig) *kafka.Conn {
	conn, err := kafka.DialLeader(context.Background(), "tcp", cfg.Url, cfg.Topic, 0)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}
	return conn
}

func IsTopicAlreadyExist(conn *kafka.Conn, topic string) bool {
	partitions, err := conn.ReadPartitions()
	if err != nil {
		panic(err.Error())
	}

	for _, p := range partitions {
		if p.Topic == topic {
			return true
		}
	}
	return false
}
