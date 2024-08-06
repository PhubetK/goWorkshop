package config

type KafkaConfig struct {
	Url   string `json:"url"`
	Topic string `json:"topic"`
}
