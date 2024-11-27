package consumers

import (
	"context"
	"encoding/json"
	"log"

	"github.com/KRVIMAL/notification-service/config"
	notifications "github.com/KRVIMAL/notification-service/notfications"
	"github.com/segmentio/kafka-go"
)

func StartKafkaConsumer(cfg *config.Config) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{cfg.Kafka.URL},
		Topic:   cfg.Kafka.Topic,
		GroupID: cfg.Kafka.Group,
	})
	defer reader.Close()

	for {
		// Read messages from Kafka
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message from Kafka: %v", err)
			continue
		}

		log.Printf("Message received: %s", msg.Value)

		// Parse message
		var data map[string]interface{}
		err = json.Unmarshal(msg.Value, &data)
		if err != nil {
			log.Printf("Error unmarshalling message: %v", err)
			continue
		}

		// Handle notifications
		go notifications.HandleNotifications(cfg, data)
		
		
	}
}
