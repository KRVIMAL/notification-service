package main

import (
	"github.com/KRVIMAL/notification-service-310/notfications/trip"
)

func main() {
	// Kafka configuration
	brokerURL := "192.168.1.8:9092"
	inputTopic := "socket_310_jsonData"

	// MongoDB configuration
	mongoURI := "mongodb://localhost:27017"
	dbName := "kafka_db"
	collectionName := "alert_config"

	// Start the trip consumer
	trip.StartTripConsumer(brokerURL, inputTopic, mongoURI, dbName, collectionName)
}
