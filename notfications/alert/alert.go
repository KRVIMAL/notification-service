package alert

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/segmentio/kafka-go"
)

var mongoClient *mongo.Client
var kafkaWriter *kafka.Writer

func init() {
	// Initialize MongoDB client
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	mongoClient = client

	// Initialize Kafka writer
	kafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "alerts_topic",
		Balancer: &kafka.LeastBytes{},
	}
}

type AlarmConfig struct {
	Event                  string   `json:"event"`
	Location               Location `json:"location"`
	IsAlreadyGenerateAlert bool     `json:"isAlreadyGenerateAlert"`
	StartDate              string   `json:"startDate"`
	EndDate                string   `json:"endDate"`
	StartAlertTime         string   `json:"startAlertTime"`
	EndAlertTime           string   `json:"endAlertTime"`
	LastAlertGeneratedTime int64    `json:"lastAlertGeneratedTime"`
}

type Location struct {
	Name        string      `json:"name"`
	GeoCodeData GeoCodeData `json:"geoCodeData"`
}

type GeoCodeData struct {
	Type     string   `json:"type"`
	Geometry Geometry `json:"geometry"`
}

type Geometry struct {
	Type        string    `json:"type"`
	Coordinates []float64 `json:"coordinates"`
	Radius      float64   `json:"radius"`
}

type DeviceData struct {
	IMEI        string        `json:"imei"`
	Latitude    float64       `json:"latitude"`
	Longitude   float64       `json:"longitude"`
	AlarmConfig []AlarmConfig `json:"alarmConfig"`
}

func HandleAlarmData(parsedData []byte) error {
	var data map[string]interface{}
	err := json.Unmarshal(parsedData, &data)
	if err != nil {
		return fmt.Errorf("failed to unmarshal parsed data: %w", err)
	}

	imei, ok := data["imei"].(string)
	if !ok {
		return fmt.Errorf("IMEI not found in parsed data")
	}

	// Fetch the alarm configuration from MongoDB
	collection := mongoClient.Database("alert_db").Collection("alert_config")
	filter := bson.M{"imei": imei}
	var deviceData DeviceData
	err = collection.FindOne(context.Background(), filter).Decode(&deviceData)
	if err == mongo.ErrNoDocuments {
		log.Printf("No alarm configuration found for IMEI: %s", imei)
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to fetch alarm configuration from MongoDB: %w", err)
	}

	lat, _ := data["latitude"].(float64)
	long, _ := data["longitude"].(float64)

	// Check alarms
	for i, alarmConfig := range deviceData.AlarmConfig {
		if !alarmConfig.IsAlreadyGenerateAlert && checkIfAlarm(lat, long, &deviceData.AlarmConfig[i], data) {
			log.Printf("Alarm triggered for event %s at location %s", alarmConfig.Event, alarmConfig.Location.Name)

			// Update MongoDB to set the alarm as already generated
			deviceData.AlarmConfig[i].IsAlreadyGenerateAlert = true
			deviceData.AlarmConfig[i].LastAlertGeneratedTime = time.Now().Unix()
			err := updateAlarmConfigInMongoDB(collection, filter, deviceData.AlarmConfig)
			if err != nil {
				log.Printf("Failed to update MongoDB: %v", err)
			}

			// Publish alert to Kafka
			alertMessage := fmt.Sprintf("Alarm triggered for event %s at location %s", alarmConfig.Event, alarmConfig.Location.Name)
			publishAlertToKafka(data, alertMessage)
		}
	}

	return nil
}

func checkIfAlarm(lat, long float64, alarmConfig *AlarmConfig, parsedData map[string]interface{}) bool {
	geoData := alarmConfig.Location.GeoCodeData
	coordinates := geoData.Geometry.Coordinates
	radius := geoData.Geometry.Radius

	// Skip alarms generated recently
	if alarmConfig.IsAlreadyGenerateAlert && time.Since(time.Unix(alarmConfig.LastAlertGeneratedTime, 0)) < time.Minute {
		return false
	}

	// Calculate distance
	dist := distance(lat, long, coordinates[0], coordinates[1])

	// Trigger alarm based on event type
	switch alarmConfig.Event {
	case "geo_in":
		if dist <= radius {
			return true
		}
	case "geo_out":
		if dist > radius {
			return true
		}
	}
	return false
}

func distance(lat1, lon1, lat2, lon2 float64) float64 {
	const R = 6371 // Earth's radius in kilometers
	latRad1 := lat1 * math.Pi / 180
	lonRad1 := lon1 * math.Pi / 180
	latRad2 := lat2 * math.Pi / 180
	lonRad2 := lon2 * math.Pi / 180

	dlat := latRad2 - latRad1
	dlon := lonRad2 - lonRad1

	a := math.Sin(dlat/2)*math.Sin(dlat/2) + math.Cos(latRad1)*math.Cos(latRad2)*math.Sin(dlon/2)*math.Sin(dlon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return R * c * 1000 // Distance in meters
}

func updateAlarmConfigInMongoDB(collection *mongo.Collection, filter bson.M, alarmConfig []AlarmConfig) error {
	update := bson.M{"$set": bson.M{"alarmConfig": alarmConfig}}
	_, err := collection.UpdateOne(context.Background(), filter, update)
	return err
}

func publishAlertToKafka(data map[string]interface{}, alertMessage string) {
	data["alert"] = alertMessage
	alertJSON, err := json.Marshal(data)
	if err != nil {
		log.Printf("Failed to marshal alert message: %v", err)
		return
	}

	err = kafkaWriter.WriteMessages(context.Background(), kafka.Message{
		Value: alertJSON,
	})
	if err != nil {
		log.Printf("Failed to publish alert to Kafka: %v", err)
	}
}
