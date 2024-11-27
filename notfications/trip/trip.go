package trip

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/KRVIMAL/notification-service/models"
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// EventInfo struct to represent event descriptions
type EventInfo struct {
	Description string
	AlertType   string
	Source      string
	Value       string
}

func StartTripConsumer(brokerURL, inputTopic, mongoURI, dbName, collectionName string) {
	// Initialize MongoDB client
	mongoClient, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer mongoClient.Disconnect(context.TODO())

	collection := mongoClient.Database(dbName).Collection(collectionName)

	// Initialize Kafka reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerURL},
		Topic:   inputTopic,
		GroupID: "trip_consumer_group", // Use a unique GroupID
	})
	defer reader.Close()

	log.Printf("Listening for messages on Kafka topic: %s", inputTopic)

	for {
		// Read message from Kafka using FetchMessage to manually commit
		msg, err := reader.FetchMessage(context.Background())
		if err != nil {
			log.Printf("Failed to fetch message: %v", err)
			continue
		}

		log.Printf("Received message: %s", msg.Value)

		// Parse JSON data
		var data map[string]interface{}
		err = json.Unmarshal(msg.Value, &data)
		if err != nil {
			log.Printf("Failed to unmarshal JSON: %v", err)
			// Commit the message even if it failed to unmarshal, to avoid reprocessing
			if err := reader.CommitMessages(context.Background(), msg); err != nil {
				log.Printf("Failed to commit message: %v", err)
			}
			continue
		}

		// Extract IMEI from JSON
		imei, ok := data["imei"].(string)
		if !ok {
			log.Printf("IMEI not found in the received message")
			// Commit the message even if IMEI not found
			if err := reader.CommitMessages(context.Background(), msg); err != nil {
				log.Printf("Failed to commit message: %v", err)
			}
			continue
		}

		log.Printf("Checking IMEI: %s in MongoDB", imei)

		// Check if IMEI exists in MongoDB
		var alertConfig models.AlertConfig
		err = collection.FindOne(context.TODO(), bson.M{"imei": imei}).Decode(&alertConfig)
		if err == mongo.ErrNoDocuments {
			log.Printf("IMEI %s not found in MongoDB", imei)
			// Commit the message even if IMEI not found
			if err := reader.CommitMessages(context.Background(), msg); err != nil {
				log.Printf("Failed to commit message: %v", err)
			}
			continue
		} else if err != nil {
			log.Printf("Failed to query MongoDB: %v", err)
			// Commit the message to avoid blocking the consumer
			if err := reader.CommitMessages(context.Background(), msg); err != nil {
				log.Printf("Failed to commit message: %v", err)
			}
			continue
		}

		// Log the alert configuration for debugging
		log.Printf("AlertConfig found for IMEI %s: %+v", imei, alertConfig)

		// Process the message and generate alerts
		err = HandleTripAlert(alertConfig, data)
		if err != nil {
			log.Printf("Error handling trip alert: %v", err)
			// Commit the message even if there was an error handling the alert
			if err := reader.CommitMessages(context.Background(), msg); err != nil {
				log.Printf("Failed to commit message: %v", err)
			}
			continue
		}

		// Commit the message after successful processing
		if err := reader.CommitMessages(context.Background(), msg); err != nil {
			log.Printf("Failed to commit message: %v", err)
		} else {
			log.Printf("Message committed: partition=%d, offset=%d", msg.Partition, msg.Offset)
		}
	}
}

// HandleTripAlert processes the alerts based on alertConfig and parsedData
func HandleTripAlert(alertConfig models.AlertConfig, parsedData map[string]interface{}) error {
	// Generate and publish alerts
	fmt.Println("HandleTripAlertHandleTripAlertHandleTripAlert")
	go generateAndPublishAlerts(parsedData, alertConfig)
	return nil
}

func generateAndPublishAlerts(parsedData map[string]interface{}, alertConfig models.AlertConfig) {
	// Extract relevant fields from parsedData
	parsedSpeed, _ := getIntFromParsedData(parsedData, "speed")
	parsedDateTimeStr, _ := getStringFromParsedData(parsedData, "dateTime")
	parsedLat, _ := getFloat64FromParsedData(parsedData, "latitude")
	parsedLong, _ := getFloat64FromParsedData(parsedData, "longitude")
	parsedBattery, _ := getFloat64FromParsedData(parsedData, "batteryPercentage")
	fmt.Println("parsedBattery", parsedBattery)
	additionalData, ok := parsedData["Additional Data"].([]interface{})
	if !ok {
		fmt.Println("Additional Data not found or empty")
		// Continue processing even if Additional Data is not present
	}

	// Parse the ISO 8601 dateTime string
	parsedDateTime, err := time.Parse(time.RFC3339, parsedDateTimeStr)
	if err != nil {
		log.Printf("Failed to parse dateTime: %s", err)
		return
	}

	// Define the alerts map
	alerts := make(map[string]string)

	// Handle overspeed alert
	var overSpeedThresholdValue int

	// Find the overspeed alarm
	for i := range alertConfig.AlarmConfig {
		alarm := &alertConfig.AlarmConfig[i]
		if alarm.Event == "overspeed" {
			overSpeedThresholdValue = int(alarm.ThresholdValue)
			shouldSendOverspeedAlert := handleOverspeedAlert(parsedSpeed, alarm)
			if shouldSendOverspeedAlert {
				alert := fmt.Sprintf("Overspeed Alert: Your vehicle speed is %d km/h, exceeding the limit of %d km/h.", parsedSpeed, overSpeedThresholdValue)
				alerts["overSpeed"] = alert
				publishAlert(alert, "overSpeed", "System", "", parsedData)
			}
			break
		}
	}

	// Handle overstop alert
	var overStopDuration string

	// Find the overstop alarm
	for i := range alertConfig.AlarmConfig {
		alarm := &alertConfig.AlarmConfig[i]
		if alarm.Event == "overstop" {
			overStopDuration = alarm.Duration
			shouldSendOverstopAlert := handleOverstopAlert(parsedDateTime, alarm)
			if shouldSendOverstopAlert {
				alert := fmt.Sprintf("Overstop Alert: Vehicle has been stationary for more than %s.", overStopDuration)
				alerts["overStop"] = alert
				publishAlert(alert, "overStop", "System", "", parsedData)
			}
			break
		}
	}

	// Handle battery alert
	var batteryThresholdValue float64

	// Find the battery alarm
	for i := range alertConfig.AlarmConfig {
		alarm := &alertConfig.AlarmConfig[i]
		if alarm.Event == "battery" {
			batteryThresholdValue = alarm.ThresholdValue
			shouldSendBatteryAlert := false

			// Handle battery alert from additionalData if available
			if additionalData != nil {
				for _, item := range additionalData {
					if obj, ok := item.(map[string]interface{}); ok {
						if _, found := obj["batteryPercentage"]; found {
							parsedBatteryPercentage, _ := getFloat64FromParsedData(obj, "batteryPercentage")
							shouldSendBatteryAlert = handleBatteryAlert(parsedBatteryPercentage, alarm)
							if shouldSendBatteryAlert {
								alert := fmt.Sprintf("Low Battery Alert: Battery level is at %.2f%%, below the threshold of %.2f%%.", parsedBatteryPercentage, batteryThresholdValue)
								alerts["battery"] = alert
								publishAlert(alert, "battery", "System", "", parsedData)
							}
							break // Assuming there's only one Battery Percentage entry
						}
					}
				}
			} else {
				// If additionalData is not available, use parsedBattery
				shouldSendBatteryAlert = handleBatteryAlert(parsedBattery, alarm)
				if shouldSendBatteryAlert {
					alert := fmt.Sprintf("Low Battery Alert: Battery level is at %.2f%%, below the threshold of %.2f%%.", parsedBattery, batteryThresholdValue)
					alerts["battery"] = alert
					publishAlert(alert, "battery", "System", "", parsedData)
				}
			}

			break
		}
	}

	// Handle geo events
	var sourceAlertGenerated bool
	for i := range alertConfig.AlarmConfig {
		alarm := &alertConfig.AlarmConfig[i]
		if alarm.Event == "geo_in" || alarm.Event == "geo_out" {
			locationType := alarm.Location.LocationType // Should be "source" or "destination"
			alertGenerated := checkGeoEvents(parsedLat, parsedLong, &alarm.Location, alarm, parsedData, locationType, alerts)
			if locationType == "source" {
				sourceAlertGenerated = sourceAlertGenerated || alertGenerated
			}
			// Optionally handle alert generation logic here
		}
	}

	// If no alert was generated at the source, check destination position for geo_in event
	if !sourceAlertGenerated {
		for i := range alertConfig.AlarmConfig {
			alarm := &alertConfig.AlarmConfig[i]
			if alarm.Event == "geo_in" || alarm.Event == "geo_out" {
				if alarm.Location.LocationType == "destination" {
					checkGeoEvents(parsedLat, parsedLong, &alarm.Location, alarm, parsedData, "destination", alerts)
					break
				}
			}
		}
	}

	// Update alertConfig if alerts are generated
	if len(alerts) > 0 {
		// Optionally, persist the updated alertConfig to maintain state
		// For example, write it to a file, database, or in-memory store
		// Since we're not using Redis, we need to decide how to handle state persistence
		fmt.Println("Alerts generated and alertConfig updated.")
	}

	fmt.Println("Additional Data", parsedData)

	// Parse alerts from Additional Data
	if additionalData != nil {
		for _, dataItem := range additionalData {
			if dataMap, ok := dataItem.(map[string]interface{}); ok {
				if eventID, eventIDOk := dataMap["eventId"].(string); eventIDOk {
					if eventType, eventTypeOk := dataMap["eventType"].(string); eventTypeOk {
						eventInfo, found := getEventDescription(eventID, eventType)
						if found {
							// Extract eventContent as a string
							if eventContent, ecOk := dataMap["eventContent"].(string); ecOk && eventContent != "" {
								eventInfo.Value = eventContent
							}

							// Merge event information into parsedData
							parsedData["source"] = eventInfo.Source
							parsedData["value"] = eventInfo.Value

							// Use the event's AlertType and Description
							alertMessage := eventInfo.Description
							alertType := eventInfo.AlertType

							alerts[alertMessage] = alertMessage
							publishAlert(alertMessage, alertType, eventInfo.Source, eventInfo.Value, parsedData)
						}
					}
				}
			}
		}
	}
}

func handleOverspeedAlert(parsedSpeed int, alarm *models.AlarmConfig) bool {
	threshold := int(alarm.ThresholdValue)
	if parsedSpeed > threshold && !alarm.IsAlreadyGenerateAlert {
		// If the speed is above the alert threshold and an alert hasn't been sent yet
		alarm.IsAlreadyGenerateAlert = true // Mark that an alert has been sent
		return true                         // Return true to indicate that an alert should be sent
	} else if parsedSpeed <= threshold {
		// If the speed is below the alert threshold
		alarm.IsAlreadyGenerateAlert = false // Reset the alert flag
	}
	return false // No alert is needed
}

func handleBatteryAlert(parsedBattery float64, alarm *models.AlarmConfig) bool {
	threshold := alarm.ThresholdValue
	if parsedBattery < threshold && !alarm.IsAlreadyGenerateAlert {
		// If the battery percentage is below the alert threshold and an alert hasn't been sent yet
		alarm.IsAlreadyGenerateAlert = true // Mark that an alert has been sent
		return true                         // Return true to indicate that an alert should be sent
	} else if parsedBattery >= threshold {
		// If the battery percentage is above the alert threshold
		alarm.IsAlreadyGenerateAlert = false // Reset the alert flag
	}
	return false // No alert is needed
}

func handleOverstopAlert(parsedDateTime time.Time, alarm *models.AlarmConfig) bool {
	overStopDuration, err := parseDuration(alarm.Duration)
	if err == nil && time.Since(parsedDateTime) > overStopDuration {
		if !alarm.IsAlreadyGenerateAlert {
			// If the stop duration is above the alert threshold and an alert hasn't been sent yet
			alarm.IsAlreadyGenerateAlert = true // Mark that an alert has been sent
			return true                         // Return true to indicate that an alert should be sent
		}
	} else {
		// If the stop duration is below the alert threshold
		alarm.IsAlreadyGenerateAlert = false // Reset the alert flag
	}
	return false // No alert is needed
}

func checkGeoEvents(lat, long float64, location *models.Location, alarm *models.AlarmConfig, parsedData map[string]interface{}, locationType string, alerts map[string]string) bool {
	alertGenerated := false

	// Check the current geofence state (inside or outside)
	isCurrentlyInside := checkIfGeoAlarm(lat, long, location, alarm.Event)

	// Log the current status
	if isCurrentlyInside {
		fmt.Printf("Vehicle is currently within the geofence radius for %s: %s (Lat: %f, Long: %f)\n", locationType, location.Name, lat, long)
	} else {
		fmt.Printf("Vehicle is currently outside the geofence radius for %s: %s (Lat: %f, Long: %f)\n", locationType, location.Name, lat, long)
	}

	if locationType == "source" {

		if !location.IsAlreadyGenerateAlert && isCurrentlyInside {
			fmt.Printf("Geo In alert triggered for source: entering geofence of %s\n", location.Name)
			alert := fmt.Sprintf("Geofence Entry Alert: Vehicle has entered the %s area.", location.Name)
			alerts["geoin_source"] = alert
			location.IsAlreadyGenerateAlert = true
			alertGenerated = true
			publishAlert(alert, "sourceGeoIn", "System", "", parsedData)
		} else if location.IsAlreadyGenerateAlert && !isCurrentlyInside {
			// If the vehicle was inside and is now outside the geofence, send a Geo Out alert
			fmt.Printf("Geo Out alert triggered for source: exiting geofence of %s\n", location.Name)
			alert := fmt.Sprintf("Geofence Exit Alert: Vehicle has exited the %s area.", location.Name)
			alerts["geoout_source"] = alert
			location.IsAlreadyGenerateAlert = false // Reset the flag to indicate the vehicle has exited the geofence
			alertGenerated = true
			publishAlert(alert, "sourceGeoOut", "System", "", parsedData)
		} else {
			// Log that no change in geofence status was detected
			fmt.Printf("No geofence state change detected for source: %s\n", location.Name)
		}
	} else if locationType == "destination" {
		// If the vehicle was outside and is now inside the destination geofence, send a Geo In alert
		if !location.IsAlreadyGenerateAlert && isCurrentlyInside {
			fmt.Printf("Geo In alert triggered for destination: entering geofence of %s\n", location.Name)
			alert := fmt.Sprintf("Geofence Entry Alert: Vehicle has arrived at the destination %s.", location.Name)
			alerts["geoin_destination"] = alert
			location.IsAlreadyGenerateAlert = true
			alertGenerated = true
			publishAlert(alert, "destinationGeoIn", "System", "", parsedData)
		} else {
			// Log that no change in geofence status was detected
			fmt.Printf("No geofence state change detected for destination: %s\n", location.Name)
		}
	} else {
		// General geofence handling if locationType is not specified
		if alarm.Event == "geo_in" {
			if !location.IsAlreadyGenerateAlert && isCurrentlyInside {
				fmt.Printf("Geo In alert triggered for location: entering geofence of %s\n", location.Name)
				alert := fmt.Sprintf("Geofence Entry Alert: Vehicle has entered the %s area.", location.Name)
				alerts["geoin"] = alert
				location.IsAlreadyGenerateAlert = true
				alertGenerated = true
				publishAlert(alert, "geo_in", "System", "", parsedData)
			} else if location.IsAlreadyGenerateAlert && !isCurrentlyInside {
				// Reset the alert flag
				fmt.Printf("Vehicle exited geofence of %s\n", location.Name)
				location.IsAlreadyGenerateAlert = false
			} else {
				// No change in geofence status
				fmt.Printf("No geofence state change detected for location: %s\n", location.Name)
			}
		} else if alarm.Event == "geo_out" {
			if !location.IsAlreadyGenerateAlert && !isCurrentlyInside {
				fmt.Printf("Geo Out alert triggered for location: exited geofence of %s\n", location.Name)
				alert := fmt.Sprintf("Geofence Exit Alert: Vehicle has exited the %s area.", location.Name)
				alerts["geoout"] = alert
				location.IsAlreadyGenerateAlert = true
				alertGenerated = true
				publishAlert(alert, "geo_out", "System", "", parsedData)
			} else if location.IsAlreadyGenerateAlert && isCurrentlyInside {
				// Reset the alert flag
				fmt.Printf("Vehicle entered geofence of %s\n", location.Name)
				location.IsAlreadyGenerateAlert = false
			} else {
				// No change in geofence status
				fmt.Printf("No geofence state change detected for location: %s\n", location.Name)
			}
		}
	}

	return alertGenerated
}

func publishAlert(alertMessage string, alertType string, eventSource string, eventValue string, parsedData map[string]interface{}) {
	// Merge the alert message and alert type into the parsedData map
	fmt.Println(alertMessage, "alertMessagealertMessagealertMessagealertMessage")
	parsedData["alertMessage"] = alertMessage
	parsedData["alertType"] = alertType
	parsedData["source"] = eventSource
	parsedData["value"] = eventValue
	parsedData["deviceTypeAlert"] = "traqloc310p"

	// Convert the merged map to a JSON object
	alertJSON, err := json.Marshal(parsedData)
	if err != nil {
		log.Printf("Failed to marshal alert message: %s", err)
		return
	}

	// Kafka writer configuration
	kafkaWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"192.168.1.8:9092"}, // Replace with your Kafka broker address
		Topic:   "testalert",                  // Replace with your Kafka topic
	})
	defer kafkaWriter.Close()

	// Publish the message to Kafka
	err = kafkaWriter.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("alert"),
			Value: alertJSON,
		},
	)
	if err != nil {
		log.Printf("Failed to publish alert to Kafka: %s", err)
		return
	}

	log.Println("Alert published to Kafka:", alertMessage)
}

func getIntFromParsedData(parsedData map[string]interface{}, key string) (int, bool) {
	if val, ok := parsedData[key]; ok {
		if intVal, ok := val.(float64); ok {
			return int(intVal), true
		}
	}
	return 0, false
}

func getStringFromParsedData(parsedData map[string]interface{}, key string) (string, bool) {
	if val, ok := parsedData[key]; ok {
		if strVal, ok := val.(string); ok {
			return strVal, true
		}
	}
	return "", false
}

func getFloat64FromParsedData(parsedData map[string]interface{}, key string) (float64, bool) {
	if val, ok := parsedData[key]; ok {
		if floatVal, ok := val.(float64); ok {
			return floatVal, true
		}
	}
	return 0, false
}

func parseDuration(durationStr string) (time.Duration, error) {
	durationStr = strings.TrimSpace(durationStr)
	if strings.HasSuffix(durationStr, "min") {
		minStr := strings.TrimSuffix(durationStr, "min")
		min, err := strconv.Atoi(minStr)
		if err != nil {
			return 0, err
		}
		return time.Duration(min) * time.Minute, nil
	}
	return 0, fmt.Errorf("invalid duration format")
}

func checkIfGeoAlarm(lat, long float64, location *models.Location, eventType string) bool {
	geoCodeData := location.GeoCodeData
	coordinates := geoCodeData.Geometry.Coordinates
	radius := geoCodeData.Geometry.Radius

	isWithinRadius := isPointWithinRadius(lat, long, coordinates[0], coordinates[1], radius)

	switch eventType {
	case "geo_in":
		return isWithinRadius
	case "geo_out":
		return !isWithinRadius
	}
	return false
}

func isPointWithinRadius(lat1, lon1, lat2, lon2, radius float64) bool {
	const R = 6371000 // Radius of the Earth in meters
	latRad1 := lat1 * math.Pi / 180
	lonRad1 := lon1 * math.Pi / 180
	latRad2 := lat2 * math.Pi / 180
	lonRad2 := lon2 * math.Pi / 180

	dlat := latRad2 - latRad1
	dlon := lonRad2 - lonRad1

	a := math.Sin(dlat/2)*math.Sin(dlat/2) + math.Cos(latRad1)*math.Cos(latRad2)*math.Sin(dlon/2)*math.Sin(dlon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	distance := R * c // Distance in meters
	return distance <= radius
}

func getEventDescription(eventID string, eventType string) (EventInfo, bool) {
	eventDescriptions := map[string]map[string]EventInfo{
		"0000": {"00": {Description: "Shackle has been sealed manually.", AlertType: "sealed", Source: "manual", Value: ""}},
		"0001": {"00": {Description: "Shackle has been unsealed manually.", AlertType: "unsealed", Source: "manual", Value: ""}},
		"0002": {"00": {Description: "Shackle has been sealed automatically.", AlertType: "sealed", Source: "auto", Value: ""}},
		"0003": {"00": {Description: "Shackle has been sealed using RFID card.", AlertType: "sealed", Source: "rfid", Value: ""}},
		"0004": {"00": {Description: "Shackle has been unsealed using RFID card.", AlertType: "unsealed", Source: "rfid", Value: ""}},
		"0005": {"00": {Description: "Shackle has been sealed via Bluetooth.", AlertType: "sealed", Source: "bluetooth", Value: ""}},
		"0006": {"00": {Description: "Shackle has been unsealed via Bluetooth.", AlertType: "unsealed", Source: "bluetooth", Value: ""}},
		"0007": {"00": {Description: "Shackle has been sealed via web command.", AlertType: "sealed", Source: "webapp", Value: ""}},
		"0008": {"00": {Description: "Shackle has been unsealed via web command.", AlertType: "unsealed", Source: "webapp", Value: ""}},
		"0009": {"00": {Description: "Shackle has been sealed via SMS command.", AlertType: "sealed", Source: "sms", Value: ""}},
		"000A": {"00": {Description: "Shackle has been unsealed via SMS command.", AlertType: "unsealed", Source: "sms", Value: ""}},
		"000B": {"00": {Description: "Unauthorized RFID card: Shackle sealing failed.", AlertType: "sealFailed", Source: "rfid", Value: ""}},
		"000C": {"00": {Description: "Unauthorized RFID card: Shackle unsealing failed.", AlertType: "unsealFailed", Source: "rfid", Value: ""}},
		"0010": {"00": {Description: "Device Tampered: Back cover tampered or removed.", AlertType: "tamper", Source: "cover", Value: ""}},
		"0011": {"01": {Description: "Shackle or wire has been cut.", AlertType: "tamper", Source: "shackle", Value: ""}},
		"0012": {"00": {Description: "Low Battery: Device going offline.", AlertType: "lowBattery", Source: "battery", Value: ""}},
		"0017": {"01": {Description: "User Set Low Battery: Battery below user defined threshold.", AlertType: "lowBattery", Source: "userThreshold", Value: ""}},
		"001C": {"01": {Description: "Communication Error: MCU communication abnormal.", AlertType: "communicationError", Source: "", Value: ""}},
		"001D": {"00": {Description: "Device battery is fully charged.", AlertType: "fullyCharged", Source: "battery", Value: ""}},
		"001E": {"00": {Description: "Charger has been disconnected.", AlertType: "chargerDisconnected", Source: "device", Value: ""}},
		"001F": {"00": {Description: "Charger has been connected.", AlertType: "chargerConnected", Source: "device", Value: ""}},
		"0020": {"00": {Description: "Terminal shutdown initiated.", AlertType: "shutdownReminder", Source: "device", Value: ""}},
		"0090": {"00": {Description: "Firmware version update reported.", AlertType: "firmwareUpdate", Source: "device", Value: ""}},
	}

	if info, ok := eventDescriptions[eventID][eventType]; ok {
		return info, true
	}
	return EventInfo{
		Description: "Unknown Event: An unidentified event has occurred.",
		AlertType:   "unknownEvent",
		Source:      "",
		Value:       "",
	}, false
}
