package notifications

import (
	"encoding/json"
	"log"

	"github.com/KRVIMAL/notification-service-310/config"
	"github.com/KRVIMAL/notification-service-310/notfications/alert"
	"github.com/KRVIMAL/notification-service-310/notfications/sms"
)

func HandleNotifications(cfg *config.Config, data map[string]interface{}) {
	// Serialize the map data to JSON for `HandleAlarmData`
	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Printf("Failed to serialize data to JSON: %v", err)
		return
	}

	// Handle alerts
	if event, ok := data["alert"].(string); ok && event == "ALERT" {
		err := alert.HandleAlarmData(jsonData)
		if err != nil {
			log.Printf("Error handling alarm data: %v", err)
		}
	}

	// Send SMS
	if phoneNumber, ok := data["phoneNumber"].(string); ok {
		message := "This is a test SMS for " + phoneNumber
		err := sms.SendMessage(phoneNumber, message)
		if err != nil {
			log.Printf("Failed to send SMS: %v", err)
		}
	}

	// Send email
	// if emailAddr, ok := data["email"].(string); ok {
	// 	subject := "Alert Notification"
	// 	body := "This is a test email for " + emailAddr

	// 	err := email.SendEmail(cfg, emailAddr, subject, body)
	// 	if err != nil {
	// 		log.Printf("Failed to send email: %v", err)
	// 	}
	// }
}
