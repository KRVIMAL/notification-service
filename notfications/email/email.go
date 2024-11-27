package email

import (
	"fmt"
	"net/smtp"
	"os"
)

var SMTPServer = os.Getenv("SMTP_SERVER")
var SMTPPort = os.Getenv("SMTP_PORT")
var SenderEmail = os.Getenv("SENDER_EMAIL")
var SenderPassword = os.Getenv("SENDER_PASSWORD")

// SendEmail sends an email using Google SMTP
func SendEmail(recipient, subject, body string) error {
	auth := smtp.PlainAuth("", SenderEmail, SenderPassword, SMTPServer)
	to := []string{recipient}
	msg := []byte(fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s\r\n", SenderEmail, recipient, subject, body))

	err := smtp.SendMail(SMTPServer+":"+SMTPPort, auth, SenderEmail, to, msg)
	if err != nil {
		return fmt.Errorf("failed to send email: %w", err)
	}
	return nil
}
