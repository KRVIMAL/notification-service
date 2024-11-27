package sms

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"
)

// SendMessage sends an OTP message to the given mobile number.
func SendMessage(mobileNumber string, message string) error {
	apiUrl := os.Getenv("URL")
	if apiUrl == "" {
		return fmt.Errorf("URL environment variable is not set")
	}

	params := url.Values{}
	params.Add("method", "SendMessage")
	params.Add("v", "1.1")
	params.Add("auth_scheme", os.Getenv("AUTH_SCHEME"))
	params.Add("msg_type", os.Getenv("MSG_TYPE"))
	params.Add("format", os.Getenv("FORMAT"))
	params.Add("msg", fmt.Sprintf("IMZ - %s is the One-Time Password (OTP) for login with IMZ", message))
	params.Add("send_to", mobileNumber)
	params.Add("userid", os.Getenv("USERID"))
	params.Add("password", os.Getenv("PASSWORD"))

	// Debug prints to check environment variables
	fmt.Println("URL:", apiUrl)
	fmt.Println("auth_scheme:", os.Getenv("AUTH_SCHEME"))
	fmt.Println("msg_type:", os.Getenv("MSG_TYPE"))
	fmt.Println("format:", os.Getenv("FORMAT"))
	fmt.Println("userid:", os.Getenv("USERID"))
	fmt.Println("password:", os.Getenv("PASSWORD"))
	fmt.Println("Params:", params.Encode())

	apiUrl += "?" + params.Encode()

	client := http.Client{
		Timeout: 120 * time.Second,
	}

	resp, err := client.Get(apiUrl)
	if err != nil {
		return fmt.Errorf("failed to send OTP: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("failed to send OTP, status code: %d", resp.StatusCode)
	}

	return nil
}
