// E2E test for logzio-go
// This program sends test logs to Logz.io and verifies they were received
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	logzio "github.com/logzio/logzio-go"
)

type LogzioSearchResponse struct {
	Hits struct {
		Total int `json:"total"`
		Hits  []struct {
			Source map[string]interface{} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go [send|verify]")
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "send":
		sendLogs()
	case "verify":
		verifyLogs()
	default:
		fmt.Printf("Unknown command: %s\n", command)
		os.Exit(1)
	}
}

func sendLogs() {
	token := os.Getenv("LOGZIO_TOKEN")
	envID := os.Getenv("ENV_ID")

	if token == "" || envID == "" {
		fmt.Println("❌ LOGZIO_TOKEN and ENV_ID environment variables are required")
		os.Exit(1)
	}

	// Create a new Logz.io sender
	l, err := logzio.New(
		token,
		logzio.SetUrl("https://listener.logz.io:8071"),
		logzio.SetDebug(os.Stderr),
		logzio.SetInMemoryQueue(true),
		logzio.SetDrainDuration(time.Second*5),
	)
	if err != nil {
		fmt.Printf("❌ Failed to create logzio sender: %v\n", err)
		os.Exit(1)
	}

	// Send test logs
	for i := 1; i <= 5; i++ {
		logEntry := map[string]interface{}{
			"message":    fmt.Sprintf("E2E test log message %d", i),
			"env_id":     envID,
			"type":       envID,
			"test_index": i,
			"timestamp":  time.Now().UTC().Format(time.RFC3339),
		}

		logBytes, err := json.Marshal(logEntry)
		if err != nil {
			fmt.Printf("❌ Failed to marshal log entry: %v\n", err)
			os.Exit(1)
		}

		err = l.Send(logBytes)
		if err != nil {
			fmt.Printf("❌ Failed to send log: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("📤 Sent log %d: %s\n", i, string(logBytes))
	}

	// Drain and stop the sender
	l.Drain()
	time.Sleep(time.Second * 5)
	l.Stop()

	fmt.Println("✅ All test logs sent successfully!")
}

func verifyLogs() {
	apiKey := os.Getenv("LOGZIO_API_KEY")
	apiURL := os.Getenv("LOGZIO_API_URL")
	envID := os.Getenv("ENV_ID")

	if apiKey == "" || envID == "" {
		fmt.Println("❌ LOGZIO_API_KEY and ENV_ID environment variables are required")
		os.Exit(1)
	}

	if apiURL == "" {
		apiURL = "https://api.logz.io/v1"
	}

	searchURL := fmt.Sprintf("%s/search", apiURL)
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{"match": map[string]string{"env_id": envID}},
					{"match": map[string]string{"type": envID}},
				},
			},
		},
		"size": 10,
	}

	queryBytes, err := json.Marshal(query)
	if err != nil {
		fmt.Printf("❌ Failed to marshal query: %v\n", err)
		os.Exit(1)
	}

	req, err := http.NewRequest("POST", searchURL, bytes.NewBuffer(queryBytes))
	if err != nil {
		fmt.Printf("❌ Failed to create request: %v\n", err)
		os.Exit(1)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-TOKEN", apiKey)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("❌ Failed to execute search request: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("❌ Failed to read response body: %v\n", err)
		os.Exit(1)
	}

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("❌ Search API returned status %d: %s\n", resp.StatusCode, string(body))
		os.Exit(1)
	}

	var searchResp LogzioSearchResponse
	if err := json.Unmarshal(body, &searchResp); err != nil {
		fmt.Printf("❌ Failed to parse search response: %v\n", err)
		os.Exit(1)
	}

	totalHits := searchResp.Hits.Total
	fmt.Printf("🔍 Found %d logs with env_id=%s\n", totalHits, envID)

	if totalHits == 0 {
		fmt.Println("❌ No logs found! E2E test failed.")
		os.Exit(1)
	}

	for _, hit := range searchResp.Hits.Hits {
		source := hit.Source
		if _, ok := source["message"]; !ok {
			fmt.Println("❌ Log missing 'message' field")
			os.Exit(1)
		}
		if _, ok := source["env_id"]; !ok {
			fmt.Println("❌ Log missing 'env_id' field")
			os.Exit(1)
		}
		if _, ok := source["type"]; !ok {
			fmt.Println("❌ Log missing 'type' field")
			os.Exit(1)
		}
		fmt.Printf("✅ Verified log: %v\n", source["message"])
	}

	fmt.Printf("✅ E2E verification passed! Found %d logs with all required fields.\n", totalHits)
}
