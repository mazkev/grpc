package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type ActivityLog struct {
	Action    string `json:"action"`
	Detail    string `json:"detail"`
	Timestamp string `json:"timestamp"`
}

func main() {
	// 1. Buat Kafka Reader yang mendengarkan topic 'product-activity'
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:9092"},
		Topic:       "product-activity",
		GroupID:     "analytics-group",
		StartOffset: kafka.FirstOffset, // Baca dari awal riwayat pesan
		MinBytes:    1,                 // 1 byte (baca instan tanpa buffer)
		MaxBytes:    10e6,              // 10MB
		MaxWait:     500 * time.Millisecond,
	})
	defer reader.Close()

	log.Println("📊 [Analytics Engine] Service Big Data Analytics Aktif!")
	log.Println("   Menyerap aliran event stream dari topic 'product-activity'...")
	log.Println("   (Tekan Ctrl+C untuk berhenti)")
	fmt.Println("==========================================================================")

	totalAktivitas := 0

	// 2. Loop abadi mendengarkan aliran commit log Kafka secara real-time
	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("⚠️  [Analytics] Error membaca pesan Kafka: %v", err)
			break
		}

		var event ActivityLog
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("⚠️  [Analytics] Format JSON tidak valid: %v", err)
			continue
		}

		totalAktivitas++

		// 3. Tampilkan hasil agregasi analitik ke layar terminal
		fmt.Printf("📈 [Event #%d] Offset: %-4d | Partisi: %d | Waktu: %s\n",
			totalAktivitas, msg.Offset, msg.Partition, event.Timestamp)
		fmt.Printf("   Aksi   : \033[1;36m%-18s\033[0m\n", event.Action)
		fmt.Printf("   Detail : %s\n", event.Detail)
		fmt.Println("--------------------------------------------------------------------------")
	}
}
