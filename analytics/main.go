package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"
)

func main() {
	log.Println("📬 [Analytics Service] Menghubungkan ke Apache Kafka di localhost:9092...")

	// 1. Inisialisasi Kafka Reader (Consumer)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "product-activity",
		GroupID:  "analytics-consumer-group", // Consumer Group ID
		MinBytes: 10e3,                      // 10KB
		MaxBytes: 10e6,                      // 10MB
	})
	defer reader.Close()

	log.Println("==================================================")
	log.Println("📊 ANALYTICS & AUDIT STREAM SERVICE (KAFKA)")
	log.Println("   Topic  : product-activity")
	log.Println("   Group  : analytics-consumer-group")
	log.Println("   Status : Mendengarkan aliran commit log secara real-time...")
	log.Println("   Tekan CTRL+C untuk berhenti")
	log.Println("==================================================")

	// Tangani sinyal graceful shutdown (CTRL+C)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	for {
		// 2. Baca pesan dari Kafka stream
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("🏁 [Analytics Service] Berhenti membaca stream. Sampai jumpa!")
				return
			}
			log.Printf("⚠️ Error membaca dari Kafka: %v", err)
			continue
		}

		// 3. Tampilkan informasi Offset, Partition, dan Nilai Pesan
		log.Printf("📝 [Kafka Stream]: Partition: %d | Offset: %d | Key: '%s'",
			m.Partition, m.Offset, string(m.Key))
		log.Printf("   📊 Data Log: %s", string(m.Value))
	}
}
