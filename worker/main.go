package main

import (
	"encoding/json"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type InvoiceTask struct {
	Event       string  `json:"event"`
	ProductID   string  `json:"product_id"`
	ProductName string  `json:"product_name"`
	Price       float64 `json:"price"`
	CreatedAt   string  `json:"created_at"`
}

func main() {
	rmqURL := os.Getenv("RABBITMQ_URL")
	if rmqURL == "" {
		rmqURL = "amqp://guest:guest@localhost:5672/"
	}

	// 1. Hubungkan ke RabbitMQ Server
	conn, err := amqp.Dial(rmqURL)
	if err != nil {
		log.Fatalf("❌ Worker gagal terhubung ke RabbitMQ di %s: %v", rmqURL, err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("❌ Worker gagal membuka channel: %v", err)
	}
	defer ch.Close()

	// 2. Pastikan antrean 'order_queue' yang ingin diserap sudah ada
	q, err := ch.QueueDeclare(
		"order_queue",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("❌ Worker gagal deklarasi antrean: %v", err)
	}

	// 3. Mulai serap / dengarkan aliran pesan dari antrean
	msgs, err := ch.Consume(
		q.Name, // Nama antrean
		"",     // Consumer tag
		false,  // Auto-Ack: FALSE (kita kirim konfirmasi manual setelah tugas beres)
		false,  // Exclusive
		false,  // No-local
		false,  // No-wait
		nil,    // Args
	)
	if err != nil {
		log.Fatalf("❌ Worker gagal consume antrean: %v", err)
	}

	log.Println("👷 [Worker] Background Worker aktif! Menunggu tugas cetak invoice dari antrean...")
	log.Println("   (Tekan Ctrl+C untuk berhenti)")

	// 4. Loop abadi untuk memproses setiap tugas yang masuk
	for msg := range msgs {
		var task InvoiceTask
		if err := json.Unmarshal(msg.Body, &task); err != nil {
			log.Printf("⚠️  [Worker] Gagal membaca data JSON: %v", err)
			msg.Nack(false, false) // Tolak pesan jika rusak
			continue
		}

		log.Println("--------------------------------------------------")
		log.Printf("📥 [Worker] Menerima Tugas Baru: [%s] jam %s", task.Event, task.CreatedAt)
		log.Printf("🖨️  [Worker] Sedang mencetak invoice PDF untuk '%s' (Rp %.0f)...", task.ProductName, task.Price)

		// Simulasi proses cetak invoice & kirim email (butuh waktu 1.5 detik)
		time.Sleep(1500 * time.Millisecond)

		log.Printf("✅ [Worker] Invoice '%s' selesai dicetak & dikirim via email!", task.ProductName)
		log.Println("--------------------------------------------------")

		// 5. Kirim Acknowledgment ke RabbitMQ tanda tugas sukses!
		msg.Ack(false)
	}
}
