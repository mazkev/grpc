package main

import (
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type EventMessage struct {
	Event     string `json:"event"`
	Sender    string `json:"sender,omitempty"`
	Detail    string `json:"detail"`
	Timestamp string `json:"timestamp"`
}

func main() {
	log.Println("🐇 [Worker] Menghubungkan worker ke RabbitMQ di localhost:5672...")

	// 1. Konek ke server RabbitMQ
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("❌ [Worker] Gagal konek ke RabbitMQ: %v", err)
	}
	defer conn.Close()

	// 2. Buka Channel komunikasi
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("❌ [Worker] Gagal membuka channel: %v", err)
	}
	defer ch.Close()

	// 3. Pastikan antrean (Queue) bernama 'event_queue' sudah terdaftar
	q, err := ch.QueueDeclare(
		"event_queue", // Nama antrean
		true,          // durable (antrean tetap aman walau RabbitMQ restart)
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		log.Fatalf("❌ [Worker] Gagal mendeklarasikan antrean: %v", err)
	}

	// 4. Dengarkan pesan masuk dari antrean (Consumer)
	msgs, err := ch.Consume(
		q.Name, // Nama antrean
		"",     // consumer tag
		true,   // auto-ack (otomatis konfirmasi pesan diterima)
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Fatalf("❌ [Worker] Gagal mendaftar consumer: %v", err)
	}

	log.Println("==================================================")
	log.Println("👷 WORKER SERVICE SIAP SIAGA!")
	log.Println("   Menunggu event tugas dari server gRPC...")
	log.Println("   Tekan CTRL+C untuk berhenti")
	log.Println("==================================================")

	// Loop mendengarkan pesan selamanya
	for d := range msgs {
		var ev EventMessage
		if err := json.Unmarshal(d.Body, &ev); err == nil {
			log.Printf("📩 [Worker - Tugas Masuk]: Jenis: [%s] | Pengirim: [%s] | Pesan: '%s' (Waktu: %s)",
				ev.Event, ev.Sender, ev.Detail, ev.Timestamp)
			log.Printf("   ⚙️  Memproses tugas background (simulasi audit log & notifikasi)... Selesai! ✅")
		} else {
			log.Printf("📩 [Worker - Pesan Mentah Masuk]: %s", string(d.Body))
		}
	}
}
