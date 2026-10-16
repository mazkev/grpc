package main

import (
	"context"
	"io"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	productpb "grpc-learning/gen/product"
)

func main() {
	// 1. Konek ke Server
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Gagal konek: %v", err)
	}
	defer conn.Close()

	client := productpb.NewProductServiceClient(conn)

	// 2. Buka jalur streaming 2 arah
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := client.LiveChat(ctx)
	if err != nil {
		log.Fatalf("Gagal buka stream chat: %v", err)
	}

	// 3. Goroutine Background: Mendengarkan balasan server
	waitChannel := make(chan struct{})
	go func() {
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				close(waitChannel)
				return
			}
			if err != nil {
				log.Printf("Error saat menerima: %v", err)
				close(waitChannel)
				return
			}
			log.Printf("💬 [Gudang Balas]: %s", resp.GetReply())
		}
	}()

	// 4. Jalur Utama: Mengirim pesan dari Kasir ke Gudang
	daftarChat := []string{
		"Halo Gudang, apakah barang P-01 siap?",
		"Tolong dicek segera ya!",
		"Oke, tolong langsung dikemas!",
	}

	for _, teks := range daftarChat {
		log.Printf("📤 [Kasus Kirim]: %s", teks)
		err := stream.Send(&productpb.ChatMessage{
			Sender: "Kasir-01",
			Text:   teks,
		})
		if err != nil {
			log.Fatalf("Gagal kirim pesan: %v", err)
		}
		time.Sleep(600 * time.Millisecond) // Jeda simulasi obrolan
	}

	// 5. Tutup jalur pengiriman client dan tunggu sampai balasan selesai diterima
	stream.CloseSend()
	<-waitChannel

	log.Println("🏁 Sesi obrolan dua arah selesai!")
}
