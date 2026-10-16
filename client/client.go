package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"

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

	// 2. Buka jalur streaming 2 arah (tanpa timeout singkat agar Anda bisa santai mengetik)
	ctx := context.Background()
	stream, err := client.LiveChat(ctx)
	if err != nil {
		log.Fatalf("Gagal buka stream chat: %v", err)
	}

	// 3. Goroutine Background: Mendengarkan balasan server secara real-time
	waitChannel := make(chan struct{})
	go func() {
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				close(waitChannel)
				return
			}
			if err != nil {
				log.Printf("Error saat menerima dari server: %v", err)
				close(waitChannel)
				return
			}
			// Tampilkan balasan server di layar
			fmt.Printf("\n💬 [Gudang Balas]: %s\nAnda (Kasir) > ", resp.GetReply())
		}
	}()

	// 4. Jalur Utama: Membaca input ketikan keyboard Anda dari terminal
	fmt.Println("==================================================")
	fmt.Println("💬 CHAT REAL-TIME KASIR DENGAN GUDANG (gRPC)")
	fmt.Println("   Ketik pesan Anda lalu tekan [ENTER]")
	fmt.Println("   Ketik 'exit' atau 'keluar' untuk mengakhiri")
	fmt.Println("==================================================")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("Anda (Kasir) > ")
		if !scanner.Scan() {
			break
		}

		pesan := scanner.Text()

		// Jika mengetik exit, hentikan obrolan
		if pesan == "exit" || pesan == "keluar" {
			fmt.Println("\nMenutup sesi obrolan...")
			break
		}

		// Jika menekan enter tanpa ketikan, abaikan
		if pesan == "" {
			continue
		}

		// Kirim pesan ke server via gRPC stream
		err := stream.Send(&productpb.ChatMessage{
			Sender: "Kasir-01",
			Text:   pesan,
		})
		if err != nil {
			log.Fatalf("Gagal mengirim pesan: %v", err)
		}
	}

	// 5. Tutup jalur pengiriman client dan tunggu sampai selesai
	stream.CloseSend()
	<-waitChannel

	fmt.Println("🏁 Sesi obrolan selesai. Sampai jumpa!")
}
