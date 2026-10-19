package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	productpb "grpc-learning/gen/product"
)

// 1. Client Interceptor untuk Streaming (Otomatis menyelipkan token)
func clientAuthStreamInterceptor(
	ctx context.Context,
	desc *grpc.StreamDesc,
	cc *grpc.ClientConn,
	method string,
	streamer grpc.Streamer,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	log.Printf("🚀 [Client Interceptor] Menyelipkan token otomatis untuk panggilan stream: %s", method)
	// Otomatis tempelkan token ke outgoing context
	authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "token-rahasia-123")
	return streamer(authCtx, desc, cc, method, opts...)
}

// 2. Client Interceptor untuk Unary (Otomatis menyelipkan token)
func clientAuthUnaryInterceptor(
	ctx context.Context,
	method string,
	req, reply any,
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	log.Printf("🚀 [Client Interceptor] Menyelipkan token otomatis untuk panggilan unary: %s", method)
	authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "token-rahasia-123")
	return invoker(authCtx, method, req, reply, cc, opts...)
}

func main() {
	// 3. Konek ke Server dengan memasang Client Interceptor
	conn, err := grpc.NewClient(
		"localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStreamInterceptor(clientAuthStreamInterceptor),
		grpc.WithUnaryInterceptor(clientAuthUnaryInterceptor),
	)
	if err != nil {
		log.Fatalf("Gagal konek: %v", err)
	}
	defer conn.Close()

	client := productpb.NewProductServiceClient(conn)

	// 4. Uji Coba Timeout: Kasir mencari barang tapi hanya mau menunggu maksimal 1 detik
	// (Sementara Server butuh waktu 3 detik untuk memprosesnya)
	fmt.Println("--------------------------------------------------")
	fmt.Println("⏱️  [Tes Timeout] Kasir mencari 'LAPTOP-01' (Batas Waktu: 1 Detik)...")
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	prodResp, err := client.GetProduct(timeoutCtx, &productpb.GetProductRequest{Id: "LAPTOP-01"})
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.DeadlineExceeded {
			fmt.Printf("❌ [Kasir] Batalkan tunggu! Waktu habis (Timeout): %s\n", st.Message())
		} else {
			fmt.Printf("❌ [Kasir] Gagal mencari produk: %v\n", err)
		}
	} else {
		fmt.Printf("✅ [Kasir] Berhasil menemukan produk: %s (Rp %.0f)\n", prodResp.Product.Name, prodResp.Product.Price)
	}
	fmt.Println("--------------------------------------------------")

	// 5. Buka jalur streaming 2 arah (LiveChat)
	// Perhatikan: Kita cukup memakai context.Background() polos!
	// Tidak perlu lagi repot nulis metadata manual, karena sudah diurus oleh Client Interceptor.
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

	// Cek jika ada error pembacaan scanner dari terminal
	if err := scanner.Err(); err != nil {
		log.Printf("Error membaca input: %v", err)
	}

	// 5. Tutup jalur pengiriman client dan tunggu sampai selesai
	stream.CloseSend()
	<-waitChannel


	fmt.Println("🏁 Sesi obrolan selesai. Sampai jumpa!")
}
