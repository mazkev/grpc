package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	// Import package protobuf yang sudah di-generate
	userpb "grpc-learning/gen/user"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func main() {
	serverAddress := "localhost:50051"

	fmt.Println("=====================================================")
	fmt.Println("📱 gRPC Client Memulai Koneksi...")
	fmt.Println("=====================================================")

	// 1. Membuka koneksi ke gRPC Server
	// Untuk local learning, kita pakai insecure credentials (tanpa SSL/TLS)
	conn, err := grpc.NewClient(serverAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Gagal menghubungi server: %v", err)
	}
	defer conn.Close()

	// 2. Membuat Client Stub dari protobuf
	client := userpb.NewUserServiceClient(conn)

	// =========================================================
	// TEST KASUS 1: Mengambil user yang ADA di database
	// =========================================================
	fmt.Println("\n▶️ [Kasus 1] Mencari user dengan ID: 'usr-1'")

	// Selalu gunakan context dengan timeout agar client tidak menggantung selamanya
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Panggil fungsi RPC GetUser seolah-olah fungsi lokal biasa!
	req := &userpb.GetUserRequest{Id: "usr-1"}
	resp, err := client.GetUser(ctx, req)
	if err != nil {
		log.Fatalf("Gagal memanggil GetUser: %v", err)
	}

	user := resp.GetUser()
	fmt.Println("✅ [Client] Berhasil menerima data dari server:")
	fmt.Printf("   • ID    : %s\n", user.GetId())
	fmt.Printf("   • Nama  : %s\n", user.GetName())
	fmt.Printf("   • Email : %s\n", user.GetEmail())

	// =========================================================
	// TEST KASUS 2: Mengambil user yang TIDAK ADA (Error Handling)
	// =========================================================
	fmt.Println("\n▶️ [Kasus 2] Mencari user dengan ID yang tidak ada: 'usr-999'")

	ctxErr, cancelErr := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelErr()

	_, err = client.GetUser(ctxErr, &userpb.GetUserRequest{Id: "usr-999"})
	if err != nil {
		// Menangkap error terstruktur dari gRPC
		st, ok := status.FromError(err)
		if ok {
			fmt.Println("⚠️ [Client] Menangkap error standar dari server:")
			fmt.Printf("   • Kode Error : %s (%d)\n", st.Code(), st.Code())
			fmt.Printf("   • Pesan Error: %s\n", st.Message())
		} else {
			fmt.Printf("Bukan error gRPC: %v\n", err)
		}
	}

	// =========================================================
	// TEST KASUS 3: SERVER STREAMING (ListUsers)
	// =========================================================
	fmt.Println("\n▶️ [Kasus 3] Server Streaming: Mengambil semua user (ListUsers)")

	ctxStream, cancelStream := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelStream()

	// Panggil method server streaming
	stream, err := client.ListUsers(ctxStream, &userpb.ListUsersRequest{})
	if err != nil {
		log.Fatalf("Gagal memanggil ListUsers: %v", err)
	}

	fmt.Println("📡 Menunggu stream data mengalir dari server...")
	itemCount := 0
	for {
		// Terima data dari stream satu per satu
		streamResp, err := stream.Recv()
		if err == io.EOF {
			// io.EOF berarti server sudah selesai mengirim seluruh stream
			fmt.Println("🏁 [Client] Sinyal EOF diterima: Server telah selesai streaming.")
			break
		}
		if err != nil {
			log.Fatalf("Error saat menerima stream: %v", err)
		}

		itemCount++
		u := streamResp.GetUser()
		fmt.Printf("   📥 [Item %d Masuk] %s (%s) - Email: %s\n", itemCount, u.GetName(), u.GetId(), u.GetEmail())
	}

	fmt.Println("\n=====================================================")
	fmt.Println("🎉 Pengujian Client Selesai dengan Sukses!")
	fmt.Println("=====================================================")
}
