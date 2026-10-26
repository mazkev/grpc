package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	pb "grpc-learning/gen/product"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const tokenRahasia = "token-rahasia-123"

// 1. Asisten untuk Unary RPC: Otomatis selipkan token ke context keluar
func clientAuthUnaryInterceptor(
	ctx context.Context,
	method string,
	req, reply any,
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	// Sisipkan kartu identitas token ke dalam metadata
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", tokenRahasia)
	return invoker(ctx, method, req, reply, cc, opts...)
}

// 2. Asisten untuk Streaming RPC: Otomatis selipkan token ke stream keluar
func clientAuthStreamInterceptor(
	ctx context.Context,
	desc *grpc.StreamDesc,
	cc *grpc.ClientConn,
	method string,
	streamer grpc.Streamer,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", tokenRahasia)
	return streamer(ctx, desc, cc, method, opts...)
}


func main() {
	// 1. Hubungkan ke Server gRPC di port 50051
	// insecure.NewCredentials() digunakan karena di lokal kita belum memakai SSL/TLS sertifikat HTTPS
	// Pasang asisten otomatis saat membuka koneksi ke server
conn, err := grpc.NewClient(
    "localhost:50051",
    grpc.WithTransportCredentials(insecure.NewCredentials()),
    grpc.WithUnaryInterceptor(clientAuthUnaryInterceptor),
    grpc.WithStreamInterceptor(clientAuthStreamInterceptor),
)
	

	if err != nil {
		log.Fatalf("❌ Gagal terhubung ke server: %v", err)
	}
	defer conn.Close()

	// 2. Buat Client Stub
	client := pb.NewProductServiceClient(conn)

		// 3. Siapkan context dengan batas waktu (timeout) 5 detik
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 4. Panggil RPC GetProduct untuk barang "E01"
	fmt.Println("🛒 [Kasir] Mencari produk 'E01'...")
	res, err := client.GetProduct(ctx, &pb.GetProductRequest{
		Id: "F01",
	})
	if err != nil {
		log.Fatalf("❌ Error saat memanggil server: %v", err)
	}

	// 5. Tampilkan data barang yang dibalas server
	product := res.GetProduct()
	fmt.Println("==================================================")
	fmt.Println("🎉 [Kasir] SUKSES! Detail Produk Diterima dari Gudang:")
	fmt.Printf("   ID       : %s\n", product.GetId())
	fmt.Printf("   Nama     : %s\n", product.GetName())
	fmt.Printf("   Kategori : %s\n", product.GetCategory().String())
	fmt.Printf("   Harga    : Rp %.0f\n", product.GetPrice())
	fmt.Printf("   Stok     : %d unit\n", product.GetStock())
	fmt.Println("==================================================")

		// 6. Panggil RPC ListProducts untuk melihat seluruh katalog produk
	fmt.Println("\n📋 [Kasir] Meminta seluruh katalog produk dari Gudang...")
	listRes, err := client.ListProducts(ctx, &pb.ListProductsRequest{
		FilterCategory: pb.Category_FOOD, // Minta semua kategori tanpa filter
	})
	if err != nil {
		log.Fatalf("❌ Gagal mengambil katalog: %v", err)
	}

	// 7. Looping data 'repeated' dan cetak dalam bentuk tabel
	fmt.Println("--------------------------------------------------------------------------")
	fmt.Printf("%-5s | %-25s | %-13s | %-12s | %-5s\n", "ID", "NAMA PRODUK", "KATEGORI", "HARGA", "STOK")
	fmt.Println("--------------------------------------------------------------------------")
	for _, p := range listRes.GetProducts() {
		fmt.Printf("%-5s | %-25s | %-13s | Rp %-9.0f | %-5d\n",
			p.GetId(), p.GetName(), p.GetCategory().String(), p.GetPrice(), p.GetStock())
	}
	fmt.Println("--------------------------------------------------------------------------")


	// Panggil fungsi live chat di akhir main()
	runLiveChat(client)
} // <--- KURUNG KURAWAL TUTUP MAIN() HARUS DI SINI!
// 8. Fungsi Sesi Live Chat ditaruh di LUAR fungsi main():
func runLiveChat(client pb.ProductServiceClient) {
	fmt.Println("\n==================================================")
	fmt.Println("💬 [Kasir] MEMULAI SESI LIVE CHAT DENGAN GUDANG")
	fmt.Println("   Ketik 'exit' atau 'keluar' untuk mengakhiri chat")
	fmt.Println("==================================================")
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("Masukkan Nama Anda (Kasir): ")
	scanner.Scan()
	namaKasir := scanner.Text()
	if namaKasir == "" {
		namaKasir = "Kasir-01"
	}
	// Buka koneksi stream dua arah tanpa timeout kaku
	stream, err := client.LiveChat(context.Background())
	if err != nil {
		log.Fatalf("❌ Gagal memulai live chat: %v", err)
	}
	done := make(chan struct{})
	// Goroutine Background: Mendengarkan balasan dari Gudang
	go func() {
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				fmt.Println("\n🚪 [Kasir] Gudang telah menutup sesi obrolan.")
				close(done)
				return
			}
			if err != nil {
				close(done)
				return
			}
			// Cetak balasan gudang di terminal kasir
			fmt.Printf("\n📦 [Gudang]: %s\nAnda (%s) > ", res.GetReply(), namaKasir)
		}
	}()
	// Loop Mengetik Pesan Kasir
	for {
		fmt.Printf("Anda (%s) > ", namaKasir)
		if !scanner.Scan() {
			break
		}
		teks := scanner.Text()
		if teks == "exit" || teks == "keluar" {
			fmt.Println("👋 Mengakhiri obrolan...")
			_ = stream.CloseSend() // Tutup saluran kirim kasir secara elegan
			break
		}
		if teks == "" {
			continue
		}
		// Kirim chat ke Gudang lewat stream
		err := stream.Send(&pb.ChatMessage{
			Sender: namaKasir,
			Text:   teks,
		})
		if err != nil {
			log.Printf("❌ Gagal mengirim pesan: %v", err)
			break
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("⚠️  [Kasir] Error input keyboard: %v", err)
	}
	<-done // Tunggu sampai sesi benar-benar selesai
}

