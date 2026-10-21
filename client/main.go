package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "grpc-learning/gen/product"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// 1. Hubungkan ke Server gRPC di port 50051
	// insecure.NewCredentials() digunakan karena di lokal kita belum memakai SSL/TLS sertifikat HTTPS
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("❌ Gagal terhubung ke server: %v", err)
	}
	defer conn.Close()

	// 2. Buat Client Stub
	client := pb.NewProductServiceClient(conn)

	// 3. Siapkan context dengan batas waktu (timeout) 2 detik
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 4. Panggil RPC GetProduct untuk barang "E01"
	fmt.Println("🛒 [Kasir] Mencari produk 'E01'...")
	res, err := client.GetProduct(ctx, &pb.GetProductRequest{
		Id: "F00",
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
}
