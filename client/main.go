package main

import (
	"context"
	"log"
	"time"

	productpb "grpc-learning/gen/product"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func main() {
	// 1. Hubungkan ke server
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Gagal terhubung ke server: %v", err)
	}
	defer conn.Close()

	// 2. Buat client stub
	client := productpb.NewProductServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// -------------------------------------------------------------
	// Uji Coba 1: Mencari Produk yang ADA ("p-01")
	// -------------------------------------------------------------
	log.Println("🔍 Mencari produk dengan ID: 'p-01'...")
	resp, err := client.GetProduct(ctx, &productpb.GetProductRequest{Id: "p-01"})
	if err != nil {
		log.Fatalf("Gagal mengambil produk: %v", err)
	}

	prod := resp.GetProduct()
	log.Printf("✅ Produk Ditemukan!")
	log.Printf("   • Nama  : %s", prod.GetName())
	log.Printf("   • Harga : Rp %.2f", prod.GetPrice())
	log.Printf("   • Stok  : %d unit", prod.GetStock())

	// -------------------------------------------------------------
	// Uji Coba 2: Mencari Produk yang TIDAK ADA ("p-999")
	// -------------------------------------------------------------
	log.Println("\n🔍 Mencari produk dengan ID yang tidak ada: 'p-999'...")
	_, errNotFound := client.GetProduct(ctx, &productpb.GetProductRequest{Id: "p-999"})
	if errNotFound != nil {
		st, ok := status.FromError(errNotFound)
		if ok {
			log.Printf("⚠️ Server merespons dengan error terstruktur:")
			log.Printf("   • Kode  : %s", st.Code())
			log.Printf("   • Pesan : %s", st.Message())
		}
	}
}
