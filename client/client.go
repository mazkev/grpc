package main

import (
	"context"
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

	// 2. Buat Client
	client := productpb.NewProductServiceClient(conn)

	// 3. Panggil GetProduct
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := client.GetProduct(ctx, &productpb.GetProductRequest{Id: "prod-101"})
	if err != nil {
		log.Fatalf("Gagal memanggil GetProduct: %v", err)
	}

	// 4. Cetak balasan dari server
	p := resp.GetProduct()
	log.Printf("✅ Berhasil dapat produk: %s (Harga: Rp %.2f, Stok: %d)", p.GetName(), p.GetPrice(), p.GetStock())
}
