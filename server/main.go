package main

import (
	"context"
	"log"
	"net"

	productpb "grpc-learning/gen/product"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type server struct {
	productpb.UnimplementedProductServiceServer

	// Katalog produk tersimpan di memori
	products map[string]*productpb.Product
}

func newServer() *server {
	return &server{
		products: map[string]*productpb.Product{
			"p-01": {
				Id:    "p-01",
				Name:  "Kopi Robusta Lampung",
				Price: 25000.0,
				Stock: 50,
			},
			"p-02": {
				Id:    "p-02",
				Name:  "Teh Hijau Celup",
				Price: 15000.0,
				Stock: 25,
			},
		},
	}
}

// Implementasi GetProduct sesuai kontrak di product.proto
func (s *server) GetProduct(ctx context.Context, req *productpb.GetProductRequest) (*productpb.GetProductResponse, error) {
	productID := req.GetId()
	log.Printf("[Server] Menerima pencarian produk ID: %s", productID)

	p, exists := s.products[productID]
	if !exists {
		// Jika produk tidak ditemukan, kembalikan error NotFound
		return nil, status.Errorf(codes.NotFound, "Produk dengan ID '%s' tidak ditemukan", productID)
	}

	return &productpb.GetProductResponse{
		Product: p,
	}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Gagal membuka port: %v", err)
	}

	s := grpc.NewServer()
	productpb.RegisterProductServiceServer(s, newServer())

	log.Println("🛒 Product Service (gRPC Server) berjalan di port :50051...")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Gagal menjalankan server: %v", err)
	}
}
