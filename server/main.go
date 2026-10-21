package main

import (
	"context"
	pb "grpc-learning/gen/product"
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Dummy Database Produk di Memori Server
var productDB = map[string]*pb.Product{
	"E01": {
		Id:       "E01",
		Name:     "Laptop Gaming ROG",
		Price:    25000000,
		Stock:    5,
		Category: pb.Category_ELECTRONICS,
	},
	"E02": {
		Id:       "E02",
		Name:     "Mechanical Keyboard RGB",
		Price:    1200000,
		Stock:    15,
		Category: pb.Category_ELECTRONICS,
	},
	"F01": {
		Id:       "F01",
		Name:     "Kopi Susu Gula Aren",
		Price:    18000,
		Stock:    100,
		Category: pb.Category_FOOD,
	},
}

// Struct Server yang Mengimplementasikan Service gRPC
type server struct {
	pb.UnimplementedProductServiceServer
}

// Implementasi Unary RPC: GetProduct
func (s *server) GetProduct(ctx context.Context, req *pb.GetProductRequest) (*pb.GetProductResponse, error) {
	log.Printf("📥 [Server] Kasir mencari produk dengan ID: '%s'", req.GetId())

	// 1. Cari produk di database memori kita
	product, found := productDB[req.GetId()]
	if !found {
		log.Printf("⚠️  [Server] Produk ID '%s' tidak ditemukan!", req.GetId())
		return nil, status.Errorf(codes.NotFound, "produk dengan ID '%s' tidak ditemukan di gudang", req.GetId())
	}

	log.Printf("✅ [Server] Produk ditemukan: %s (Rp %.0f)", product.GetName(), product.GetPrice())

	// 2. Kembalikan response yang dibungkus struct GetProductResponse
	return &pb.GetProductResponse{
		Product: product,
	}, nil
}

// Implementasi Unary RPC: ListProducts (Mendukung repeated & filter enum)
func (s *server) ListProducts(ctx context.Context, req *pb.ListProductsRequest) (*pb.ListProductsResponse, error) {
	log.Printf("📋 [Server] Kasir meminta daftar katalog produk (Filter Kategori: %v)...", req.GetFilterCategory())

	var hasil []*pb.Product

	// 1. Lakukan perulangan (looping) pada seluruh database produk kita
	for _, product := range productDB {
		// Jika kasir memilih CATEGORY_UNSPECIFIED (0), artinya tampilkan SEMUA produk tanpa filter
		// Jika kasir memilih kategori tertentu, hanya masukkan produk yang kategorinya cocok
		if req.GetFilterCategory() == pb.Category_CATEGORY_UNSPECIFIED || product.GetCategory() == req.GetFilterCategory() {
			hasil = append(hasil, product)
		}
	}

	log.Printf("✅ [Server] Mengembalikan %d barang ke kasir", len(hasil))

	// 2. Kembalikan slice 'hasil' ke dalam field 'Products' (repeated)
	return &pb.ListProductsResponse{
		Products: hasil,
	}, nil
}


func main() {
	// 1. Buka socket TCP di port 50051
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("❌ Gagal membuka port 50051: %v", err)
	}

	// 2. Buat instance server gRPC
	grpcServer := grpc.NewServer()

	// 3. Daftarkan service produk kita ke server gRPC
	pb.RegisterProductServiceServer(grpcServer, &server{})

	log.Printf("🚀 [Server] gRPC Server aktif berjalan di port %v ...", lis.Addr())

	// 4. Mulai layani permintaan yang masuk
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("❌ Gagal menjalankan server: %v", err)
	}
}
