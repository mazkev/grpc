package main

import (
	"bufio"
	"context"
	"fmt"
	pb "grpc-learning/gen/product"
	"io"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
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

// Implementasi Bidirectional Streaming RPC: LiveChat
func (s *server) LiveChat(stream grpc.BidiStreamingServer[pb.ChatMessage, pb.ChatResponse]) error {
	fmt.Println("\n==================================================")
	fmt.Println("💬 [Server] SESI CHAT GUDANG DENGAN KASIR DIMULAI!")
	fmt.Println("   Ketik balasan Anda lalu tekan [ENTER]")
	fmt.Println("==================================================")

	done := make(chan struct{})

	// 1. Goroutine Background: Terus mendengarkan pesan masuk dari Kasir
	go func() {
		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				fmt.Println("\n🚪 [Server] Kasir telah mengakhiri obrolan.")
				close(done)
				return
			}
			if err != nil {
				log.Printf("⚠️  [Server] Error menerima pesan kasir: %v", err)
				close(done)
				return
			}

			// Cetak pesan kasir di layar Server
			fmt.Printf("\n📩 [%s]: %s\nAnda (Gudang) > ", msg.GetSender(), msg.GetText())
		}
	}()

	// 2. Loop Utama: Membaca ketikan keyboard operator Gudang di terminal
	scanner := bufio.NewScanner(os.Stdin)
	for {
		select {
		case <-done:
			return nil
		default:
			fmt.Print("Anda (Gudang) > ")
			if !scanner.Scan() {
				return scanner.Err()
			}

			balasan := scanner.Text()
			if balasan == "" {
				continue
			}

			// Kirim balasan gudang ke Kasir lewat stream
			err := stream.Send(&pb.ChatResponse{
				Reply: balasan,
			})
			if err != nil {
				return err
			}
		}
	}
}

// 1. Fungsi Mandiri: Memeriksa Kartu Akses / Token (Satpam Pusat)
func authorize(ctx context.Context) error {
	// Ekstrak metadata dari context yang dikirim tamu
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Errorf(codes.Unauthenticated, "❌ Satpam: Akses ditolak! Tamu tidak membawa kartu identitas (metadata)")
	}

	// Periksa header 'authorization'
	tokens := md.Get("authorization")
	if len(tokens) == 0 || tokens[0] != "token-rahasia-123" {
		return status.Errorf(codes.Unauthenticated, "❌ Satpam: Akses ditolak! Kartu identitas (token) palsu atau tidak berlaku")
	}

	return nil // Token sah!
}

// 2. Satpam untuk Unary RPC (GetProduct & ListProducts)
func authUnaryInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	log.Printf("👮‍♂️ [Satpam Unary] Tamu datang hendak menuju ruangan: %s", info.FullMethod)

	// Periksa identitas di lobi
	if err := authorize(ctx); err != nil {
		log.Printf("⛔ [Satpam Unary] Mencegat dan mengusir tamu dari: %s", info.FullMethod)
		return nil, err
	}

	log.Printf("🟢 [Satpam Unary] Tiket valid! Mempersilakan tamu masuk ke: %s", info.FullMethod)
	// Persilakan tamu masuk ke fungsi tujuan aslinya
	return handler(ctx, req)
}

// 3. Satpam untuk Streaming RPC (LiveChat)
func authStreamInterceptor(
	srv any,
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	log.Printf("👮‍♂️ [Satpam Stream] Tamu datang hendak membuka sambungan live: %s", info.FullMethod)

	// Periksa identitas di lobi
	if err := authorize(ss.Context()); err != nil {
		log.Printf("⛔ [Satpam Stream] Mencegat dan menolak streaming: %s", info.FullMethod)
		return err
	}

	log.Printf("🟢 [Satpam Stream] Tiket valid! Mempersilakan streaming: %s", info.FullMethod)
	return handler(srv, ss)
}



func main() {
	// 1. Buka socket TCP di port 50051
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("❌ Gagal membuka port 50051: %v", err)
	}

	// Pasang kedua satpam di pintu masuk server gRPC
	grpcServer := grpc.NewServer(
    grpc.UnaryInterceptor(authUnaryInterceptor),
    grpc.StreamInterceptor(authStreamInterceptor),
)

	// 3. Daftarkan service produk kita ke server gRPC
	pb.RegisterProductServiceServer(grpcServer, &server{})

	log.Printf("🚀 [Server] gRPC Server aktif berjalan di port %v ...", lis.Addr())

	// 4. Mulai layani permintaan yang masuk
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("❌ Gagal menjalankan server: %v", err)
	}
}
