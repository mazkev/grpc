package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	productpb "grpc-learning/gen/product"
)

type server struct {
	productpb.UnimplementedProductServiceServer
}

// 1. Fungsi Mandiri: Memeriksa Token (Satpam Pusat)
func authorize(ctx context.Context) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Errorf(codes.Unauthenticated, "akses ditolak: metadata tidak ditemukan")
	}

	tokens := md.Get("authorization")
	if len(tokens) == 0 || tokens[0] != "token-rahasia-123" {
		return status.Errorf(codes.Unauthenticated, "akses ditolak: token salah atau tidak valid")
	}
	return nil
}

// 2. Interceptor untuk Streaming RPC (seperti LiveChat)
func authStreamInterceptor(
	srv any,
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	log.Printf("🛡️  [Lobi Interceptor - Stream] Tamu menuju: %s", info.FullMethod)

	// Periksa tiket di gerbang lobi
	if err := authorize(ss.Context()); err != nil {
		log.Printf("❌ [Lobi Interceptor - Stream] Akses ditolak untuk: %s", info.FullMethod)
		return err
	}

	log.Printf("✅ [Lobi Interceptor - Stream] Tiket valid! Mempersilakan masuk ke: %s", info.FullMethod)
	return handler(srv, ss)
}

// 3. Interceptor untuk Unary RPC (seperti GetProduct)
func authUnaryInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	log.Printf("🛡️  [Lobi Interceptor - Unary] Tamu menuju: %s", info.FullMethod)

	if err := authorize(ctx); err != nil {
		log.Printf("❌ [Lobi Interceptor - Unary] Akses ditolak untuk: %s", info.FullMethod)
		return nil, err
	}

	log.Printf("✅ [Lobi Interceptor - Unary] Tiket valid! Mempersilakan masuk ke: %s", info.FullMethod)
	return handler(ctx, req)
}

func (s *server) LiveChat(stream grpc.BidiStreamingServer[productpb.ChatMessage, productpb.ChatResponse]) error {
	// Catatan: Fungsi ini sekarang BERSIH dari kode validasi token!
	// Satpam di lobi sudah menjamin hanya client bertiket sah yang bisa sampai ke sini.
	fmt.Println("==================================================")
	fmt.Println("💬 SESI CHAT GUDANG DENGAN KASIR DIMULAI!")
	fmt.Println("   Ketik balasan Anda lalu tekan [ENTER]")
	fmt.Println("==================================================")

	done := make(chan struct{})

	// 1. Goroutine Background: Terus mendengarkan pesan masuk dari Client
	go func() {
		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				fmt.Println("\n[Info] Kasir telah menutup obrolan.")
				close(done)
				return
			}
			if err != nil {
				log.Printf("Error saat menerima pesan client: %v", err)
				close(done)
				return
			}
			// Tampilkan pesan Kasir di layar Server
			fmt.Printf("\n📩 [Kasir]: %s\nAnda (Gudang) > ", msg.GetText())
		}
	}()

	// 2. Jalur Utama: Membaca ketikan keyboard manusia di terminal Server
	scanner := bufio.NewScanner(os.Stdin)
	for {
		select {
		case <-done:
			return nil
		default:
			fmt.Print("Anda (Gudang) > ")
			if !scanner.Scan() {
				if err := scanner.Err(); err != nil {
					return err
				}
				return nil
			}

			pesanBalasan := scanner.Text()
			if pesanBalasan == "" {
				continue
			}

			// Kirim balasan manual ke Client lewat gRPC stream
			err := stream.Send(&productpb.ChatResponse{
				Reply: pesanBalasan,
			})
			if err != nil {
				return err
			}
		}
	}
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// 4. Pasang Interceptor saat membuat Server gRPC
	s := grpc.NewServer(
		grpc.UnaryInterceptor(authUnaryInterceptor),
		grpc.StreamInterceptor(authStreamInterceptor),
	)
	productpb.RegisterProductServiceServer(s, &server{})

	log.Printf("Server running on port %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
