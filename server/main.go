package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	// Import hasil generate dari folder gen/user
	userpb "grpc-learning/gen/user"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// 1. Struct server: mengimplementasikan UserServiceServer dari user_grpc.pb.go
type userServer struct {
	// UnimplementedUserServiceServer wajib di-embed untuk forward compatibility
	userpb.UnimplementedUserServiceServer

	// Dummy database di memori untuk keperluan belajar
	db map[string]*userpb.User
}

// Fungsi pembantu untuk membuat server dengan data awal
func newUserServer() *userServer {
	return &userServer{
		db: map[string]*userpb.User{
			"usr-1": {
				Id:    "usr-1",
				Name:  "Budi Santoso",
				Email: "budi@example.com",
			},
			"usr-2": {
				Id:    "usr-2",
				Name:  "Siti Rahma",
				Email: "siti@example.com",
			},
		},
	}
}

// 2. Logika Bisnis: Implementasi fungsi GetUser sesuai kontrak di user.proto
func (s *userServer) GetUser(ctx context.Context, req *userpb.GetUserRequest) (*userpb.UserResponse, error) {
	requestedID := req.GetId()
	log.Printf("[Server] Menerima request GetUser dengan ID: %s", requestedID)

	// Cari user di database memori
	user, exists := s.db[requestedID]
	if !exists {
		// Jika tidak ada, kembalikan kode error standar gRPC (NotFound)
		return nil, status.Errorf(codes.NotFound, "User dengan ID '%s' tidak ditemukan", requestedID)
	}

	// Kembalikan UserResponse yang membungkus data user
	return &userpb.UserResponse{
		User: user,
	}, nil
}

// 3. Server Streaming: Mengirim data user satu per satu secara streaming ke client
func (s *userServer) ListUsers(req *userpb.ListUsersRequest, stream grpc.ServerStreamingServer[userpb.UserResponse]) error {
	log.Printf("[Server] Menerima request ListUsers. Mulai streaming %d user...", len(s.db))

	for _, user := range s.db {
		log.Printf("[Server] Mengirim user lewat stream: %s (%s)", user.GetName(), user.GetId())
		if err := stream.Send(&userpb.UserResponse{User: user}); err != nil {
			return err
		}
		// Beri jeda 800ms agar kita bisa mengamati streaming secara visual
		time.Sleep(800 * time.Millisecond)
	}

	log.Println("[Server] Selesai mengirim seluruh stream user.")
	return nil
}


func main() {
	port := ":50051"

	// 3. Buka koneksi TCP di port 50051
	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Gagal membuka port %s: %v", port, err)
	}

	// 4. Buat instance server gRPC
	grpcServer := grpc.NewServer()

	// 5. Daftarkan service kita ke server gRPC
	serverInstance := newUserServer()
	userpb.RegisterUserServiceServer(grpcServer, serverInstance)

	fmt.Println("=====================================================")
	fmt.Printf("🚀 gRPC Server sedang berjalan di port %s...\n", port)
	fmt.Println("   Menunggu panggilan dari client...")
	fmt.Println("=====================================================")

	// 6. Mulai melayani request yang masuk
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Server berhenti dengan error: %v", err)
	}
}
