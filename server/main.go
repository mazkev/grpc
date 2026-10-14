package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
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

	mu  sync.RWMutex
	seq int
	// Dummy database di memori untuk keperluan belajar
	db  map[string]*userpb.User
}

// Fungsi pembantu untuk membuat server dengan data awal
func newUserServer() *userServer {
	return &userServer{
		seq: 2,
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

// 4. Client Streaming: Menerima banyak stream user dari client, lalu membalas 1 ringkasan
func (s *userServer) CreateUsers(stream grpc.ClientStreamingServer[userpb.CreateUserRequest, userpb.CreateUsersSummary]) error {
	log.Println("[Server] Memulai menerima stream CreateUsers dari client...")
	var createdIDs []string

	for {
		// Terima data dari stream client
		req, err := stream.Recv()
		if err == io.EOF {
			// Sinyal EOF dari client: client sudah selesai mengirim
			log.Printf("[Server] Client selesai streaming. Total dibuat: %d user.", len(createdIDs))
			// Server mengirim balasan ringkasan tunggal dan menutup stream
			return stream.SendAndClose(&userpb.CreateUsersSummary{
				TotalCreated: int32(len(createdIDs)),
				Ids:          createdIDs,
			})
		}
		if err != nil {
			return err
		}

		// Simpan user baru ke database
		s.mu.Lock()
		s.seq++
		newID := fmt.Sprintf("usr-%d", s.seq)
		newUser := &userpb.User{
			Id:    newID,
			Name:  req.GetName(),
			Email: req.GetEmail(),
		}
		s.db[newID] = newUser
		s.mu.Unlock()

		createdIDs = append(createdIDs, newID)
		log.Printf("[Server] [Stream In] Menerima & membuat user: %s (%s)", newUser.GetName(), newID)
	}
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
