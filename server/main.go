package main

import (
	"context"
	"fmt"
	"log"
	"net"

	hellopb "grpc-learning/gen/hello"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server mengimplementasikan interface GreeterServer dari protobuf
type server struct {
	hellopb.UnimplementedGreeterServer
}

// Logika fungsi SayHello: menerima nama, membalas salam
func (s *server) SayHello(ctx context.Context, req *hellopb.HelloRequest) (*hellopb.HelloResponse, error) {
	name := req.GetName()
	reply := fmt.Sprintf("Hello, %s!", name)

	return &hellopb.HelloResponse{
		Message: reply,
	}, nil
}

// Logika fungsi SayGoodbye: menerima nama, membalas salam perpisahan
func (s *server) SayGoodbye(ctx context.Context, req *hellopb.HelloRequest) (*hellopb.HelloResponse, error) {
	name := req.GetName()
	reply := fmt.Sprintf("Goodbye, %s! Sampai jumpa lagi.", name)

	return &hellopb.HelloResponse{
		Message: reply,
	}, nil
}

// Logika fungsi Add: menjumlahkan dua angka dari client
func (s *server) Add(ctx context.Context, req *hellopb.AddRequest) (*hellopb.AddResponse, error) {
	hasil := req.GetA() + req.GetB()
	return &hellopb.AddResponse{
		Result: hasil,
	}, nil
}

// Logika fungsi Divide: membagi a dengan b (validasi jika b == 0)
func (s *server) Divide(ctx context.Context, req *hellopb.DivideRequest) (*hellopb.DivideResponse, error) {
	if req.GetB() == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "angka pembagi (b) tidak boleh nol!")
	}
	hasil := req.GetA() / req.GetB()
	return &hellopb.DivideResponse{
		Result: hasil,
	}, nil
}




func main() {
	// 1. Listen port TCP
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// 2. Buat gRPC server
	s := grpc.NewServer()

	// 3. Register service
	hellopb.RegisterGreeterServer(s, &server{})

	log.Println("Server gRPC berjalan di port :50051...")

	// 4. Jalankan
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
