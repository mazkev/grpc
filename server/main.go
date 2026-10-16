package main

import (
	"context"
	"log"
	"net"

	"google.golang.org/grpc"

	productpb "grpc-learning/gen/product"
)

type server struct {
	productpb.UnimplementedProductServiceServer
}

func (s *server) GetProduct(ctx context.Context, req *productpb.GetProductRequest) (*productpb.GetProductResponse, error) {
	log.Printf("Received: %v", req)
	return &productpb.GetProductResponse{Product: &productpb.Product{Id: req.GetId(), Name: "ptex", Price: 9000, Stock: 10}}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	productpb.RegisterProductServiceServer(s, &server{})

	log.Printf("Server running on port %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
