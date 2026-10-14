package main

import (
	"context"
	"log"
	"time"

	hellopb "grpc-learning/gen/hello"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// 1. Konek ke server
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	// 2. Buat client stub
	client := hellopb.NewGreeterClient(conn)

	// 3. Panggil fungsi SayHello
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := client.SayHello(ctx, &hellopb.HelloRequest{Name: "Kevin"})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}

	// 4. Cetak hasil
	log.Printf("Response dari server: %s", resp.GetMessage())
}
