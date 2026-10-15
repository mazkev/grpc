package main

import (
	"context"
	"log"
	"time"

	hellopb "grpc-learning/gen/hello"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
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

	// 4. Cetak hasil SayHello
	log.Printf("[SayHello] Response: %s", resp.GetMessage())

	// 5. Panggil fungsi SayGoodbye
	goodbyeResp, err := client.SayGoodbye(ctx, &hellopb.HelloRequest{Name: "Kevin"})
	if err != nil {
		log.Fatalf("could not say goodbye: %v", err)
	}
	log.Printf("[SayGoodbye] Response: %s", goodbyeResp.GetMessage())

	// 6. Panggil fungsi baru: Add (Penjumlahan)
	angka1 := int32(15)
	angka2 := int32(25)
	addResp, err := client.Add(ctx, &hellopb.AddRequest{A: angka1, B: angka2})
	if err != nil {
		log.Fatalf("could not add: %v", err)
	}
	log.Printf("[Add] Hasil penjumlahan %d + %d = %d", angka1, angka2, addResp.GetResult())

	// 7. Panggil fungsi Divide (Kasus Normal: 100 / 4)
	divResp, err := client.Divide(ctx, &hellopb.DivideRequest{A: 100.0, B: 4.0})
	if err != nil {
		log.Fatalf("could not divide: %v", err)
	}
	log.Printf("[Divide Normal] 100.0 / 4.0 = %.2f", divResp.GetResult())

	// 8. Panggil fungsi Divide (Kasus Error: 100 / 0)
	_, errDivZero := client.Divide(ctx, &hellopb.DivideRequest{A: 100.0, B: 0.0})
	if errDivZero != nil {
		st, ok := status.FromError(errDivZero)
		if ok {
			log.Printf("[Divide Error Terdeteksi] Code: %s | Pesan: %s", st.Code(), st.Message())
		}
	}
}


