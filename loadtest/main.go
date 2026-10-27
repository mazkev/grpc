package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	pb "grpc-learning/gen/product"
	"grpc-learning/pkg/tracer"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const (
	targetAddress = "localhost:50051"
	tokenRahasia  = "token-rahasia-123"
	totalRequests = 1000 // Total 1.000 panggilan
	concurrency   = 25   // 25 kasir menembak serentak
)

func main() {
	// Inisialisasi OpenTelemetry pelacak untuk Load Tester
	tp, err := tracer.InitTracer(context.Background(), "grpc-load-tester")
	if err == nil {
		defer tp.Shutdown(context.Background())
	}

	fmt.Println("==================================================")
	fmt.Printf("🚀 MEMULAI LOAD TEST: %d REQUESTS (CONCURRENCY: %d)\n", totalRequests, concurrency)
	fmt.Println("==================================================")

	conn, err := grpc.NewClient(
		targetAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()), // Kirim semua jejak ke Jaeger!
	)
	if err != nil {
		log.Fatalf("❌ Gagal terhubung ke server: %v", err)
	}
	defer conn.Close()

	client := pb.NewProductServiceClient(conn)

	jobs := make(chan int, totalRequests)
	for i := 1; i <= totalRequests; i++ {
		jobs <- i
	}
	close(jobs)

	var (
		successCount uint64
		failCount    uint64
		wg           sync.WaitGroup
	)

	startTime := time.Now()

	for w := 1; w <= concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			productIDs := []string{"E01", "E02", "F01"}

			for jobID := range jobs {
				targetID := productIDs[jobID%len(productIDs)]

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				ctx = metadata.AppendToOutgoingContext(ctx, "authorization", tokenRahasia)

				_, err := client.GetProduct(ctx, &pb.GetProductRequest{Id: targetID})
				cancel()

				if err != nil {
					atomic.AddUint64(&failCount, 1)
				} else {
					atomic.AddUint64(&successCount, 1)
				}
			}
		}(w)
	}

	wg.Wait()
	duration := time.Since(startTime)

	rps := float64(successCount) / duration.Seconds()

	fmt.Println("\n==================================================")
	fmt.Println("📊 HASIL STRESS TEST SELESAI:")
	fmt.Printf("   Total Waktu       : %v\n", duration)
	fmt.Printf("   Sukses            : %d requests\n", successCount)
	fmt.Printf("   Gagal             : %d requests\n", failCount)
	fmt.Printf("   Throughput (RPS)  : %.2f requests/detik ⚡\n", rps)
	fmt.Println("==================================================")
}
