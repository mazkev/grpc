package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	pb "grpc-learning/gen/product"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const (
	targetAddress = "localhost:50051"
	apiSecretKey  = "token-rahasia-123"
)

// clientAuthInterceptor otomatis menyisipkan token ke metadata request
func clientAuthInterceptor(
	ctx context.Context,
	method string,
	req, reply any,
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", apiSecretKey)
	return invoker(ctx, method, req, reply, cc, opts...)
}

func clientStreamAuthInterceptor(
	ctx context.Context,
	desc *grpc.StreamDesc,
	cc *grpc.ClientConn,
	method string,
	streamer grpc.Streamer,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", apiSecretKey)
	return streamer(ctx, desc, cc, method, opts...)
}

type LatencyStats struct {
	TotalRequests int64
	SuccessCount  int64
	FailureCount  int64
	Durations     []time.Duration
	TotalDuration time.Duration
}

func printReport(title string, stats LatencyStats) {
	fmt.Println("\n" + "==========================================================")
	fmt.Printf("   📊 LAPORAN HASIL LOAD TESTING: %s\n", title)
	fmt.Println("==========================================================")
	fmt.Printf("Total Permintaan      : %d\n", stats.TotalRequests)
	fmt.Printf("Berhasil              : %d (%.2f%%)\n", stats.SuccessCount, float64(stats.SuccessCount)/float64(stats.TotalRequests)*100)
	fmt.Printf("Gagal                 : %d (%.2f%%)\n", stats.FailureCount, float64(stats.FailureCount)/float64(stats.TotalRequests)*100)
	fmt.Printf("Total Waktu Eksekusi  : %v\n", stats.TotalDuration)

	rps := float64(stats.TotalRequests) / stats.TotalDuration.Seconds()
	fmt.Printf("Throughput (RPS)      : \033[1;32m%.2f Requests/detik\033[0m\n", rps)

	if len(stats.Durations) > 0 {
		sort.Slice(stats.Durations, func(i, j int) bool {
			return stats.Durations[i] < stats.Durations[j]
		})

		var sum time.Duration
		for _, d := range stats.Durations {
			sum += d
		}
		avg := sum / time.Duration(len(stats.Durations))
		min := stats.Durations[0]
		max := stats.Durations[len(stats.Durations)-1]
		p50 := stats.Durations[int(float64(len(stats.Durations))*0.50)]
		p90 := stats.Durations[int(float64(len(stats.Durations))*0.90)]
		p99 := stats.Durations[int(float64(len(stats.Durations))*0.99)]

		fmt.Println("----------------------------------------------------------")
		fmt.Printf("Latensi Tercepat (Min): %v\n", min)
		fmt.Printf("Latensi Rata-rata     : %v\n", avg)
		fmt.Printf("Latensi Median (P50)  : %v\n", p50)
		fmt.Printf("Latensi 90%% User (P90): %v\n", p90)
		fmt.Printf("Latensi 99%% User (P99): %v\n", p99)
		fmt.Printf("Latensi Terlama (Max) : %v\n", max)
	}
	fmt.Println("==========================================================")
	fmt.Println("💡 Periksa grafik lonjakan realtime di Grafana: http://localhost:3000/d/grpc-overview")
	fmt.Println()
}

// 1. Skenario Flash Sale (High Concurrency Cache-Hit)
func runFlashSaleSpike(client pb.ProductServiceClient, totalRequests int, concurrency int) {
	fmt.Printf("\n🚀 Memulai Flash Sale Spike Test (%d requests dengan %d concurrent workers)...\n", totalRequests, concurrency)

	// Pastikan data pertama sudah masuk cache dulu
	_, _ = client.GetProduct(context.Background(), &pb.GetProductRequest{Id: "E01"})

	start := time.Now()
	var successCount int64
	var failureCount int64

	jobs := make(chan struct{}, totalRequests)
	for i := 0; i < totalRequests; i++ {
		jobs <- struct{}{}
	}
	close(jobs)

	var mu sync.Mutex
	durations := make([]time.Duration, 0, totalRequests)

	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localDurations := make([]time.Duration, 0, totalRequests/concurrency+10)

			for range jobs {
				t0 := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				_, err := client.GetProduct(ctx, &pb.GetProductRequest{Id: "E01"})
				cancel()
				elapsed := time.Since(t0)

				localDurations = append(localDurations, elapsed)
				if err != nil {
					atomic.AddInt64(&failureCount, 1)
				} else {
					atomic.AddInt64(&successCount, 1)
				}
			}

			mu.Lock()
			durations = append(durations, localDurations...)
			mu.Unlock()
		}()
	}

	wg.Wait()
	totalDuration := time.Since(start)

	printReport("Flash Sale Spike Test (Redis Caching)", LatencyStats{
		TotalRequests: int64(totalRequests),
		SuccessCount:  successCount,
		FailureCount:  failureCount,
		Durations:     durations,
		TotalDuration: totalDuration,
	})
}

// 2. Skenario Mixed Chaos Traffic
func runMixedChaos(client pb.ProductServiceClient, totalRequests int, concurrency int) {
	fmt.Printf("\n🌪️ Memulai Mixed Chaos Traffic Test (%d requests, %d concurrent workers)...\n", totalRequests, concurrency)

	productIDs := []string{"E01", "E02", "F01", "F02", "C01", "UNKNOWN_99"}

	start := time.Now()
	var successCount int64
	var failureCount int64

	jobs := make(chan string, totalRequests)
	for i := 0; i < totalRequests; i++ {
		jobs <- productIDs[rand.Intn(len(productIDs))]
	}
	close(jobs)

	var mu sync.Mutex
	durations := make([]time.Duration, 0, totalRequests)

	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localDurations := make([]time.Duration, 0, totalRequests/concurrency+10)

			for pid := range jobs {
				t0 := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				_, err := client.GetProduct(ctx, &pb.GetProductRequest{Id: pid})
				cancel()
				elapsed := time.Since(t0)

				localDurations = append(localDurations, elapsed)
				if err != nil {
					atomic.AddInt64(&failureCount, 1)
				} else {
					atomic.AddInt64(&successCount, 1)
				}
			}

			mu.Lock()
			durations = append(durations, localDurations...)
			mu.Unlock()
		}()
	}

	wg.Wait()
	totalDuration := time.Since(start)

	printReport("Mixed Chaos Traffic Test", LatencyStats{
		TotalRequests: int64(totalRequests),
		SuccessCount:  successCount,
		FailureCount:  failureCount,
		Durations:     durations,
		TotalDuration: totalDuration,
	})
}

// 3. Skenario Chat Stream Bombardment
func runChatBombardment(client pb.ProductServiceClient, totalStreams int, messagesPerStream int) {
	totalExpectedMessages := totalStreams * messagesPerStream
	fmt.Printf("\n💬 Memulai Chat Stream Bombardment (%d streams simultan x %d pesan = %d total pesan)...\n",
		totalStreams, messagesPerStream, totalExpectedMessages)

	start := time.Now()
	var successMessages int64
	var failureMessages int64

	var wg sync.WaitGroup
	for s := 0; s < totalStreams; s++ {
		wg.Add(1)
		go func(streamID int) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			stream, err := client.LiveChat(ctx)
			if err != nil {
				atomic.AddInt64(&failureMessages, int64(messagesPerStream))
				return
			}

			// Goroutine reader server response (agar stream tidak blocking)
			go func() {
				for {
					_, err := stream.Recv()
					if err != nil {
						return
					}
				}
			}()

			for m := 0; m < messagesPerStream; m++ {
				err := stream.Send(&pb.ChatMessage{
					Sender: fmt.Sprintf("Bot-Kasir-%02d", streamID),
					Text:   fmt.Sprintf("Load test chat ping #%d [%s]", m+1, time.Now().Format("15:04:05.000")),
				})

				if err != nil {
					atomic.AddInt64(&failureMessages, 1)
				} else {
					atomic.AddInt64(&successMessages, 1)
				}
				time.Sleep(10 * time.Millisecond) // micro-throttle
			}

			_ = stream.CloseSend()
		}(s + 1)
	}

	wg.Wait()
	totalDuration := time.Since(start)

	printReport("Chat Streaming Bombardment", LatencyStats{
		TotalRequests: int64(totalExpectedMessages),
		SuccessCount:  successMessages,
		FailureCount:  failureMessages,
		TotalDuration: totalDuration,
	})
}

func main() {
	conn, err := grpc.NewClient(
		targetAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(clientAuthInterceptor),
		grpc.WithStreamInterceptor(clientStreamAuthInterceptor),
	)
	if err != nil {
		fmt.Printf("❌ Gagal terhubung ke gRPC server: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := pb.NewProductServiceClient(conn)

	for {
		fmt.Println("==========================================================")
		fmt.Println("       🔥 gRPC STRESS & LOAD TESTING SUITE 🔥             ")
		fmt.Println("==========================================================")
		fmt.Println("1. 🚀 Flash Sale Spike Test (2.000 Request, 50 Workers)")
		fmt.Println("2. 🌪️ Mixed Chaos Traffic Test (3.000 Request acak)")
		fmt.Println("3. 💬 Chat Stream Bombardment (20 Stream x 50 Pesan = 1.000 Pesan)")
		fmt.Println("4. ⚙️ Custom Stress Test (Atur Request & Concurrency Sendiri)")
		fmt.Println("5. 🚪 Keluar")
		fmt.Println("==========================================================")
		fmt.Print("Pilih opsi pengujian [1-5]: ")

		var choice int
		_, err := fmt.Scanln(&choice)
		if err != nil {
			break
		}

		switch choice {
		case 1:
			runFlashSaleSpike(client, 2000, 50)
		case 2:
			runMixedChaos(client, 3000, 50)
		case 3:
			runChatBombardment(client, 20, 50)
		case 4:
			var req, workers int
			fmt.Print("Masukkan Total Request: ")
			fmt.Scanln(&req)
			fmt.Print("Masukkan Jumlah Concurrent Workers: ")
			fmt.Scanln(&workers)
			if req > 0 && workers > 0 {
				runFlashSaleSpike(client, req, workers)
			}
		case 5:
			fmt.Println("👋 Keluar dari Load Testing Suite.")
			return
		default:
			fmt.Println("⚠️ Pilihan tidak valid.")
		}
	}
}
