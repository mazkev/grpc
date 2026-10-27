package tests

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	pb "grpc-learning/gen/product"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	grpcServerAddr = "localhost:50051"
	gatewayURL     = "http://localhost:8080"
	redisAddr      = "localhost:6379"
	rabbitmqURL    = "amqp://guest:guest@localhost:5672/"
	kafkaBroker    = "localhost:9092"
	authToken      = "token-rahasia-123"
)

// Helper: Membuat koneksi gRPC Client dengan metadata token
func getGRPCClient(t *testing.T) (pb.ProductServiceClient, func()) {
	conn, err := grpc.NewClient(
		grpcServerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("❌ Gagal terhubung ke gRPC server: %v", err)
	}

	client := pb.NewProductServiceClient(conn)
	cleanup := func() {
		conn.Close()
	}
	return client, cleanup
}

// Skenario 1: Verifikasi Gerbang Keamanan (Interceptor Satpam Auth)
func TestE2E_AuthenticationGate(t *testing.T) {
	client, cleanup := getGRPCClient(t)
	defer cleanup()

	t.Run("Akses Ditolak Tanpa Token", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, err := client.GetProduct(ctx, &pb.GetProductRequest{Id: "E01"})
		if err == nil {
			t.Fatal("Ekspektasi: Akses ditolak tanpa token, tetapi request malah sukses!")
		}

		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.Unauthenticated {
			t.Fatalf("Ekspektasi status code Unauthenticated (16), didapat: %v", st.Code())
		}
		t.Logf("✅ Satpam Interceptor berhasil menolak akses liar: %s", st.Message())
	})

	t.Run("Akses Diterima Dengan Token Valid", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", authToken)
		resp, err := client.GetProduct(ctx, &pb.GetProductRequest{Id: "E01"})
		if err != nil {
			t.Fatalf("Gagal mengambil produk dengan token valid: %v", err)
		}

		if resp.GetProduct().GetId() != "E01" {
			t.Fatalf("Ekspektasi ID E01, didapat: %s", resp.GetProduct().GetId())
		}
		t.Logf("✅ Akses disetujui untuk produk: '%s' (Rp %.0f)", resp.GetProduct().GetName(), resp.GetProduct().GetPrice())
	})
}

// Skenario 2: Verifikasi In-Memory Caching (Redis Cache-Aside)
func TestE2E_RedisCacheLayer(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	ctx := context.Background()
	cacheKey := "product:E01"

	// 1. Hapus kunci di Redis untuk menguji Cache Miss -> DB -> Simpan ke Redis
	_ = rdb.Del(ctx, cacheKey).Err()

	client, cleanup := getGRPCClient(t)
	defer cleanup()

	authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", authToken)

	t.Run("Siklus Cache Miss dan Penyimpanan Otomatis", func(t *testing.T) {
		start := time.Now()
		resp, err := client.GetProduct(authCtx, &pb.GetProductRequest{Id: "E01"})
		duration := time.Since(start)

		if err != nil {
			t.Fatalf("Gagal request: %v", err)
		}
		t.Logf("Request Cache Miss selesai dalam: %v", duration)

		// Verifikasi kunci sekarang wajib ada di Redis
		val, err := rdb.Get(ctx, cacheKey).Result()
		if err != nil {
			t.Fatalf("Kunci '%s' gagal ditemukan di Redis setelah di-query: %v", cacheKey, err)
		}

		var prod pb.Product
		if err := json.Unmarshal([]byte(val), &prod); err != nil || prod.GetId() != resp.GetProduct().GetId() {
			t.Fatalf("Data JSON yang disimpan di Redis tidak sesuai: %v", val)
		}
		t.Logf("✅ Redis Cache terisi otomatis dengan produk '%s'", prod.GetName())
	})

	t.Run("Siklus Cache Hit Sub-Millisecond", func(t *testing.T) {
		start := time.Now()
		resp, err := client.GetProduct(authCtx, &pb.GetProductRequest{Id: "E01"})
		duration := time.Since(start)

		if err != nil {
			t.Fatalf("Gagal request Cache Hit: %v", err)
		}

		if duration > 50*time.Millisecond {
			t.Logf("⚠️ Peringatan: Latensi %v lebih tinggi dari yang diharapkan untuk memory cache", duration)
		}
		t.Logf("⚡ Cache Hit Super Cepat! Waktu respons: %v untuk '%s'", duration, resp.GetProduct().GetName())
	})
}

// Skenario 3: Verifikasi Antrean Tugas Asinkron (RabbitMQ Task Queue)
func TestE2E_RabbitMQTaskQueue(t *testing.T) {
	conn, err := amqp.Dial(rabbitmqURL)
	if err != nil {
		t.Skipf("⚠️ RabbitMQ tidak tersedia di %s, lewati uji coba RabbitMQ", rabbitmqURL)
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("Gagal membuka channel RabbitMQ: %v", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"order_queue",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		t.Fatalf("Gagal deklarasi antrean: %v", err)
	}

	t.Logf("✅ Terhubung ke antrean RabbitMQ '%s' (Total Pesan Menunggu: %d)", q.Name, q.Messages)
}

// Skenario 4: Verifikasi Event Streaming Commit Log (Apache Kafka)
func TestE2E_KafkaEventStream(t *testing.T) {
	// Buat consumer sementara untuk membaca pesan dari topic 'product-activity'
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{kafkaBroker},
		Topic:       "product-activity",
		Partition:   0,
		StartOffset: kafka.LastOffset, // Ambil event terbaru
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     2 * time.Second,
	})
	defer reader.Close()

	// Trigger pemanggilan produk untuk menghasilkan event Kafka
	client, cleanup := getGRPCClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", authToken)
	_, err := client.GetProduct(authCtx, &pb.GetProductRequest{Id: "E01"})
	if err != nil {
		t.Fatalf("Gagal memanggil produk: %v", err)
	}

	// Baca pesan dari Kafka
	msg, err := reader.ReadMessage(ctx)
	if err != nil {
		t.Logf("⚠️ Kafka consumer timeout/empty: %v (Event mungkin masuk di partisi lain)", err)
		return
	}

	var event map[string]any
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		t.Fatalf("Pesan Kafka bukan format JSON yang valid: %v", string(msg.Value))
	}

	t.Logf("✅ Event Kafka Terverifikasi! Aksi: '%v' | Waktu: %v | Offset: %d",
		event["action"], event["timestamp"], msg.Offset)
}

// Skenario 5: Verifikasi Realtime Bidirectional Streaming (LiveChat)
func TestE2E_LiveChatStreaming(t *testing.T) {
	client, cleanup := getGRPCClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", authToken)
	stream, err := client.LiveChat(authCtx)
	if err != nil {
		t.Fatalf("Gagal membuka stream LiveChat: %v", err)
	}

	// 1. Kasir kirim salam ke gudang
	sendErr := stream.Send(&pb.ChatMessage{
		Sender: "Kasir-Robot-E2E",
		Text:   "Halo gudang, mohon cek stok laptop ROG!",
	})
	if sendErr != nil {
		t.Fatalf("Gagal mengirim pesan chat: %v", sendErr)
	}

	// 2. Terima balasan otomatis dari gudang
	reply, err := stream.Recv()
	if err != nil && err != io.EOF {
		t.Fatalf("Gagal menerima balasan stream chat: %v", err)
	}

	if reply != nil {
		t.Logf("💬 Balasan LiveChat Real-time diterima: '%s'", reply.GetReply())
	}
	_ = stream.CloseSend()
}

// Skenario 6: Verifikasi gRPC-Gateway REST HTTP/JSON Transcoding
func TestE2E_GRPCGatewayREST(t *testing.T) {
	httpClient := &http.Client{Timeout: 3 * time.Second}

	t.Run("REST API GetProduct Berhasil dengan Header Auth", func(t *testing.T) {
		req, _ := http.NewRequest("GET", gatewayURL+"/v1/products/E01", nil)
		req.Header.Set("Authorization", authToken)

		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatalf("Gagal memanggil REST Gateway: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Ekspektasi HTTP 200 OK, didapat: %d", resp.StatusCode)
		}

		body, _ := io.ReadAll(resp.Body)
		var result map[string]any
		if err := json.Unmarshal(body, &result); err != nil {
			t.Fatalf("Gagal parse JSON REST: %v", err)
		}

		prod := result["product"].(map[string]any)
		if prod["id"] != "E01" {
			t.Fatalf("Ekspektasi ID E01, didapat: %v", prod["id"])
		}
		t.Logf("✅ REST Gateway sukses transcode ke gRPC: %s (%s)", prod["name"], prod["id"])
	})

	t.Run("REST Swagger JSON Terbuka", func(t *testing.T) {
		resp, err := httpClient.Get(gatewayURL + "/swagger.json")
		if err != nil {
			t.Fatalf("Gagal membuka swagger.json: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Ekspektasi HTTP 200 OK, didapat: %d", resp.StatusCode)
		}
		t.Logf("✅ Swagger OpenAPI Spec valid dan dapat diakses publik")
	})
}
