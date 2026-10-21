package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	productpb "grpc-learning/gen/product"
)

type server struct {
	productpb.UnimplementedProductServiceServer
}

// Data master produk di memori (Dummy Database)
var productDB = []*productpb.Product{
	{Id: "E01", Name: "Laptop Gaming ROG", Price: 25000000, Stock: 5, Category: productpb.Category_ELECTRONICS},
	{Id: "E02", Name: "Mechanical Keyboard RGB", Price: 1200000, Stock: 15, Category: productpb.Category_ELECTRONICS},
	{Id: "F01", Name: "Kopi Susu Gula Aren", Price: 18000, Stock: 100, Category: productpb.Category_FOOD},
	{Id: "F02", Name: "Croissant Cokelat", Price: 25000, Stock: 20, Category: productpb.Category_FOOD},
	{Id: "C01", Name: "Jaket Hoodie Hitam", Price: 350000, Stock: 12, Category: productpb.Category_FASHION},
}

// 0. Implementasi Unary RPC: ListProducts (Mendukung repeated & enum)
func (s *server) ListProducts(ctx context.Context, req *productpb.ListProductsRequest) (*productpb.ListProductsResponse, error) {
	log.Printf("📋 [Server] Kasir meminta daftar produk (Filter: %v)...", req.GetFilterCategory())

	var hasil []*productpb.Product
	for _, p := range productDB {
		// Jika UNSPECIFIED, ambil semua produk. Jika ada filter, ambil yang cocok.
		if req.GetFilterCategory() == productpb.Category_CATEGORY_UNSPECIFIED || p.Category == req.GetFilterCategory() {
			hasil = append(hasil, p)
		}
	}

	// Lempar event ke RabbitMQ
	publishEvent("CATALOG_VIEW", "Kasir", fmt.Sprintf("Melihat katalog (Filter: %v)", req.GetFilterCategory()))

	// Alirkan event ke Kafka commit log
	publishKafkaLog("CATALOG_VIEW", fmt.Sprintf("Kasir melihat katalog (Filter: %v)", req.GetFilterCategory()))

	return &productpb.ListProductsResponse{
		Products: hasil,
	}, nil
}

// Koneksi Redis Client
var rdb *redis.Client

func initRedis() {
	rdb = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Printf("⚠️  [Redis] Gagal terhubung ke Redis: %v. Berjalan tanpa cache.", err)
	} else {
		log.Println("⚡ [Redis] Berhasil terhubung ke Redis di localhost:6379 ✅")
	}
}

// ----------------------------------------------------
// Metrik Prometheus (Observability)
// ----------------------------------------------------
var (
	grpcRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "grpc_requests_total",
			Help: "Total jumlah pemanggilan RPC yang masuk ke server",
		},
		[]string{"method", "status"},
	)
	grpcRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "grpc_request_duration_seconds",
			Help:    "Durasi pemrosesan RPC dalam satuan detik",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method"},
	)
	cacheHitsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "redis_cache_hits_total",
			Help: "Total cache hit pada Redis",
		},
	)
	cacheMissesTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "redis_cache_misses_total",
			Help: "Total cache miss pada Redis",
		},
	)
	grpcChatMessagesTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "grpc_chat_messages_total",
			Help: "Total pesan chat yang dikirim kasir lewat LiveChat stream",
		},
	)
)

func initMetrics() {
	prometheus.MustRegister(grpcRequestsTotal, grpcRequestDuration, cacheHitsTotal, cacheMissesTotal, grpcChatMessagesTotal)

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("📊 [Prometheus] Endpoint metrik aktif di http://localhost:2112/metrics ✅")
		if err := http.ListenAndServe(":2112", nil); err != nil {
			log.Printf("⚠️  [Prometheus] HTTP metrics error: %v", err)
		}
	}()
}

// Koneksi RabbitMQ Channel
var rmqChannel *amqp.Channel

func initRabbitMQ() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Printf("⚠️  [RabbitMQ] Gagal terhubung ke RabbitMQ: %v. Berjalan tanpa message broker.", err)
		return
	}
	ch, err := conn.Channel()
	if err != nil {
		log.Printf("⚠️  [RabbitMQ] Gagal membuka channel: %v", err)
		return
	}
	_, err = ch.QueueDeclare(
		"event_queue",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Printf("⚠️  [RabbitMQ] Gagal mendeklarasikan antrean: %v", err)
		return
	}
	rmqChannel = ch
	log.Println("🐇 [RabbitMQ] Berhasil terhubung ke RabbitMQ di localhost:5672 ✅")
}

// Fungsi helper melempar event pesan ke antrean RabbitMQ
func publishEvent(event, sender, detail string) {
	if rmqChannel == nil {
		return
	}
	payload, _ := json.Marshal(map[string]string{
		"event":     event,
		"sender":    sender,
		"detail":    detail,
		"timestamp": time.Now().Format("15:04:05"),
	})

	err := rmqChannel.PublishWithContext(
		context.Background(),
		"",            // default exchange
		"event_queue", // nama antrean
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        payload,
		},
	)
	if err != nil {
		log.Printf("⚠️  [RabbitMQ] Gagal melempar event: %v", err)
	} else {
		log.Printf("📢 [RabbitMQ - Producer] Event dilempar ke antrean: [%s] '%s'", event, detail)
	}
}

// Koneksi Apache Kafka Writer
var kafkaWriter *kafka.Writer

func initKafka() {
	kafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "product-activity",
		Balancer: &kafka.LeastBytes{},
	}
	log.Println("📬 [Kafka] Berhasil menginisialisasi Kafka Writer di localhost:9092 ✅")
}

// Fungsi helper mengalirkan event ke Kafka topic
func publishKafkaLog(key, detail string) {
	if kafkaWriter == nil {
		return
	}
	payload, _ := json.Marshal(map[string]string{
		"key":       key,
		"detail":    detail,
		"timestamp": time.Now().Format("2006-01-02 15:04:05"),
	})

	go func() {
		err := kafkaWriter.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(key),
				Value: payload,
			},
		)
		if err != nil {
			log.Printf("⚠️  [Kafka] Gagal menulis ke Kafka stream: %v", err)
		} else {
			log.Printf("📬 [Kafka - Producer] Log dialirkan ke topic 'product-activity' [Key: %s]", key)
		}
	}()
}

// 0. Implementasi Unary RPC: GetProduct dengan Cache-Aside Pattern
func (s *server) GetProduct(ctx context.Context, req *productpb.GetProductRequest) (*productpb.GetProductResponse, error) {
	cacheKey := "product:" + req.GetId()

	// 1. Cek ke Redis Cache dulu (Cache Hit)
	if rdb != nil {
		cachedData, err := rdb.Get(ctx, cacheKey).Result()
		if err == nil {
			var prod productpb.Product
			if err := json.Unmarshal([]byte(cachedData), &prod); err == nil {
				log.Printf("⚡ [Server - Cache HIT] Produk ID '%s' diambil dari REDIS (<2ms)! Hemat query DB.", req.GetId())
				cacheHitsTotal.Inc()
				return &productpb.GetProductResponse{Product: &prod}, nil
			}
		}
	}

	// 2. Cache MISS: Produk belum ada di Redis, ambil dari DB (simulasi lambat 2 detik)
	cacheMissesTotal.Inc()
	log.Printf("🐢 [Server - Cache MISS] Produk ID '%s' belum ada di Redis. Mengambil dari DB (butuh 2 detik)...", req.GetId())

	select {
	case <-time.After(2 * time.Second):
		// Cari produk di dummy database
		var found *productpb.Product
		for _, p := range productDB {
			if p.Id == req.GetId() {
				found = p
				break
			}
		}
		if found == nil {
			found = &productpb.Product{
				Id:       req.GetId(),
				Name:     "Laptop Gaming ROG",
				Price:    25000000,
				Stock:    5,
				Category: productpb.Category_ELECTRONICS,
			}
		}

		// 3. Simpan hasil ke Redis Cache (TTL: 60 detik)
		if rdb != nil {
			if bytes, err := json.Marshal(found); err == nil {
				rdb.Set(context.Background(), cacheKey, string(bytes), 60*time.Second)
				log.Printf("💾 [Server - Cache SAVED] Produk ID '%s' disimpan ke Redis (TTL: 60 detik)", req.GetId())
			}
		}

		// Lempar event ke RabbitMQ
		publishEvent("PRODUCT_SEARCH", "Kasir", fmt.Sprintf("Menemukan produk '%s' (Rp %.0f)", found.GetName(), found.GetPrice()))

		// Alirkan log ke Kafka
		publishKafkaLog("PRODUCT_SEARCH", fmt.Sprintf("ID: %s | Nama: %s | Harga: Rp %.0f", found.GetId(), found.GetName(), found.GetPrice()))

		return &productpb.GetProductResponse{Product: found}, nil

	case <-ctx.Done():
		log.Printf("⚠️  [Server] Operasi dibatalkan! Client kehabisan waktu: %v", ctx.Err())
		return nil, status.Errorf(codes.DeadlineExceeded, "pemrosesan terlalu lama, batas waktu habis (timeout)")
	}
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
		grpcRequestsTotal.WithLabelValues(info.FullMethod, "UNAUTHENTICATED").Inc()
		return err
	}

	log.Printf("✅ [Lobi Interceptor - Stream] Tiket valid! Mempersilakan masuk ke: %s", info.FullMethod)
	grpcRequestsTotal.WithLabelValues(info.FullMethod, "STREAM_CONNECTED").Inc()
	return handler(srv, ss)
}

// 3. Interceptor untuk Unary RPC (seperti GetProduct & ListProducts)
func authUnaryInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	start := time.Now()
	log.Printf("🛡️  [Lobi Interceptor - Unary] Tamu menuju: %s", info.FullMethod)

	if err := authorize(ctx); err != nil {
		log.Printf("❌ [Lobi Interceptor - Unary] Akses ditolak untuk: %s", info.FullMethod)
		grpcRequestsTotal.WithLabelValues(info.FullMethod, "UNAUTHENTICATED").Inc()
		return nil, err
	}

	log.Printf("✅ [Lobi Interceptor - Unary] Tiket valid! Mempersilakan masuk ke: %s", info.FullMethod)
	resp, err := handler(ctx, req)
	duration := time.Since(start).Seconds()

	grpcRequestDuration.WithLabelValues(info.FullMethod).Observe(duration)
	if err != nil {
		grpcRequestsTotal.WithLabelValues(info.FullMethod, "ERROR").Inc()
	} else {
		grpcRequestsTotal.WithLabelValues(info.FullMethod, "OK").Inc()
	}

	return resp, err
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
			// Tambahkan hitungan metrik pesan chat Prometheus
			grpcChatMessagesTotal.Inc()
			// Lempar pesan chat ke RabbitMQ agar dicatat worker
			publishEvent("CHAT_MESSAGE", msg.GetSender(), msg.GetText())
			// Alirkan ke Kafka stream
			publishKafkaLog("CHAT_STREAM", fmt.Sprintf("[%s]: %s", msg.GetSender(), msg.GetText()))
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
	// Inisialisasi koneksi Redis Caching
	initRedis()

	// Inisialisasi koneksi RabbitMQ Message Broker
	initRabbitMQ()

	// Inisialisasi koneksi Apache Kafka Event Stream
	initKafka()

	// Inisialisasi Prometheus Metrics Server
	initMetrics()

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
