package main

import (
	"context"
	"encoding/json"
	"fmt"
	pb "grpc-learning/gen/product"
	"grpc-learning/pkg/tracer"
	"io"

	"log"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Dummy Database Produk di Memori Server
var productDB = map[string]*pb.Product{
	"E01": {
		Id:       "E01",
		Name:     "Laptop Gaming ROG",
		Price:    25000000,
		Stock:    5,
		Category: pb.Category_ELECTRONICS,
	},
	"E02": {
		Id:       "E02",
		Name:     "Mechanical Keyboard RGB",
		Price:    1200000,
		Stock:    15,
		Category: pb.Category_ELECTRONICS,
	},
	"F01": {
		Id:       "F01",
		Name:     "Kopi Susu Gula Aren",
		Price:    18000,
		Stock:    100,
		Category: pb.Category_FOOD,
	},
}

// ----------------------------------------------------
// Sensor Metrik Prometheus (Observability)
// ----------------------------------------------------
var (
	// Sensor 1: Menghitung total panggilan RPC gRPC (dengan label method & status)
	grpcRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "grpc_requests_total",
			Help: "Total jumlah pemanggilan RPC yang masuk ke server",
		},
		[]string{"method", "status"},
	)

	// Sensor 2: Menghitung efektivitas Redis Cache Hit
	cacheHitsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "redis_cache_hits_total",
			Help: "Total cache hit pada Redis",
		},
	)

	// Sensor 3: Menghitung saat Redis Cache Miss (butuh baca DB)
	cacheMissesTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "redis_cache_misses_total",
			Help: "Total cache miss pada Redis",
		},
	)

	// Sensor 4: Menghitung setiap frame pesan chat yang diketik kasir
	grpcChatMessagesTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "grpc_chat_messages_total",
			Help: "Total pesan chat yang dikirim kasir lewat LiveChat stream",
		},
	)
)

// Inisialisasi dan hidupkan endpoint HTTP :2112/metrics untuk diserap Prometheus
func initMetrics() {
	prometheus.MustRegister(grpcRequestsTotal, cacheHitsTotal, cacheMissesTotal, grpcChatMessagesTotal)

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("📊 [Prometheus] Endpoint metrik aktif di http://localhost:2112/metrics ✅")
		if err := http.ListenAndServe(":2112", nil); err != nil {
			log.Printf("⚠️  [Prometheus] HTTP metrics error: %v", err)
		}
	}()
}


// Variabel Client Redis Global
var rdb *redis.Client

// Inisialisasi koneksi ke Redis Server
func initRedis() {
	redisHost := os.Getenv("REDIS_HOST")
	if redisHost == "" {
		redisHost = "localhost:6379"
	}

	rdb = redis.NewClient(&redis.Options{
		Addr: redisHost,
	})

	// Uji koneksi dengan PING (batas waktu 2 detik)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Printf("⚠️  [Redis] Gagal terhubung ke Redis di %s: %v. Server tetap berjalan tanpa cache.", redisHost, err)
	} else {
		log.Printf("⚡ [Redis] Berhasil terhubung ke Redis di %s ✅\n", redisHost)
	}
}


// Variabel Saluran RabbitMQ Global
var rmqChannel *amqp.Channel

// Inisialisasi koneksi ke RabbitMQ
func initRabbitMQ() {
	rmqURL := os.Getenv("RABBITMQ_URL")
	if rmqURL == "" {
		rmqURL = "amqp://guest:guest@localhost:5672/"
	}

	conn, err := amqp.Dial(rmqURL)
	if err != nil {
		log.Printf("⚠️  [RabbitMQ] Gagal terhubung ke RabbitMQ di %s: %v. Berjalan tanpa message queue.", rmqURL, err)
		return
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Printf("⚠️  [RabbitMQ] Gagal membuka channel: %v", err)
		return
	}

	// Buat/pastikan antrean bernama 'order_queue' sudah ada
	_, err = ch.QueueDeclare(
		"order_queue", // Nama antrean
		true,          // Durable (pesan tidak hilang jika RabbitMQ restart)
		false,         // Auto-delete
		false,         // Exclusive
		false,         // No-wait
		nil,           // Arguments
	)
	if err != nil {
		log.Printf("⚠️  [RabbitMQ] Gagal deklarasi antrean: %v", err)
		return
	}

	rmqChannel = ch
	log.Printf("🐇 [RabbitMQ] Berhasil terhubung ke RabbitMQ di %s ✅\n", rmqURL)
}


// Variabel Kafka Writer Global
var kafkaWriter *kafka.Writer

// Inisialisasi koneksi Kafka Producer
func initKafka() {
	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	kafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    "product-activity", // Nama Topic buku besar transaksi
		Balancer: &kafka.LeastBytes{},
	}
	log.Printf("📬 [Kafka] Berhasil menginisialisasi Kafka Writer di %s ✅\n", kafkaBroker)
}


// Fungsi Helper: Mengalirkan catatan event ke Kafka Commit Log
func publishKafkaLog(ctx context.Context, key, detail string) {
	if kafkaWriter == nil {
		return
	}

	tr := otel.Tracer("grpc-server")
	_, span := tr.Start(ctx, "Kafka Publish Log")
	defer span.End()

	payload, _ := json.Marshal(map[string]string{
		"action":    key,
		"detail":    detail,
		"timestamp": time.Now().Format("2006-01-02 15:04:05"),
	})

	// Jalankan di Goroutine agar kasir tidak terhambat (Non-blocking)
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



// Fungsi Helper: Melempar tugas cetak invoice ke antrean RabbitMQ
func publishInvoiceTask(ctx context.Context, product *pb.Product) {
	if rmqChannel == nil {
		return
	}

	tr := otel.Tracer("grpc-server")
	_, span := tr.Start(ctx, "RabbitMQ Publish Invoice")
	defer span.End()

	// Bungkus data tugas dalam bentuk JSON
	payload, _ := json.Marshal(map[string]any{
		"event":        "CETAK_INVOICE",
		"product_id":   product.GetId(),
		"product_name": product.GetName(),
		"price":        product.GetPrice(),
		"created_at":   time.Now().Format("15:04:05"),
	})

	// Lempar pesan ke antrean 'order_queue'
	err := rmqChannel.PublishWithContext(
		context.Background(),
		"",            // Exchange default
		"order_queue", // Nama antrean tujuan
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        payload,
		},
	)
	if err != nil {
		log.Printf("⚠️  [RabbitMQ] Gagal melempar tugas invoice: %v", err)
	} else {
		log.Printf("📢 [RabbitMQ - Producer] Tugas invoice '%s' berhasil dilempar ke antrean!", product.GetName())
	}
}



// Struct Server yang Mengimplementasikan Service gRPC
type server struct {
	pb.UnimplementedProductServiceServer
}

// Implementasi Unary RPC: GetProduct dengan Cache-Aside Pattern & OpenTelemetry Tracing
func (s *server) GetProduct(ctx context.Context, req *pb.GetProductRequest) (*pb.GetProductResponse, error) {
	tr := otel.Tracer("grpc-server")
	log.Printf("📥 [Server] Kasir mencari produk dengan ID: '%s'", req.GetId())
	cacheKey := "product:" + req.GetId()

	// 1. LANGKAH PERTAMA: Cek ke Redis Cache dulu (Meja Cepat)
	if rdb != nil {
		redisCtx, redisSpan := tr.Start(ctx, "Redis Cache Lookup")
		cachedData, err := rdb.Get(redisCtx, cacheKey).Result()
		redisSpan.End()

		if err == nil {
			var prod pb.Product
			// Bongkar JSON dari Redis menjadi struct Product Go
			if err := json.Unmarshal([]byte(cachedData), &prod); err == nil {
				log.Printf("⚡ [Server - Cache HIT] ...")
				cacheHitsTotal.Inc() // Pemicu Sensor Cache Hit (+1)

				return &pb.GetProductResponse{Product: &prod}, nil
			}
		}
	}

	// 2. LANGKAH KEDUA (Cache MISS): Produk belum ada di Redis, ambil dari DB
	log.Printf("🐢 [Server - Cache MISS] ...")
	cacheMissesTotal.Inc() // Pemicu Sensor Cache Miss (+1)

	_, dbSpan := tr.Start(ctx, "Database Disk Query (Simulated)")
	time.Sleep(2 * time.Second) // Simulasi lambat membaca disk database
	product, found := productDB[req.GetId()]
	dbSpan.End()

	if !found {
		log.Printf("⚠️  [Server] Produk ID '%s' tidak ditemukan di DB!", req.GetId())
		return nil, status.Errorf(codes.NotFound, "produk dengan ID '%s' tidak ditemukan di gudang", req.GetId())
	}

	// 3. LANGKAH KETIGA: Simpan fotokopi hasil DB ke Redis (TTL: 60 detik)
	if rdb != nil {
		if bytes, err := json.Marshal(product); err == nil {
			saveCtx, saveSpan := tr.Start(ctx, "Redis Cache Save")
			_ = rdb.Set(saveCtx, cacheKey, string(bytes), 60*time.Second).Err()
			saveSpan.End()
			log.Printf("💾 [Server - Cache SAVED] Produk '%s' disimpan ke Redis (TTL: 60 detik)", req.GetId())
		}
	}

	// Lempar tugas pembuatan invoice ke antrean RabbitMQ (Asinkron)
	publishInvoiceTask(ctx, product)
	publishKafkaLog(ctx, "TRANSAKSI_PRODUK", fmt.Sprintf("ID: %s | Nama: %s | Harga: Rp %.0f", product.GetId(), product.GetName(), product.GetPrice()))
	log.Printf("✅ [Server] Produk '%s' berhasil dikembalikan dari DB", product.GetName())
	return &pb.GetProductResponse{
		Product: product,
	}, nil
}

// Implementasi Unary RPC: ListProducts (Mendukung repeated & filter enum)
func (s *server) ListProducts(ctx context.Context, req *pb.ListProductsRequest) (*pb.ListProductsResponse, error) {
	log.Printf("📋 [Server] Kasir meminta daftar katalog produk (Filter Kategori: %v)...", req.GetFilterCategory())

	var hasil []*pb.Product

	// 1. Lakukan perulangan (looping) pada seluruh database produk kita
	for _, product := range productDB {
		// Jika kasir memilih CATEGORY_UNSPECIFIED (0), artinya tampilkan SEMUA produk tanpa filter
		// Jika kasir memilih kategori tertentu, hanya masukkan produk yang kategorinya cocok
		if req.GetFilterCategory() == pb.Category_CATEGORY_UNSPECIFIED || product.GetCategory() == req.GetFilterCategory() {
			hasil = append(hasil, product)
		}
	}

	log.Printf("✅ [Server] Mengembalikan %d barang ke kasir", len(hasil))

	// 2. Kembalikan slice 'hasil' ke dalam field 'Products' (repeated)
	return &pb.ListProductsResponse{
		Products: hasil,
	}, nil
}

// Implementasi Bidirectional Streaming RPC: LiveChat (Auto-Responder Bot Gudang)
func (s *server) LiveChat(stream grpc.BidiStreamingServer[pb.ChatMessage, pb.ChatResponse]) error {
	log.Println("💬 [Server] Sesi LiveChat dibuka oleh Kasir!")

	for {
		// 1. Terima pesan dari Kasir secara real-time
		msg, err := stream.Recv()
		if err == io.EOF {
			log.Println("🚪 [Server] Kasir mengakhiri sesi obrolan.")
			return nil
		}
		if err != nil {
			log.Printf("⚠️  [Server] Error menerima pesan kasir: %v", err)
			return err
		}

		log.Printf("📩 [Server] Pesan masuk dari [%s]: '%s'", msg.GetSender(), msg.GetText())
		grpcChatMessagesTotal.Inc() // Pemicu Sensor Metrik Chat Masuk (+1)

		// 2. Buat balasan pintar dari Bot Gudang Pusat
		balasan := fmt.Sprintf("🤖 [Bot Gudang]: Halo %s! Pesanmu '%s' sudah dicatat di sistem gudang pusat.", msg.GetSender(), msg.GetText())

		// 3. Kirim balasan langsung kembali ke Kasir lewat stream gRPC
		if err := stream.Send(&pb.ChatResponse{Reply: balasan}); err != nil {
			log.Printf("⚠️  [Server] Gagal membalas chat: %v", err)
			return err
		}
	}
}


// 1. Fungsi Mandiri: Memeriksa Kartu Akses / Token (Satpam Pusat)
func authorize(ctx context.Context) error {
	// Ekstrak metadata dari context yang dikirim tamu
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Errorf(codes.Unauthenticated, "❌ Satpam: Akses ditolak! Tamu tidak membawa kartu identitas (metadata)")
	}

	// Periksa header 'authorization'
	tokens := md.Get("authorization")
	if len(tokens) == 0 || tokens[0] != "token-rahasia-123" {
		return status.Errorf(codes.Unauthenticated, "❌ Satpam: Akses ditolak! Kartu identitas (token) palsu atau tidak berlaku")
	}

	return nil // Token sah!
}

// 2. Satpam untuk Unary RPC (GetProduct & ListProducts)
func authUnaryInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	log.Printf("👮‍♂️ [Satpam Unary] Tamu datang hendak menuju ruangan: %s", info.FullMethod)

	// Periksa identitas di lobi
	if err := authorize(ctx); err != nil {
    grpcRequestsTotal.WithLabelValues(info.FullMethod, "UNAUTHENTICATED").Inc()
    return nil, err
}

	resp, err := handler(ctx, req)
if err != nil {
    grpcRequestsTotal.WithLabelValues(info.FullMethod, "ERROR").Inc()
} else {
    grpcRequestsTotal.WithLabelValues(info.FullMethod, "OK").Inc()
}
return resp, err

}

// 3. Satpam untuk Streaming RPC (LiveChat)
func authStreamInterceptor(
	srv any,
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	log.Printf("👮‍♂️ [Satpam Stream] Tamu datang hendak membuka sambungan live: %s", info.FullMethod)

	// Periksa identitas di lobi
	if err := authorize(ss.Context()); err != nil {
		log.Printf("⛔ [Satpam Stream] Mencegat dan menolak streaming: %s", info.FullMethod)
		return err
	}

	log.Printf("🟢 [Satpam Stream] Tiket valid! Mempersilakan streaming: %s", info.FullMethod)
	return handler(srv, ss)
}



func main() {

	
	// 0. Hubungkan ke Jaeger Distributed Tracing
	tp, err := tracer.InitTracer(context.Background(), "grpc-server")
	if err != nil {
		log.Printf("⚠️  [Tracer] Gagal inisialisasi OpenTelemetry: %v", err)
	} else {
		defer func() {
			if err := tp.Shutdown(context.Background()); err != nil {
				log.Printf("⚠️  [Tracer] Gagal shutdown TracerProvider: %v", err)
			}
		}()
	}

	initMetrics()
	initRedis()
	initRabbitMQ()
	initKafka()

	// 1. Buka socket TCP di port 50051
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("❌ Gagal membuka port 50051: %v", err)
	}

	// Pasang kedua satpam di pintu masuk server gRPC
	grpcServer := grpc.NewServer(
	grpc.StatsHandler(otelgrpc.NewServerHandler()),
    grpc.UnaryInterceptor(authUnaryInterceptor),
    grpc.StreamInterceptor(authStreamInterceptor),
)

	// 3. Daftarkan service produk kita ke server gRPC
	pb.RegisterProductServiceServer(grpcServer, &server{})

	log.Printf("🚀 [Server] gRPC Server aktif berjalan di port %v ...", lis.Addr())

	// 4. Mulai layani permintaan yang masuk
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("❌ Gagal menjalankan server: %v", err)
	}
}
