package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	pb "grpc-learning/gen/product"
	"io"

	"log"
	"net"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
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

// Variabel Client Redis Global
var rdb *redis.Client

// Inisialisasi koneksi ke Redis Server
func initRedis() {
	rdb = redis.NewClient(&redis.Options{
		Addr: "localhost:6379", // Alamat port default Redis
	})

	// Uji koneksi dengan PING (batas waktu 2 detik)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Printf("⚠️  [Redis] Gagal terhubung ke Redis: %v. Server tetap berjalan tanpa cache.", err)
	} else {
		log.Println("⚡ [Redis] Berhasil terhubung ke Redis di localhost:6379 ✅")
	}
}

// Variabel Saluran RabbitMQ Global
var rmqChannel *amqp.Channel

// Inisialisasi koneksi ke RabbitMQ
func initRabbitMQ() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Printf("⚠️  [RabbitMQ] Gagal terhubung ke RabbitMQ: %v. Berjalan tanpa message queue.", err)
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
	log.Println("🐇 [RabbitMQ] Berhasil terhubung ke RabbitMQ di localhost:5672 ✅")
}

// Variabel Kafka Writer Global
var kafkaWriter *kafka.Writer

// Inisialisasi koneksi Kafka Producer
func initKafka() {
	kafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"), // Alamat external port Kafka
		Topic:    "product-activity",          // Nama Topic buku besar transaksi
		Balancer: &kafka.LeastBytes{},
	}
	log.Println("📬 [Kafka] Berhasil menginisialisasi Kafka Writer di localhost:9092 ✅")
}

// Fungsi Helper: Mengalirkan catatan event ke Kafka Commit Log
func publishKafkaLog(key, detail string) {
	if kafkaWriter == nil {
		return
	}

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
func publishInvoiceTask(product *pb.Product) {
	if rmqChannel == nil {
		return
	}

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

// Implementasi Unary RPC: GetProduct dengan Cache-Aside Pattern
func (s *server) GetProduct(ctx context.Context, req *pb.GetProductRequest) (*pb.GetProductResponse, error) {
	log.Printf("📥 [Server] Kasir mencari produk dengan ID: '%s'", req.GetId())
	cacheKey := "product:" + req.GetId()

	// 1. LANGKAH PERTAMA: Cek ke Redis Cache dulu (Meja Cepat)
	if rdb != nil {
		cachedData, err := rdb.Get(ctx, cacheKey).Result()
		if err == nil {
			var prod pb.Product
			// Bongkar JSON dari Redis menjadi struct Product Go
			if err := json.Unmarshal([]byte(cachedData), &prod); err == nil {
				log.Printf("⚡ [Server - Cache HIT] Produk '%s' diambil dari REDIS (<1ms)! Hemat beban DB.", req.GetId())
				return &pb.GetProductResponse{Product: &prod}, nil
			}
		}
	}

	// 2. LANGKAH KEDUA (Cache MISS): Produk belum ada di Redis, ambil dari DB
	log.Printf("🐢 [Server - Cache MISS] Produk '%s' belum ada di Redis! Mengambil dari DB (simulasi lambat 2 detik)...", req.GetId())
	time.Sleep(2 * time.Second) // Simulasi lambat membaca disk database

	// Cari produk di database memori
	product, found := productDB[req.GetId()]
	if !found {
		log.Printf("⚠️  [Server] Produk ID '%s' tidak ditemukan di DB!", req.GetId())
		return nil, status.Errorf(codes.NotFound, "produk dengan ID '%s' tidak ditemukan di gudang", req.GetId())
	}

	// 3. LANGKAH KETIGA: Simpan fotokopi hasil DB ke Redis (TTL: 60 detik)
	if rdb != nil {
		if bytes, err := json.Marshal(product); err == nil {
			rdb.Set(context.Background(), cacheKey, string(bytes), 60*time.Second)
			log.Printf("💾 [Server - Cache SAVED] Produk '%s' disimpan ke Redis (TTL: 60 detik)", req.GetId())
		}
	}

		// Lempar tugas pembuatan invoice ke antrean RabbitMQ (Asinkron)
	publishInvoiceTask(product)
	publishKafkaLog("TRANSAKSI_PRODUK", fmt.Sprintf("ID: %s | Nama: %s | Harga: Rp %.0f", product.GetId(), product.GetName(), product.GetPrice()))
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

// Implementasi Bidirectional Streaming RPC: LiveChat
func (s *server) LiveChat(stream grpc.BidiStreamingServer[pb.ChatMessage, pb.ChatResponse]) error {
	fmt.Println("\n==================================================")
	fmt.Println("💬 [Server] SESI CHAT GUDANG DENGAN KASIR DIMULAI!")
	fmt.Println("   Ketik balasan Anda lalu tekan [ENTER]")
	fmt.Println("==================================================")

	done := make(chan struct{})

	// 1. Goroutine Background: Terus mendengarkan pesan masuk dari Kasir
	go func() {
		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				fmt.Println("\n🚪 [Server] Kasir telah mengakhiri obrolan.")
				close(done)
				return
			}
			if err != nil {
				log.Printf("⚠️  [Server] Error menerima pesan kasir: %v", err)
				close(done)
				return
			}

			// Cetak pesan kasir di layar Server
			fmt.Printf("\n📩 [%s]: %s\nAnda (Gudang) > ", msg.GetSender(), msg.GetText())
		}
	}()

	// 2. Loop Utama: Membaca ketikan keyboard operator Gudang di terminal
	scanner := bufio.NewScanner(os.Stdin)
	for {
		select {
		case <-done:
			return nil
		default:
			fmt.Print("Anda (Gudang) > ")
			if !scanner.Scan() {
				return scanner.Err()
			}

			balasan := scanner.Text()
			if balasan == "" {
				continue
			}

			// Kirim balasan gudang ke Kasir lewat stream
			err := stream.Send(&pb.ChatResponse{
				Reply: balasan,
			})
			if err != nil {
				return err
			}
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
		log.Printf("⛔ [Satpam Unary] Mencegat dan mengusir tamu dari: %s", info.FullMethod)
		return nil, err
	}

	log.Printf("🟢 [Satpam Unary] Tiket valid! Mempersilakan tamu masuk ke: %s", info.FullMethod)
	// Persilakan tamu masuk ke fungsi tujuan aslinya
	return handler(ctx, req)
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
	// 0. Hubungkan ke Redis In-Memory Cache
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
