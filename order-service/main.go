package main

import (
	"context"
	"fmt"
	"log"
	"time"

	paymentpb "grpc-learning/gen/payment"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// OrderService merepresentasikan backend pemesanan barang
type OrderService struct {
	paymentClient paymentpb.PaymentServiceClient
}

func NewOrderService(paymentConn *grpc.ClientConn) *OrderService {
	return &OrderService{
		// Membuat client stub untuk memanggil gRPC Payment Service
		paymentClient: paymentpb.NewPaymentServiceClient(paymentConn),
	}
}

// CreateOrder adalah alur bisnis ketika ada pembeli membuat pesanan baru
func (s *OrderService) CreateOrder(orderID string, userID string, itemName string, price float64) {
	fmt.Println("-----------------------------------------------------------------")
	log.Printf("[ORDER-SERVICE] 📦 Pesanan baru masuk:")
	log.Printf("   • Order ID : %s", orderID)
	log.Printf("   • User     : %s", userID)
	log.Printf("   • Barang   : %s", itemName)
	log.Printf("   • Harga    : Rp %.2f", price)
	log.Printf("   ⚡ Menghubungi Payment Service via gRPC untuk verifikasi dana...")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Memanggil fungsi ProcessPayment di Backend Payment Service lewat jaringan gRPC!
	paymentResp, err := s.paymentClient.ProcessPayment(ctx, &paymentpb.PaymentRequest{
		OrderId: orderID,
		UserId:  userID,
		Amount:  price,
	})

	if err != nil {
		log.Printf("[ORDER-SERVICE] ❌ Gagal menghubungi Payment Service: %v", err)
		return
	}

	// Mengecek respon dari Payment Service
	if paymentResp.GetSuccess() {
		log.Printf("[ORDER-SERVICE] 🎉 PESANAN DIKONFIRMASI! Pembayaran Lunas.")
		log.Printf("   • Payment ID: %s", paymentResp.GetPaymentId())
		log.Printf("   • Status    : %s", paymentResp.GetMessage())
		log.Printf("   📦 Barang '%s' siap dikemas dan dikirim ke pembeli!", itemName)
	} else {
		log.Printf("[ORDER-SERVICE] ⚠️ PESANAN DIBATALKAN: %s", paymentResp.GetMessage())
	}
}

func main() {
	paymentServiceAddr := "localhost:50051"

	fmt.Println("=========================================================")
	fmt.Println("🛒 BACKEND 2: ORDER SERVICE Berjalan")
	fmt.Println("   Menghubungkan ke Backend 1 (Payment Service) via gRPC...")
	fmt.Println("=========================================================")

	// 1. Hubungkan koneksi internal ke Payment Service
	conn, err := grpc.NewClient(paymentServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Tidak dapat terhubung ke Payment Service: %v", err)
	}
	defer conn.Close()

	orderService := NewOrderService(conn)

	// Simulasi: 2 Transaksi Pembelian Nyata
	// SKENARIO 1: User-1 (Punya saldo Rp 500.000) beli Sepatu Rp 200.000 -> Pembayaran Sukses!
	orderService.CreateOrder("ORD-001", "USER-1", "Sepatu Olahraga Running", 200000.0)

	time.Sleep(2 * time.Second)

	// SKENARIO 2: User-2 (Hanya punya saldo Rp 50.000) beli Laptop Rp 1.500.000 -> Pembayaran Gagal!
	orderService.CreateOrder("ORD-002", "USER-2", "Laptop Gaming Pro", 1500000.0)

	fmt.Println("\n=========================================================")
	fmt.Println("🏁 Semua simulasi pemesanan selesai dijalankan!")
	fmt.Println("=========================================================")
}
