package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	paymentpb "grpc-learning/gen/payment"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// paymentServer merepresentasikan backend Payment Service
type paymentServer struct {
	paymentpb.UnimplementedPaymentServiceServer

	mu       sync.Mutex
	balances map[string]float64 // Database saldo user
	counter  int
}

func newPaymentServer() *paymentServer {
	return &paymentServer{
		balances: map[string]float64{
			"USER-1": 500000.0, // User 1 punya saldo Rp 500.000
			"USER-2": 50000.0,  // User 2 punya saldo Rp 50.000 (saldo pas-pasan)
		},
	}
}

// ProcessPayment adalah fungsi gRPC yang bisa dipanggil oleh Backend lain (Order Service)
func (s *paymentServer) ProcessPayment(ctx context.Context, req *paymentpb.PaymentRequest) (*paymentpb.PaymentResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	userID := req.GetUserId()
	orderID := req.GetOrderId()
	amount := req.GetAmount()

	log.Printf("[PAYMENT-SERVICE] 🔔 Menerima permintaan bayar dari Order Service:")
	log.Printf("   • Order ID : %s", orderID)
	log.Printf("   • User ID  : %s", userID)
	log.Printf("   • Nominal  : Rp %.2f", amount)

	// Validasi input
	if amount <= 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Nominal pembayaran harus lebih dari 0")
	}

	// Cek apakah user terdaftar di payment service
	currentBalance, exists := s.balances[userID]
	if !exists {
		return nil, status.Errorf(codes.NotFound, "User '%s' tidak terdaftar di sistem pembayaran", userID)
	}

	// Cek apakah saldo user mencukupi
	if currentBalance < amount {
		log.Printf("   ❌ Saldo tidak cukup! Saldo saat ini: Rp %.2f, Tagihan: Rp %.2f", currentBalance, amount)
		return &paymentpb.PaymentResponse{
			PaymentId: "",
			Success:   false,
			Message:   fmt.Sprintf("Gagal: Saldo tidak mencukupi (Saldo: Rp %.2f, Tagihan: Rp %.2f)", currentBalance, amount),
		}, nil
	}

	// Potong saldo
	s.balances[userID] -= amount
	s.counter++
	paymentID := fmt.Sprintf("PAY-%d-%d", time.Now().Unix(), s.counter)

	log.Printf("   ✅ Pembayaran BERHASIL! Payment ID: %s. Sisa Saldo: Rp %.2f", paymentID, s.balances[userID])

	return &paymentpb.PaymentResponse{
		PaymentId: paymentID,
		Success:   true,
		Message:   "Pembayaran berhasil diproses.",
	}, nil
}

func main() {
	port := ":50051"
	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Gagal membuka port %s: %v", port, err)
	}

	grpcServer := grpc.NewServer()
	paymentpb.RegisterPaymentServiceServer(grpcServer, newPaymentServer())

	fmt.Println("=========================================================")
	fmt.Printf("🏦 BACKEND 1: PAYMENT SERVICE berjalan di port %s\n", port)
	fmt.Println("   Siap menerima panggilan gRPC dari backend lain...")
	fmt.Println("=========================================================")

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Payment service berhenti: %v", err)
	}
}
