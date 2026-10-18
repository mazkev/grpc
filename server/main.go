package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"

	productpb "grpc-learning/gen/product"
)

type server struct {
	productpb.UnimplementedProductServiceServer
}

func (s *server) LiveChat(stream grpc.BidiStreamingServer[productpb.ChatMessage, productpb.ChatResponse]) error {
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
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	productpb.RegisterProductServiceServer(s, &server{})

	log.Printf("Server running on port %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
