package main

import (
	"fmt"
	"io"
	"log"
	"net"

	"google.golang.org/grpc"

	productpb "grpc-learning/gen/product"
)

type server struct {
	productpb.UnimplementedProductServiceServer
}

func (s *server) LiveChat(stream grpc.BidiStreamingServer[productpb.ChatMessage, productpb.ChatResponse]) error {
    log.Println("Sesi LiveChat dimulai!")

    for {
        // 1. DENGARKAN pesan yang masuk dari Client
        msg, err := stream.Recv()
        if err == io.EOF {
            // io.EOF artinya: Client sudah selesai bicara dan menutup stream
            log.Println("Client menutup obrolan.")
            return nil
        }
        if err != nil {
            return err
        }

        log.Printf("[Gudang Menerima] Dari '%s': %s", msg.GetSender(), msg.GetText())

        // 2. BALAS langsung ke Client lewat stream yang sama
        balasan := fmt.Sprintf("Halo %s, gudang menerima pesanmu: '%s'", msg.GetSender(), msg.GetText())
        err = stream.Send(&productpb.ChatResponse{Reply: balasan})
        if err != nil {
            return err
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
