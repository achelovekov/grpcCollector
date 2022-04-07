package main

import (
	"log"
	"net"

	"github.com/achelovekov/grpcCollector/internal/server"
)

func main() {
	port := ":10000"
	log.Printf("Starting listening on port %s", port)
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	log.Printf("Listening on port %s", port)

	srv := server.NewGRPCDialOutSever()
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
