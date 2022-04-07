package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"

	//dialout "github.com/CiscoSE/grpc/proto/mdt_dialout"
	dialout "github.com/achelovekov/grpcCollector/proto/mdt_dialout"
	"google.golang.org/grpc"
)

type DialOutServer struct {
	cancel context.CancelFunc
	ctx    context.Context
}

var port = flag.Int("port", 10000, "The server port")
var producerFilter = flag.String("producer-address", "0.0.0.0", "The producer ip address")

func (c *DialOutServer) MdtDialout(stream dialout.GRPCMdtDialout_MdtDialoutServer) error {
	// Validate the context
	fmt.Printf("executed")

	return nil
}

func main() {
	// Read params
	flag.Parse()

	// Create new server struct
	c := &DialOutServer{}

	// Add context
	c.ctx, c.cancel = context.WithCancel(context.Background())

	// Configure protocol and ports
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *port))
	log.Printf("Listening in port: %d - Accepting connections from %v", *port, *producerFilter)
	if err != nil {
		log.Fatalf("Failed to listen in configured port: %v", err)
	}

	// Create empty option
	var opts []grpc.ServerOption

	// Create new gRPC server
	grpcServer := grpc.NewServer(opts...)

	// Register server to gRPC calls
	dialout.RegisterGRPCMdtDialoutServer(grpcServer, c)

	// Start server
	grpcServer.Serve(lis)
}
