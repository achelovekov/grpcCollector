package main

import (
	"fmt"

	server "github.com/achelovekov/grpcCollector/internal/server"
)

func main() {
	modelInflux := server.ParseModel("bgp-model.json")
	r := server.GenerateFilterFromModel(&modelInflux)

	fmt.Println(r)
}
