package main

import (
	"fmt"

	server "github.com/achelovekov/grpcCollector/internal/server"
)

func main() {
	model := server.ParseModel("bgp-model.json")
	r := server.GenerateFilterFromModel(&model)

	fmt.Println(r)
}
