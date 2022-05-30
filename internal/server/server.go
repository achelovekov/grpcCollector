package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"encoding/json"

	//dialout "github.com/CiscoSE/grpc/proto/mdt_dialout"
	// "encoding/json"

	dialout "github.com/achelovekov/grpcCollector/proto/mdt_dialout"
	telemetry "github.com/achelovekov/grpcCollector/proto/telemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/proto"
)

type DialOutServer struct {
	cancel context.CancelFunc
	ctx    context.Context
}

func (c *DialOutServer) MdtDialout(stream dialout.GRPCMdtDialout_MdtDialoutServer) error {

	peer, peerOK := peer.FromContext(stream.Context())
	if peerOK {
		log.Printf("Accepted Cisco MDT GRPC dialout connection from %s", peer.Addr)
	}

	for {
		packet, err := stream.Recv()
		if err != nil {
			if err != io.EOF && c.ctx.Err() == nil {
				fmt.Printf("E! GRPC dialout receive error: %v", err)
			}
			break
		}

		if len(packet.Data) == 0 && len(packet.Errors) != 0 {
			log.Printf("no more data")
			break
		}

		c.handleTelemetry(packet.Data)
	}

	if peerOK {
		log.Printf("Closed Cisco MDT GRPC dialout connection from %s", peer.Addr)
	}

	return nil
}

type Model struct {
	Name   string `json:"name"`
	Nested []*Model `json:"nested"`
}

func (c *DialOutServer) handleTelemetry(data []byte) {

	telemetryData := &telemetry.Telemetry{}
	err := proto.Unmarshal(data, telemetryData)
	if err != nil {
		log.Printf("Error: %s", err.Error())
		return
	}

	model := Model{Name: telemetryData.EncodingPath}

	destructureTelemetry(telemetryData, &model)
	//flattenTelemetry(telemetryData)

	//PrintModel(&model,  0)
	b, err := json.MarshalIndent(model, "", "  ")
    if err != nil {
        fmt.Println(err)
    }
    fmt.Print(string(b))
	//b, err := json.Marshal(telemetryData)

	// if err != nil {
	// 	fmt.Println(err)
	// }

	// fmt.Println(string(b))
}

func printMap(m map[string]interface{}) {
	for k, v := range m {
        fmt.Println(k, ":", v)
    }
}

func flatten(telemetryData *telemetry.TelemetryField, prefix []string, m map[string]interface{}) {
	if len(telemetryData.Fields) > 0 {
		if (len(telemetryData.Name) > 0 && telemetryData.Name != "keys" && telemetryData.Name != "content") {
			prefix = append(prefix, telemetryData.Name)
		}

		for _, item := range telemetryData.Fields {
			flatten(item, prefix, m)
		}
	} else {
		if (len(telemetryData.Name) > 0 && telemetryData.Name != "keys" && telemetryData.Name != "content") {
			fullPath := append(prefix, telemetryData.Name)
			i := telemetryData.GetValueByType()
			switch i.(type) {
			case *telemetry.TelemetryField_StringValue:
				m[strings.Join(fullPath,".")] = telemetryData.GetStringValue()
			case *telemetry.TelemetryField_Uint64Value:
				m[strings.Join(fullPath,".")] = telemetryData.GetUint64Value()
			}
		}
	}
}

func flattenTelemetry(telemetryData *telemetry.Telemetry) {
	var prefix []string
	if len(telemetryData.DataGpbkv) > 0 {
		for _, item := range telemetryData.DataGpbkv {
			m := make(map[string]interface{})
			flatten(item, prefix, m)
			printMap(m)
			fmt.Println("----------------------------")
		}
	}
}

func contains(sli []*Model, elem *Model) bool {
    for _, item := range sli {
        if elem.Name == item.Name {
            return true
        }
    }
    return false
}

func destructureTelemetry(telemetryData *telemetry.Telemetry, model *Model) {
	if len(telemetryData.DataGpbkv) > 0 {
		destructureFields(telemetryData.DataGpbkv[0].Fields, model)
	}
}

func destructureFields(fields []*telemetry.TelemetryField, model *Model) {
	for _, field := range fields {
		if (field.Name == "keys" && len(field.Fields) > 0) {
			for _, field := range field.Fields {
				fmt.Println("key field: ", field.Name)
				newModel := &Model{Name: field.Name}
				if !contains(model.Nested, newModel) {
					model.Nested = append(model.Nested, newModel)
				}
			}
		}
		if field.Name == "content" {
			for _, field := range field.Fields {
				fmt.Println("content field: ", field.Name)
				newModel := &Model{Name: field.Name}
				if !contains(model.Nested, newModel) {
					model.Nested = append(model.Nested, newModel)
				}
				if len(field.Fields) > 0 {
					fmt.Println("go deep with: ", field.Name)
					destructureFields(field.Fields, newModel)
				}
			}
		} else if (field.Name != "keys" && field.Name != "content") {
			fmt.Println("nested field: ", field.Name)
			newModel := &Model{Name: field.Name}
			if !contains(model.Nested, newModel) {
				model.Nested = append(model.Nested, newModel)
			}
			if len(field.Fields) > 0 {
				fmt.Println("go deep with: ", field.Name)
				destructureFields(field.Fields, newModel)
			}
		}
	}
}

func PrintModel(model *Model, tab int) {
	fmt.Println(strings.Repeat(" ", tab) + model.Name)
	if len(model.Nested) > 0 {
		tab += 2
		for _, item := range model.Nested {
			PrintModel(item, tab)
		}
	}
}

func NewGRPCDialOutSever() *grpc.Server {
	c := &DialOutServer{}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	dialout.RegisterGRPCMdtDialoutServer(grpcServer, c)

	return grpcServer
}
