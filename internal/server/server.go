package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"encoding/json"
	"os"
	"io/ioutil"
	//"time"
	"sort"
	//"reflect"

	//dialout "github.com/CiscoSE/grpc/proto/mdt_dialout"
	// "encoding/json"

	dialout "github.com/achelovekov/grpcCollector/proto/mdt_dialout"
	telemetry "github.com/achelovekov/grpcCollector/proto/telemetry"
	"github.com/influxdata/line-protocol/v2/lineprotocol"
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
	IsTag bool `json:"isTag"`
	IsField bool `json:"isField`
}

func (model *Model) String() string {
    return fmt.Sprintf("%v: isTag: %v isField: %v", model.Name, model.IsTag, model.IsField)
}

func ParseModel(filename string) Model {
	// Open jsonFile
	jsonFile, err := os.Open(filename)
	// if we os.Open returns an error then handle it
	if err != nil {
		fmt.Println(err)
		return Model{}
	}
	fmt.Println("Successfully Opened:", filename)
	// defer the closing of our jsonFile so that we can parse it later on
	defer jsonFile.Close()
	byteValue, _ := ioutil.ReadAll(jsonFile)

	var model Model

	// we unmarshal our byteArray which contains our
	// jsonFile's content into 'users' which we defined above
	json.Unmarshal(byteValue, &model)

	return model
}

func GenerateFilterFromModel(model *Model) ([]*Model, []*Model) {
	tags := []*Model{}
	fields := []*Model{}

	prefix := []string{}

	for _, item := range model.Nested {
		FilterFlatten(item, &tags, &fields, prefix)
	}

	sort.SliceStable(tags, func(i, j int) bool {
		return tags[i].Name < tags[j].Name
	  })

	sort.SliceStable(fields, func(i, j int) bool {
		return fields[i].Name < fields[j].Name
	  })

	return tags, fields
}

func FilterFlatten(model *Model, tags *[]*Model, fields *[]*Model, prefix []string) {
	if len(model.Nested) > 0 {
		prefix = append(prefix, model.Name)
		for _, item := range model.Nested {
			FilterFlatten(item, tags, fields, prefix)
		}
	} else {
		prefix = append(prefix, model.Name)
		name := strings.Join(prefix,".")
		newModel := new(Model)
		newModel.Name = name
		if model.IsTag {
			newModel.IsTag = model.IsTag
			*tags = append(*tags, newModel)
		}
		if model.IsField {
			newModel.IsField = model.IsField
			*fields = append(*fields, newModel)
		}
	}
}

func (c *DialOutServer) handleTelemetry(data []byte) {

	telemetryData := &telemetry.Telemetry{}
	err := proto.Unmarshal(data, telemetryData)
	if err != nil {
		log.Printf("Error: %s", err.Error())
		return
	}

	//model := Model{Name: telemetryData.EncodingPath}

	//destructureTelemetry(telemetryData, &model)
	flattenTelemetry(telemetryData)

	//PrintModel(&model,  0)
	// b, err := json.MarshalIndent(model, "", "  ")
    // if err != nil {
    //     fmt.Println(err)
    // }
    // fmt.Print(string(b))
	//b, err := json.Marshal(telemetryData)

	// if err != nil {
	// 	fmt.Println(err)
	// }

	// fmt.Println(string(b))
	//NEW ENTRY!!!
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

func containsString(s []string, v string) bool {
	for _, item := range s {
        if item == v {
            return true
        }
    }
    return false
}

func PrepareLine(measurement string, m map[string]interface{}, tags []*Model, fields []*Model, enc lineprotocol.Encoder) lineprotocol.Encoder {
	enc.SetPrecision(lineprotocol.Microsecond)
	enc.StartLine(measurement)
	for _, item := range tags {
		val, ok := m[item.Name]
		if ok {
			enc.AddTag(item.Name, val.(string))
		}
	}

	for _, item := range fields {
		val, ok := m[item.Name]
		if ok {
			enc.AddField(item.Name, lineprotocol.MustNewValue(val))
		}
	}

	fmt.Println(len(enc.Bytes()))
	//enc.EndLine(time.Time{})
	if err := enc.Err(); err != nil {
		panic(fmt.Errorf("encoding error: %v", err))
	}
	fmt.Printf("--------->%s", enc.Bytes())

	return enc
}

func flattenTelemetry(telemetryData *telemetry.Telemetry) {
	model := ParseModel("bgp-model.json")
	tags, fields := GenerateFilterFromModel(&model)
	fmt.Println(tags)
	fmt.Println(fields)
	var prefix []string
	if len(telemetryData.DataGpbkv) > 0 {
		for _, item := range telemetryData.DataGpbkv {
			m := make(map[string]interface{})

			flatten(item, prefix, m)
			var enc lineprotocol.Encoder

			PrepareLine("grpcBgpOper", m, tags, fields, enc)

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
