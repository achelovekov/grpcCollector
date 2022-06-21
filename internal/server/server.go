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
	"time"
	"sort"
	"strconv"
	//"reflect"

	//dialout "github.com/CiscoSE/grpc/proto/mdt_dialout"
	// "encoding/json"

	dialout "github.com/achelovekov/grpcCollector/proto/mdt_dialout"
	telemetry "github.com/achelovekov/grpcCollector/proto/telemetry"
	"github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/proto"
)

type DialOutServer struct {
	cancel context.CancelFunc
	ctx    context.Context
}

type MetricContext struct {
	Client influxdb2.Client
	Measurement string
	Models []string
	BucketName string
	OrgName string
}

func (c *DialOutServer) MdtDialout(stream dialout.GRPCMdtDialout_MdtDialoutServer) error {

	peer, peerOK := peer.FromContext(stream.Context())
	if peerOK {
		log.Printf("Accepted Cisco MDT GRPC dialout connection from %s", peer.Addr)
	}


	client := influxdb2.NewClient(
		"http://10.0.17.11:8086",
		"Qh7S7jcZL-Y1DkK524RHDUPjkJdW-a_85sGbSPnKSzIXg9R8OAGb92XFLfgbxLEgqbA5zbvOQFBD3sJX3Xis2g==")

	orgName := "Neto"
	bucketName := "grpc"

	writeAPI := client.WriteAPI(orgName, bucketName)
	errorsCh := writeAPI.Errors()
    go func() {
        for err := range errorsCh {
            fmt.Printf("write error: %s\n", err.Error())
        }
    }()


	metricContext := MetricContext{
		Client: client,
		Measurement: "bgp",
		Models: []string{"bgp-model-afi.json", "bgp-model-neighbors.json"},
		BucketName: bucketName,
		OrgName: orgName,
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

		c.handleTelemetry(writeAPI, metricContext, packet.Data)
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
	IsField bool `json:"isField"`
	IsList bool `json:"isList"`
}

func (model *Model) String() string {
    return fmt.Sprintf("Name: %v isTag: %v isField: %v isList: %v", model.Name, model.IsTag, model.IsField, model.IsList)
}

func (model *Model) GetNestedByName(name string) *Model {
	for _, item := range model.Nested {
		if item.Name == name {
			return item
		}
	}
    return &Model{}
}

func (model *Model) GetName() string {
	if model != nil {
		return model.Name
	}
	return ""
}

func (model *Model) GetIsTag() bool {
	if model != nil {
		return model.IsTag
	}
	return false
}

func (model *Model) GetIsField() bool {
	if model != nil {
		return model.IsField
	}
	return false
}

func (model *Model) GetNested() []*Model {
	if model != nil {
		return model.Nested
	}
	return []*Model{}
}

func (model *Model) CheckNestedByName(s string) bool {
	for _, item := range model.Nested {
		if item.Name == s {
			return true
		}
	}
	return false
}

func ParseModel(filename string) Model {
	jsonFile, err := os.Open(filename)
	if err != nil {
		fmt.Println(err)
		return Model{}
	}
	defer jsonFile.Close()
	byteValue, _ := ioutil.ReadAll(jsonFile)

	var model Model

	json.Unmarshal(byteValue, &model)

	return model
}

func (c *DialOutServer) handleTelemetry(writeAPI api.WriteAPI, metricContext MetricContext, data []byte) {

	telemetryData := &telemetry.Telemetry{}
	err := proto.Unmarshal(data, telemetryData)
	if err != nil {
		log.Printf("Error: %s", err.Error())
		return
	}

	for _, model := range metricContext.Models {
		model := ParseModel(model)
		go bar(metricContext.Measurement, writeAPI, &model, telemetryData)
	}
}

func GetContent(model *Model, tf *telemetry.TelemetryField) (*telemetry.TelemetryField, error) {
	modelName := model.GetName()

	if len(tf.GetName()) == 0  && len(tf.GetFields()) > 0 {
		for _, tf := range tf.GetFields() {
			if name := tf.GetName(); name == "keys" || name == "content" {
				for _, tf := range tf.GetFields() {
					if tf.GetName() == modelName {
						return tf, nil
					}
				}
			}
		}
	}
	if len(tf.GetName()) > 0 && len(tf.GetFields()) > 0 {
		for _, tf := range tf.GetFields() {
			if tf.GetName() == modelName {
				return tf, nil
			}
		}
	}

	return &telemetry.TelemetryField{}, fmt.Errorf("no content %v in nested of %s", model.GetName(), tf.GetName())
}

func GetContentList(model *Model, tf *telemetry.TelemetryField) []*telemetry.TelemetryField  {
	result := []*telemetry.TelemetryField{}
	for _, tfField := range tf.GetFields() {
		if tfField.GetName() == model.GetName() {
			newTf := telemetry.TelemetryField{}
			newTf.Name = tf.GetName()
			newTf.Fields = append(newTf.Fields, tfField)
			result = append(result, &newTf)
		}
	}
	return result
}

type Keys struct {
	Leafs []*Model
	Nesteds []*Model
	WLists []*Model
}

func (keys *Keys) String() string {
    return fmt.Sprintf("Leafs: %v Nesteds: %v WLists: %v", keys.Leafs, keys.Nesteds, keys.WLists)
}

func FindKeys(model *Model) Keys {
	var leafs []*Model
	var nesteds []*Model
	var wLists []*Model

	nested := model.GetNested()

	if len(nested) > 0 {
		for _, model := range nested {
			if len(model.GetNested()) == 0 && !model.IsList {
				leafs = append(leafs, model)
			}
			if len(model.GetNested()) > 0 && !model.IsList{
				nesteds = append(nesteds, model)
			}
			if model.IsList {
				wLists = append(wLists, model)
			}
		}
	}

	return Keys{Leafs: leafs, Nesteds: nesteds, WLists: wLists}
}

type Leaf struct {
	Value interface{}
	Model
}

func (leaf *Leaf) String() string {
    return fmt.Sprintf("Name: %v isTag: %v isField: %v isList: %v value: %v", leaf.Name, leaf.IsTag, leaf.IsField, leaf.IsList, leaf.Value)
}

func CopySlice(s *[]*Leaf) *[]*Leaf {
    d := make([]*Leaf, len(*s))
    copy(d, *s)

	return &d
}

func PrintSlice(s *[]*Leaf) {
	for _, item := range *s {
		fmt.Println(item)
	}
}

func ProcessSlice(sli *[]*Leaf) ([]*Leaf, []*Leaf) {
	tags := []*Leaf{}
	fields := []*Leaf{}

	for _, item := range *sli {
		if item.IsTag {
			tags = append(tags, item)
		}
		if item.IsField {
			fields = append(fields, item)
		}
	}

	sort.SliceStable(tags, func(i, j int) bool {
		return tags[i].Name < tags[j].Name
	  })

	sort.SliceStable(fields, func(i, j int) bool {
		return fields[i].Name < fields[j].Name
	  })

	return tags, fields
}

func PushSlice(measurement string, writeAPI api.WriteAPI, sli *[]*Leaf) {
	tags, fields := ProcessSlice(sli)

	point := influxdb2.NewPointWithMeasurement(measurement)
	for _, tag := range tags {
		switch tag.Value.(type) {
		case string:
			point.AddTag(tag.GetName(), tag.Value.(string))
		case int64:
			value := tag.Value.(int64)
			point.AddTag(tag.GetName(), strconv.FormatInt(value, 10))
		}
	}
	for _, field := range fields {
		point.AddField(field.GetName(), field.Value)
	}
	point.SetTime(time.Now())
	writeAPI.WritePoint(point)
	writeAPI.Flush()
}

func UpdateSli(model *Model, tf *telemetry.TelemetryField, sli *[]*Leaf, prefix []string) {

	tf, err := GetContent(model, tf)

	if err != nil {
		log.Println(err)
		return
	}

	fullPath := append(prefix, model.GetName())
	newLeaf := Leaf{}
	newLeaf.Name = strings.Join(fullPath,".")
	newLeaf.IsTag = model.GetIsTag()
	newLeaf.IsField = model.GetIsField()
	i := tf.GetValueByType()
	switch i.(type) {
	case *telemetry.TelemetryField_BytesValue:
		newLeaf.Value = tf.GetBytesValue()
	case *telemetry.TelemetryField_StringValue:
		newLeaf.Value = tf.GetStringValue()
	case *telemetry.TelemetryField_BoolValue:
		newLeaf.Value = tf.GetBoolValue()
	case *telemetry.TelemetryField_Uint32Value:
		newLeaf.Value = int64(tf.GetUint32Value())
	case *telemetry.TelemetryField_Uint64Value:
		newLeaf.Value = tf.GetUint64Value()
	case *telemetry.TelemetryField_Sint32Value:
		newLeaf.Value = tf.GetSint32Value()
	case *telemetry.TelemetryField_Sint64Value:
		newLeaf.Value = tf.GetSint64Value()
	case *telemetry.TelemetryField_DoubleValue:
		newLeaf.Value = tf.GetDoubleValue()
	case *telemetry.TelemetryField_FloatValue:
		newLeaf.Value = tf.GetFloatValue()
	}

	*sli = append(*sli, &newLeaf)
}

func foo(measurement string, writeAPI api.WriteAPI, model *Model, tf *telemetry.TelemetryField, sli *[]*Leaf, prefix []string) {
	tf, err := GetContent(model, tf)

	if err != nil {
		return
	}

	keys := FindKeys(model)

	for _, leaf := range keys.Leafs {
		UpdateSli(leaf, tf, sli, prefix)
	}

	if len(keys.Nesteds) > 0 {
		prefix := append(prefix, model.GetName())
		for _, nested := range keys.Nesteds {
			newSli := CopySlice(sli)
			var newPrefix []string
    		newPrefix = append(newPrefix, prefix...)
			newPrefix = append(newPrefix, nested.GetName())

			go foo(measurement, writeAPI, nested, tf, newSli, newPrefix)
		}
	}

	if len(keys.WLists) > 0 {
		for _, wList := range keys.WLists {
			for _, content := range GetContentList(wList, tf) {
				newSli := CopySlice(sli)
				var newPrefix []string
				newPrefix = append(newPrefix, prefix...)
				if len(wList.GetNested()) > 0 {
					newPrefix = append(newPrefix, wList.GetName())
				}
				go foo(measurement, writeAPI, wList, content, newSli, newPrefix)
			}
		}
	}

	if len(keys.Nesteds) == 0 && len(keys.WLists) == 0 {
		PushSlice(measurement, writeAPI, sli)
	}

}

func bar(measurement string, writeAPI api.WriteAPI, model *Model, td *telemetry.Telemetry) {
	sli := []*Leaf{}
	prefix := []string{}
	for _, tf := range td.GetDataGpbkv() {
		go foo(measurement, writeAPI, model, tf, &sli, prefix)
	}
}

func destructureTelemetry(telemetry *telemetry.Telemetry, model *Model) {
	telemetryData := telemetry.GetDataGpbkv()

	for _, item := range telemetryData {
		destructureFields(item.GetFields(), model)
	}
}

func destructureFields(fields []*telemetry.TelemetryField, model *Model) {
	for _, item := range fields {
		if name := item.GetName(); len(name) >= 0 {
			if len(name) == 0 {
				destructureFields(item.GetFields(), model)
			}
			switch name {
			case "keys", "content":
				destructureFields(item.GetFields(), model)
			default:
				newModel := &Model{Name: item.GetName()}
				if exist := model.GetNestedByName(item.GetName()); len(exist.Name) > 0 {
					exist.IsList = true
				} else {
					model.Nested = append(model.Nested, newModel)
				}
				if len(item.GetFields()) > 0 {
					destructureFields(item.GetFields(), newModel)
				}
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
